// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::sync::Arc;

use super::{ParquetAccessPlan, ParquetFileMetrics, RowGroupAccess};
use crate::BloomFilterStatistics;
use arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use arrow::datatypes::Schema;
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_datasource::FileRange;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, IsNullExpr, NotExpr};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprSimplifier};
use datafusion_pruning::{PruningPredicate, PruningPredicateBuilder};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::RowGroupMetaData;
use parquet::schema::types::SchemaDescriptor;

/// Reduces the [`ParquetAccessPlan`] based on row group level metadata.
///
/// This struct implements the various types of pruning that are applied to a
/// set of row groups within a parquet file, progressively narrowing down the
/// set of row groups (and ranges/selections within those row groups) that
/// should be scanned, based on the available metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct RowGroupAccessPlanFilter {
    /// which row groups should be accessed
    access_plan: ParquetAccessPlan,
}

impl RowGroupAccessPlanFilter {
    /// Create a new `RowGroupPlanBuilder` for pruning out the groups to scan
    /// based on metadata and statistics
    pub fn new(access_plan: ParquetAccessPlan) -> Self {
        Self { access_plan }
    }

    /// Return true if there are no row groups
    pub fn is_empty(&self) -> bool {
        self.access_plan.is_empty()
    }

    /// Return the number of row groups that are currently expected to be scanned
    pub fn remaining_row_group_count(&self) -> usize {
        self.access_plan.row_group_index_iter().count()
    }

    /// Return indexes of row groups that still need to be scanned.
    pub fn row_group_indexes(&self) -> impl Iterator<Item = usize> + '_ {
        self.access_plan.row_group_index_iter()
    }

    /// Returns the inner access plan.
    pub fn build(self) -> ParquetAccessPlan {
        self.access_plan
    }

    /// Returns the is_fully_matched vector.
    pub fn is_fully_matched(&self) -> &Vec<bool> {
        self.access_plan.fully_matched()
    }

    /// Prunes the access plan based on the limit and fully contained row groups.
    ///
    /// The pruning works by leveraging the concept of fully matched row groups. Consider a query like:
    /// `WHERE species LIKE 'Alpine%' AND s >= 50 LIMIT N`
    ///
    /// After initial filtering, row groups can be classified into three states:
    ///
    /// 1. Not Matching / Pruned
    /// 2. Partially Matching (Row Group/Page contains some matches)
    /// 3. Fully Matching (Entire range is within predicate)
    ///
    /// +-----------------------------------------------------------------------+
    /// |                            NOT MATCHING                               |
    /// |  Row group 1                                                          |
    /// |  +-----------------------------------+-----------------------------+  |
    /// |  | SPECIES                           | S                           |  |
    /// |  +-----------------------------------+-----------------------------+  |
    /// |  | Snow Vole                         | 7                           |  |
    /// |  | Brown Bear                        | 133 ✅                      |  |
    /// |  | Gray Wolf                         | 82  ✅                      |  |
    /// |  +-----------------------------------+-----------------------------+  |
    /// +-----------------------------------------------------------------------+
    ///
    /// +---------------------------------------------------------------------------+
    /// |                          PARTIALLY MATCHING                               |
    /// |                                                                           |
    /// |  Row group 2                              Row group 4                     |
    /// |  +------------------+--------------+      +------------------+----------+ |
    /// |  | SPECIES          | S            |      | SPECIES          | S        | |
    /// |  +------------------+--------------+      +------------------+----------+ |
    /// |  | Lynx             | 71 ✅        |      | Europ. Mole      | 4        | |
    /// |  | Red Fox          | 40           |      | Polecat          | 16       | |
    /// |  | Alpine Bat  ✅   | 6            |      | Alpine Ibex ✅  | 97 ✅    | |
    /// |  +------------------+--------------+      +------------------+----------+ |
    /// +---------------------------------------------------------------------------+
    ///
    /// +-----------------------------------------------------------------------+
    /// |                           FULLY MATCHING                              |
    /// |  Row group 3                                                          |
    /// |  +-----------------------------------+-----------------------------+  |
    /// |  | SPECIES                           | S                           |  |
    /// |  +-----------------------------------+-----------------------------+  |
    /// |  | Alpine Ibex  ✅                  | 101    ✅                   |  |
    /// |  | Alpine Goat  ✅                  | 76     ✅                   |  |
    /// |  | Alpine Sheep ✅                  | 83     ✅                   |  |
    /// |  +-----------------------------------+-----------------------------+  |
    /// +-----------------------------------------------------------------------+
    ///
    /// ### Identification of Fully Matching Row Groups
    ///
    /// DataFusion identifies row groups where ALL rows satisfy the filter by inverting the
    /// predicate and checking if statistics prove the inverted version is false for the group.
    ///
    /// For example, prefix matches like `species LIKE 'Alpine%'` are pruned using ranges:
    /// 1. Candidate Range: `species >= 'Alpine' AND species < 'Alpinf'`
    /// 2. Inverted Condition (to prove full match): `species < 'Alpine' OR species >= 'Alpinf'`
    /// 3. Statistical Evaluation (check if any row *could* satisfy the inverted condition):
    ///    `min < 'Alpine' OR max >= 'Alpinf'`
    ///
    /// If this evaluation is **false**, it proves no row can fail the original filter,
    /// so the row group is **FULLY MATCHING**.
    ///
    /// ### Impact of Statistics Truncation
    ///
    /// The precision of pruning depends on the metadata quality. Truncated statistics
    /// may prevent the system from proving a full match.
    ///
    /// **Example**: `WHERE species LIKE 'Alpine%'` (Target range: `['Alpine', 'Alpinf')`)
    ///
    /// | Truncation Length | min / max           | Inverted Evaluation                                                 | Status                 |
    /// |-------------------|---------------------|---------------------------------------------------------------------|------------------------|
    /// | **Length 6**      | `Alpine` / `Alpine` | `"Alpine" < "Alpine" (F) OR "Alpine" >= "Alpinf" (F)` -> **false**  | **FULLY MATCHING**     |
    /// | **Length 3**      | `Alp` / `Alq`       | `"Alp" < "Alpine" (T) OR "Alq" >= "Alpinf" (T)` -> **true**         | **PARTIALLY MATCHING** |
    ///
    /// Even though Row Group 3 only contains matching rows, truncation to length 3 makes
    /// the statistics `[Alp, Alq]` too broad to prove it (they could include "Alpha").
    /// The system must conservatively scan the group.
    ///
    /// Without limit pruning: Scan Partition 2 → Partition 3 → Partition 4 (until limit reached)
    /// With limit pruning: If Partition 3 contains enough rows to satisfy the limit,
    /// skip Partitions 2 and 4 entirely and go directly to Partition 3.
    ///
    /// This optimization is particularly effective when:
    /// - The limit is small relative to the total dataset size
    /// - There are row groups that are fully matched by the filter predicates
    /// - The fully matched row groups contain sufficient rows to satisfy the limit
    ///
    /// For more information, see the [paper](https://arxiv.org/pdf/2504.11540)'s "Pruning for LIMIT Queries" part
    pub fn prune_by_limit(
        &mut self,
        limit: usize,
        rg_metadata: &[RowGroupMetaData],
        metrics: &ParquetFileMetrics,
    ) {
        let mut fully_matched_row_group_indexes: Vec<usize> = Vec::new();
        let mut fully_matched_rows_count: usize = 0;

        // Iterate through the currently accessible row groups and try to
        // find a set of matching row groups that can satisfy the limit
        for &idx in self.access_plan.row_group_indexes().iter() {
            if self.access_plan.is_fully_matched(idx) {
                let row_group_row_count = match &self.access_plan.inner()[idx] {
                    RowGroupAccess::Skip => continue,
                    RowGroupAccess::Scan => rg_metadata[idx].num_rows() as usize,
                    RowGroupAccess::Selection(selection) => selection.row_count(),
                };
                fully_matched_row_group_indexes.push(idx);
                fully_matched_rows_count += row_group_row_count;
                if fully_matched_rows_count >= limit {
                    break;
                }
            }
        }

        // If we can satisfy the limit with fully matching row groups,
        // rewrite the plan to do so
        if fully_matched_rows_count >= limit {
            let original_num_accessible_row_groups = self.access_plan.row_group_indexes().len();
            let new_num_accessible_row_groups = fully_matched_row_group_indexes.len();
            let pruned_count =
                original_num_accessible_row_groups.saturating_sub(new_num_accessible_row_groups);
            metrics.limit_pruned_row_groups.add_pruned(pruned_count);

            let mut new_access_plan = ParquetAccessPlan::new_none(rg_metadata.len());
            for &idx in &fully_matched_row_group_indexes {
                new_access_plan.set(idx, self.access_plan.inner()[idx].clone());
                new_access_plan.mark_fully_matched(idx);
            }
            self.access_plan = new_access_plan;
        }
    }

    /// Prune remaining row groups to only those  within the specified range.
    ///
    /// Updates this set to mark row groups that should not be scanned
    ///
    /// # Panics
    /// if `groups.len() != self.len()`
    pub fn prune_by_range(&mut self, groups: &[RowGroupMetaData], range: &FileRange) {
        assert_eq!(groups.len(), self.access_plan.len());
        for (idx, metadata) in groups.iter().enumerate() {
            if !self.access_plan.should_scan(idx) {
                continue;
            }

            // Skip the row group if the first dictionary/data page are not
            // within the range.
            //
            // note don't use the location of metadata
            // <https://github.com/apache/datafusion/issues/5995>
            let col = metadata.column(0);
            let offset = col
                .dictionary_page_offset()
                .unwrap_or_else(|| col.data_page_offset());
            if !range.contains(offset) {
                self.access_plan.skip(idx);
            }
        }
    }
    /// Prune remaining row groups using min/max/null_count statistics and
    /// the [`PruningPredicate`] to determine if the predicate can not be true.
    ///
    /// Updates this set to mark row groups that should not be scanned
    ///
    /// Note: This method currently ignores ColumnOrder
    /// <https://github.com/apache/datafusion/issues/8335>
    ///
    /// # Panics
    /// if `groups.len() != self.len()`
    pub fn prune_by_statistics(
        &mut self,
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        groups: &[RowGroupMetaData],
        predicate: &PruningPredicate,
        metrics: &ParquetFileMetrics,
    ) {
        // scoped timer updates on drop
        let _timer_guard = metrics.statistics_eval_time.timer();

        assert_eq!(groups.len(), self.access_plan.len());
        // Indexes of row groups still to scan
        let row_group_indexes = self.access_plan.row_group_indexes();
        let row_group_metadatas = row_group_indexes
            .iter()
            .map(|&i| &groups[i])
            .collect::<Vec<_>>();

        let pruning_stats = RowGroupPruningStatistics {
            parquet_schema,
            row_group_metadatas,
            arrow_schema,
            // Preserve the existing row-group pruning behavior. This path only
            // proves whether matching rows may exist, so it uses the
            // StatisticsConverter default for older parquet-rs files where a
            // missing null count can mean there are zero nulls.
            missing_null_counts_as_zero: true,
        };

        // try to prune the row groups in a single call
        match predicate.prune(&pruning_stats) {
            Ok(values) => {
                let mut fully_contained_candidates_original_idx: Vec<usize> = Vec::new();
                for (idx, &value) in row_group_indexes.iter().zip(values.iter()) {
                    if !value {
                        self.access_plan.skip(*idx);
                        metrics.row_groups_pruned_statistics.add_pruned(1);
                    } else {
                        metrics.row_groups_pruned_statistics.add_matched(1);
                        fully_contained_candidates_original_idx.push(*idx);
                    }
                }

                // Check if any of the matched row groups are fully contained by the predicate
                self.identify_fully_matched_row_groups(
                    &fully_contained_candidates_original_idx,
                    arrow_schema,
                    parquet_schema,
                    groups,
                    predicate,
                    metrics,
                );
            }
            // stats filter array could not be built, so we can't prune
            Err(e) => {
                log::debug!("Error evaluating row group predicate values {e}");
                metrics.predicate_evaluation_errors.add(1);
            }
        }
    }

    /// Identifies row groups that are fully matched by the predicate.
    ///
    /// This optimization checks whether all rows in a row group satisfy the predicate
    /// by inverting the predicate and checking if it prunes the row group. If the
    /// inverted predicate prunes a row group, it means no rows match the inverted
    /// predicate, which implies all rows match the original predicate.
    ///
    /// Note: This optimization is relatively inexpensive for a limited number of row groups.
    fn identify_fully_matched_row_groups(
        &mut self,
        candidate_row_group_indices: &[usize],
        arrow_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
        groups: &[RowGroupMetaData],
        predicate: &PruningPredicate,
        metrics: &ParquetFileMetrics,
    ) {
        if candidate_row_group_indices.is_empty() {
            return;
        }

        let mut inverted_expr: Arc<dyn PhysicalExpr> =
            Arc::new(NotExpr::new(Arc::clone(predicate.orig_expr())));

        // Rows where the predicate evaluates to NULL do not pass the filter.
        // Include NULL checks in the inverted expression so a row group is only
        // considered fully matched when every referenced column is known non-null.
        // This is conservative for null-accepting predicates, but fully matched
        // row groups must not have false positives.
        let mut columns = collect_columns(predicate.orig_expr())
            .into_iter()
            .filter(|column| arrow_schema.field(column.index()).is_nullable())
            .collect::<Vec<_>>();
        columns.sort_by(|a, b| {
            a.index()
                .cmp(&b.index())
                .then_with(|| a.name().cmp(b.name()))
        });

        for column in columns {
            inverted_expr = Arc::new(BinaryExpr::new(
                inverted_expr,
                Operator::Or,
                Arc::new(IsNullExpr::new(Arc::new(column))),
            ));
        }

        // Simplify the inverted expression (e.g., NOT(c1 = 0) -> c1 != 0)
        // before building the pruning predicate
        let simplifier = PhysicalExprSimplifier::new(arrow_schema);
        let Ok(inverted_expr) = simplifier.simplify(inverted_expr) else {
            return;
        };

        let Ok(inverted_predicate) = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(predicate.schema()))
            .try_build(inverted_expr)
        else {
            return;
        };

        let inverted_pruning_stats = RowGroupPruningStatistics {
            parquet_schema,
            row_group_metadatas: candidate_row_group_indices
                .iter()
                .map(|&i| &groups[i])
                .collect::<Vec<_>>(),
            arrow_schema,
            // Fully matched row groups require a stronger proof: every row
            // must pass the predicate. Missing null counts are unknown here;
            // treating them as zero can incorrectly mark nullable row groups as
            // fully matched and make limit pruning unsound.
            missing_null_counts_as_zero: false,
        };

        let Ok(inverted_values) = inverted_predicate.prune(&inverted_pruning_stats) else {
            return;
        };

        for (i, &original_row_group_idx) in candidate_row_group_indices.iter().enumerate() {
            // If the inverted predicate *also* prunes this row group (meaning inverted_values[i] is false),
            // it implies that *all* rows in this group satisfy the original predicate.
            if !inverted_values[i] {
                self.access_plan.mark_fully_matched(original_row_group_idx);
                metrics.row_groups_pruned_statistics.add_fully_matched(1);
            }
        }
    }

    /// Prune remaining row groups using loaded bloom filters and the
    /// [`PruningPredicate`].
    ///
    /// Updates this set with row groups that should not be scanned.
    /// `row_group_bloom_filters[idx]` contains the bloom filters for the
    /// parquet row group at index `idx`.
    ///
    /// # Panics
    /// if `row_group_bloom_filters` does not have the same number of row groups as this set
    pub fn prune_by_bloom_filters(
        &mut self,
        predicate: &PruningPredicate,
        metrics: &ParquetFileMetrics,
        row_group_bloom_filters: &[BloomFilterStatistics],
    ) {
        // scoped timer updates on drop
        let _timer_guard = metrics.bloom_filter_eval_time.timer();

        assert_eq!(row_group_bloom_filters.len(), self.access_plan.len());
        for (idx, stats) in row_group_bloom_filters.iter().enumerate() {
            if !self.access_plan.should_scan(idx) {
                continue;
            }

            // Can this group be pruned?
            let prune_group = match predicate.prune(stats) {
                Ok(values) => !values[0],
                Err(e) => {
                    log::debug!("Error evaluating row group predicate on bloom filter: {e}");
                    metrics.predicate_evaluation_errors.add(1);
                    false
                }
            };

            if prune_group {
                metrics.row_groups_pruned_bloom_filter.add_pruned(1);
                self.access_plan.skip(idx)
            } else {
                metrics.row_groups_pruned_bloom_filter.add_matched(1);
            }
        }
    }
}

/// Wraps a slice of [`RowGroupMetaData`] in a way that implements [`PruningStatistics`].
///
/// Visible to sibling modules so runtime row-group pruners (e.g. the dynamic
/// TopK pruner in `push_decoder.rs`) can reuse this adapter without
/// duplicating the statistics-to-`PruningStatistics` plumbing.
pub(crate) struct RowGroupPruningStatistics<'a> {
    pub(crate) parquet_schema: &'a SchemaDescriptor,
    pub(crate) row_group_metadatas: Vec<&'a RowGroupMetaData>,
    pub(crate) arrow_schema: &'a Schema,
    pub(crate) missing_null_counts_as_zero: bool,
}

impl<'a> RowGroupPruningStatistics<'a> {
    /// Return an iterator over the row group metadata
    fn metadata_iter(&'a self) -> impl Iterator<Item = &'a RowGroupMetaData> + 'a {
        self.row_group_metadatas.iter().copied()
    }

    fn statistics_converter<'b>(&'a self, column: &'b Column) -> Result<StatisticsConverter<'a>> {
        Ok(
            StatisticsConverter::try_new(&column.name, self.arrow_schema, self.parquet_schema)?
                .with_missing_null_counts_as_zero(self.missing_null_counts_as_zero),
        )
    }
}

impl PruningStatistics for RowGroupPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_mins(self.metadata_iter())?))
            .ok()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_maxes(self.metadata_iter())?))
            .ok()
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadatas.len()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_null_counts(self.metadata_iter())?))
            .ok()
            .map(|counts| Arc::new(counts) as ArrayRef)
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        // Row counts are container-level — read directly from row group metadata.
        let counts: UInt64Array = self
            .metadata_iter()
            .map(|rg| Some(rg.num_rows() as u64))
            .collect();
        Some(Arc::new(counts) as ArrayRef)
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
