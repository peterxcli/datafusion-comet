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

//! Push-based Parquet decoder setup and stream driver.
//!
//! This module owns the push-decoder lifecycle:
//!
//! - [`DecoderBuilderConfig`] holds the shared options applied to the
//!   [`ParquetPushDecoderBuilder`] for a file scan, exposing a single `build`
//!   entry point.
//! - [`PushDecoderStreamState`] is the per-file stream driver. It owns a
//!   **single** [`ParquetPushDecoder`] plus an [`RgPlanEntry`] queue
//!   (`rg_plan`) and uses arrow-rs's [`ParquetRecordBatchReader`] iterator
//!   to pause at row-group boundaries. At each boundary the optional
//!   [`RowGroupPruner`] is consulted; row groups it proves unwinnable are
//!   dropped from the head of `rg_plan` and the decoder is rebuilt via
//!   [`ParquetPushDecoder::into_builder`] +
//!   [`ParquetPushDecoderBuilder::with_row_groups`] so the skipped RGs are
//!   bypassed entirely — no decode, no row-filter eval.
//!
//! The opener constructs both halves and hands the state off to
//! [`PushDecoderStreamState::into_stream`] for consumption.

use bytes::Bytes;
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Mutex;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::stream::BoxStream;
use log::debug;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, RowSelectionPolicy,
};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use parquet::file::metadata::ParquetMetaData;

use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_physical_expr::expressions::DynamicFilterTracking;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::{BaselineMetrics, Count, Gauge};
use datafusion_pruning::{PruningPredicate, PruningPredicateBuilder};

use crate::access_plan::PreparedAccessPlan;
use crate::decoder_projection::DecoderProjection;
use crate::row_group_filter::RowGroupPruningStatistics;

/// Shared options applied to the [`ParquetPushDecoderBuilder`] for a file
/// scan, and to any later rebuilds performed via
/// [`ParquetPushDecoder::into_builder`] at row-group boundaries (e.g. when
/// the [`RowGroupPruner`] drops subsequent row groups).
pub(crate) struct DecoderBuilderConfig<'a> {
    /// Projection mask installed on every decoder in the scan. Sourced from
    /// the file's [`DecoderProjection`].
    pub(crate) projection_mask: &'a ProjectionMask,
    pub(crate) batch_size: usize,
    pub(crate) arrow_reader_metrics: &'a ArrowReaderMetrics,
    pub(crate) force_filter_selections: bool,
    pub(crate) decoder_limit: Option<usize>,
}

impl DecoderBuilderConfig<'_> {
    /// Build a [`ParquetPushDecoderBuilder`] from a prepared access plan.
    ///
    /// The caller is expected to attach the
    /// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) and predicate
    /// cache size on the returned builder.
    pub(crate) fn build(
        &self,
        prepared_access_plan: PreparedAccessPlan,
        metadata: ArrowReaderMetadata,
    ) -> ParquetPushDecoderBuilder {
        let mut builder = ParquetPushDecoderBuilder::new_with_metadata(metadata)
            .with_projection(self.projection_mask.clone())
            .with_batch_size(self.batch_size)
            .with_metrics(self.arrow_reader_metrics.clone());
        if self.force_filter_selections {
            builder = builder.with_row_selection_policy(RowSelectionPolicy::Selectors);
        }
        if let Some(row_selection) = prepared_access_plan.row_selection {
            builder = builder.with_row_selection(row_selection);
        }
        builder = builder.with_row_groups(prepared_access_plan.row_group_indexes);
        if let Some(limit) = self.decoder_limit {
            builder = builder.with_limit(limit);
        }
        builder
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RgPlanEntry {
    pub(crate) rg_index: usize,
}

/// Runtime row-group pruner driven by a dynamic predicate (e.g. the
/// threshold expression a `TopK` operator pushes down).
///
/// Mirrors the [`FilePruner`](datafusion_pruning::FilePruner) pattern at
/// the row-group level: subscribes once to every still-incomplete dynamic
/// filter inside the predicate via
/// [`DynamicFilterTracker`](datafusion_physical_expr::expressions::DynamicFilterTracker)
/// and only rebuilds the [`PruningPredicate`] when one of those
/// subscriptions reports an update, then evaluates the cached predicate
/// against the statistics of the requested row groups.
pub(crate) struct RowGroupPruner {
    predicate: Arc<dyn PhysicalExpr>,
    arrow_schema: SchemaRef,
    parquet_metadata: Arc<ParquetMetaData>,
    /// Classifies the predicate's dynamic-filter content. The `Watching`
    /// variant carries a tracker that subscribes to every not-yet-complete
    /// dynamic filter; for `Static` / `AllComplete` the predicate cannot
    /// change so a single up-front `pruning_predicate` build suffices.
    tracking: DynamicFilterTracking,
    /// First-call sentinel: forces an initial `pruning_predicate` build
    /// even when `tracking` is `Static` / `AllComplete`.
    needs_initial_build: bool,
    /// Cached pruning predicate. `None` means we couldn't build one for the
    /// current generation (e.g. the predicate has no analyzable bounds);
    /// in that case we conservatively don't prune.
    pruning_predicate: Option<Arc<PruningPredicate>>,
    /// Metric for `build_pruning_predicate` failures (predicate creation).
    predicate_creation_errors: Count,
    /// Metric for `PruningPredicate::prune` failures (evaluating an
    /// already-built predicate against row-group statistics).
    predicate_evaluation_errors: Count,
    /// Cap on the `IN (...)` list size that the pruning predicate will
    /// rewrite into per-value statistics checks. Longer lists skip
    /// container-level pruning. Sourced from
    /// `datafusion.execution.parquet.max_in_list_size`.
    max_in_list_size: usize,
}

impl RowGroupPruner {
    pub(crate) fn new(
        predicate: Arc<dyn PhysicalExpr>,
        arrow_schema: SchemaRef,
        parquet_metadata: Arc<ParquetMetaData>,
        predicate_creation_errors: Count,
        predicate_evaluation_errors: Count,
        max_in_list_size: usize,
    ) -> Self {
        let tracking = DynamicFilterTracking::classify(&predicate);
        Self {
            predicate,
            arrow_schema,
            parquet_metadata,
            tracking,
            needs_initial_build: true,
            pruning_predicate: None,
            predicate_creation_errors,
            predicate_evaluation_errors,
            max_in_list_size,
        }
    }

    /// Returns `true` when the statistics for `row_group_indices` prove that
    /// every requested row group can be skipped under the current value of
    /// the dynamic predicate.
    ///
    /// On any error (predicate construction, statistics evaluation) the
    /// pruner conservatively returns `false` and logs the failure, so a
    /// flaky pruning path never silently drops data.
    pub(crate) fn should_prune(&mut self, row_group_indices: &[usize]) -> bool {
        if row_group_indices.is_empty() {
            return false;
        }

        // Refresh the cached `PruningPredicate` on the first call and
        // whenever a watched dynamic filter has advanced since we last
        // looked. `changed()` is a single atomic load per still-incomplete
        // filter — no tree walk on every check.
        let dynamic_changed = self
            .tracking
            .watcher()
            .is_some_and(|tracker| tracker.changed());
        if self.needs_initial_build || dynamic_changed {
            self.pruning_predicate = PruningPredicateBuilder::new()
                .with_file_schema(Arc::clone(&self.arrow_schema))
                .with_error_counter(&self.predicate_creation_errors)
                .with_max_in_list_size(self.max_in_list_size)
                .build(Arc::clone(&self.predicate));
            self.needs_initial_build = false;
        }

        let Some(pp) = self.pruning_predicate.as_ref() else {
            return false;
        };

        let row_group_metadatas = row_group_indices
            .iter()
            .map(|&i| self.parquet_metadata.row_group(i))
            .collect::<Vec<_>>();
        let stats = RowGroupPruningStatistics {
            parquet_schema: self.parquet_metadata.file_metadata().schema_descr(),
            row_group_metadatas,
            arrow_schema: self.arrow_schema.as_ref(),
            // Match the existing static row-group pruning behavior: when a
            // statistic's null count is missing, treat it as zero. This is
            // sound for runtime pruning because the predicate only needs to
            // prove a row group *cannot* contain matching rows.
            missing_null_counts_as_zero: true,
        };

        match pp.prune(&stats) {
            // `prune` returns `false` per container that the predicate proves
            // cannot contain matching rows. We can skip the run only when
            // every requested row group is in that state.
            Ok(values) => values.iter().all(|&keep| !keep),
            Err(e) => {
                // The predicate was already built successfully (we hold `pp`);
                // this failure is in *evaluating* it against the row-group
                // stats, so it belongs in the evaluation-errors counter, not
                // creation-errors.
                debug!("Ignoring error evaluating runtime row-group pruning predicate: {e}");
                self.predicate_evaluation_errors.add(1);
                false
            }
        }
    }
}

/// Execution-local prefetch configuration, supplied by the embedding engine.
#[derive(Clone, Debug)]
pub(crate) struct RowGroupPrefetchOptions {
    pub(crate) max_bytes: usize,
    pub(crate) memory_pool: Arc<dyn MemoryPool>,
}

/// At most one future row group is in flight. The task owns the reservation so
/// cancellation keeps its bytes accounted until the I/O future is actually dropped.
pub(crate) struct PrefetchedRowGroup {
    row_group: usize,
    ranges: Vec<Range<u64>>,
    task: SpawnedTask<Result<(Vec<Bytes>, MemoryReservation)>>,
}

impl PrefetchedRowGroup {
    fn start(
        row_group: usize,
        metadata: &ParquetMetaData,
        projection: &ProjectionMask,
        options: &RowGroupPrefetchOptions,
        reader: Arc<Mutex<Box<dyn AsyncFileReader>>>,
        metrics: &crate::metrics::PrefetchMetrics,
    ) -> Option<Self> {
        let ranges: Vec<_> = metadata
            .row_group(row_group)
            .columns()
            .iter()
            .enumerate()
            .filter(|(i, _)| projection.leaf_included(*i))
            .map(|(_, column)| {
                let (start, len) = column.byte_range();
                start.checked_add(len).map(|end| start..end)
            })
            .collect::<Option<_>>()?;
        let bytes = ranges.iter().try_fold(0usize, |total, range| {
            total.checked_add(usize::try_from(range.end - range.start).ok()?)
        })?;
        if bytes == 0 || bytes > options.max_bytes {
            metrics.budget_skips.add(1);
            return None;
        }
        let reservation =
            MemoryConsumer::new("Parquet row-group prefetch").register(&options.memory_pool);
        if reservation.try_grow(bytes).is_err() {
            metrics.budget_skips.add(1);
            return None;
        }
        let metrics = metrics.clone();
        let fetch_ranges = ranges.clone();
        let task = SpawnedTask::spawn(async move {
            let data = reader.lock().await.get_byte_ranges(fetch_ranges).await?;
            metrics.bytes.add(data.iter().map(Bytes::len).sum());
            metrics.row_groups.add(1);
            Ok((data, reservation))
        });
        Some(Self {
            row_group,
            ranges,
            task,
        })
    }
}

/// State for a stream that decodes a single Parquet file using a push-based decoder.
///
/// The [`transition`](Self::transition) method drives the decoder in a loop: it requests
/// byte ranges from the [`AsyncFileReader`], pushes the fetched data into the
/// [`ParquetPushDecoder`], and yields projected [`RecordBatch`]es until the file is
/// fully consumed.
pub(crate) struct PushDecoderStreamState {
    pub(crate) decoder: Option<ParquetPushDecoder>,
    pub(crate) active_reader: Option<ParquetRecordBatchReader>,
    pub(crate) rg_plan: VecDeque<RgPlanEntry>,
    pub(crate) reader: Arc<Mutex<Box<dyn AsyncFileReader>>>,
    pub(crate) row_group_prefetch: Option<RowGroupPrefetchOptions>,
    pub(crate) parquet_metadata: Arc<ParquetMetaData>,
    pub(crate) pending_prefetch: Option<PrefetchedRowGroup>,
    pub(crate) prefetch_metrics: crate::metrics::PrefetchMetrics,
    pub(crate) prefetch_reservation: Option<MemoryReservation>,
    /// Per-file projection: the mask installed on every decoder and the
    /// per-batch transform applied by [`Self::project_batch`].
    pub(crate) decoder_projection: DecoderProjection,
    pub(crate) arrow_reader_metrics: ArrowReaderMetrics,
    pub(crate) predicate_cache_inner_records: Gauge,
    pub(crate) predicate_cache_records: Gauge,
    pub(crate) baseline_metrics: BaselineMetrics,
    /// Dynamic row-group pruner consulted at every row-group boundary.
    ///
    /// When the file scan was opened with a still-watching dynamic predicate
    /// (typically the threshold expression a `TopK` `SortExec` pushed down),
    /// we re-evaluate that predicate against the next pending RG's
    /// statistics and drop RGs the current threshold proves cannot
    /// contribute. The decoder is rebuilt via
    /// [`ParquetPushDecoder::into_builder`] +
    /// [`ParquetPushDecoderBuilder::with_row_groups`] so the skipped RGs are
    /// bypassed entirely. `None` when the scan has no watching dynamic
    /// predicate or only one row group remains.
    pub(crate) row_group_pruner: Option<RowGroupPruner>,
    /// Count of row groups skipped at runtime by [`Self::row_group_pruner`].
    pub(crate) row_groups_pruned_dynamic: Count,
}

impl PushDecoderStreamState {
    /// Drive the state machine to completion as a [`futures::Stream`] of record batches.
    ///
    /// The returned stream is fused and boxed so the caller can wrap it (for
    /// example, with an early-stopping adapter) without naming the unfold type.
    pub(crate) fn into_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::unfold(self, |state| async move { state.transition().await })
            .fuse()
            .boxed()
    }

    /// Advances the decoder state machine until the next [`RecordBatch`] is
    /// produced, the file is fully consumed, or an error occurs.
    ///
    /// At a row-group boundary the decoder is polled via [`ParquetPushDecoder::try_next_reader`]:
    /// - [`NeedsData`](DecodeResult::NeedsData) – the requested byte ranges are
    ///   fetched from the [`AsyncFileReader`] and fed back into the decoder.
    /// - [`Data`](DecodeResult::Data) – a reader is retained for subsequent batch decoding.
    /// - [`Finished`](DecodeResult::Finished) – signals end-of-stream (`None`).
    ///
    /// Takes `self` by value (rather than `&mut self`) so the generated future
    /// owns the state directly. This avoids a Stacked Borrows violation under
    /// miri where `&mut self` creates a single opaque borrow that conflicts
    /// with `unfold`'s ownership across yield points.
    async fn transition(mut self) -> Option<(Result<RecordBatch>, Self)> {
        loop {
            // Step 1: drain a batch from the active reader if any.
            if let Some(reader) = self.active_reader.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        let mut timer = self.baseline_metrics.elapsed_compute().timer();
                        self.copy_arrow_reader_metrics();
                        let result = self.project_batch(&batch);
                        timer.stop();
                        drop(timer);
                        return Some((result, self));
                    }
                    Some(Err(e)) => {
                        return Some((Err(DataFusionError::from(e)), self));
                    }
                    None => {
                        // Reader exhausted: drop and fall through to per-RG
                        // boundary handling, then try_next_reader.
                        self.active_reader = None;
                    }
                }
            }

            // Step 2: when the decoder is sitting on a row-group boundary,
            // scan the entire `rg_plan` and drop every RG the pruner proves
            // cannot contribute — head, interior, and tail alike. Evaluating
            // per-RG stats against the cached `PruningPredicate` is cheap;
            // the expensive part is the `into_builder` rebuild, so we do at
            // most one rebuild per boundary regardless of how many RGs were
            // dropped. Buffered bytes for already-fetched RGs carry across
            // the rebuild.
            //
            // `into_builder` errors out mid-row-group, so we gate the prune
            // pass on `is_at_row_group_boundary()`. When the decoder is
            // mid-RG (e.g. byte ranges have been pushed but no reader has
            // been handed back yet), step 3 drives it forward and we get
            // another chance at the next boundary — the pruner is stateful
            // and idempotent, so deferring loses nothing.
            let at_boundary = self
                .decoder
                .as_ref()
                .expect("decoder present")
                .is_at_row_group_boundary();
            // Only the runtime pruner rebuilds the decoder from `rg_plan`, so
            // only it needs `rg_plan` kept in sync with the decoder frontier.
            // arrow-rs silently finishes row groups whose post-predicate
            // selection is empty without handing back a reader, so without this
            // sync `rg_plan` trails the decoder by one and a rebuild re-reads an
            // already-delivered row group (#24352). Gating on the pruner also
            // avoids the O(remaining row groups) cost of `peek_next_row_group()`
            // on ordinary scans that never rebuild.
            if at_boundary
                && self.row_group_pruner.is_some()
                && let Err(e) = self.sync_rg_plan_to_decoder_frontier()
            {
                return Some((Err(e), self));
            }
            if at_boundary && !self.rg_plan.is_empty() {
                let mut pruned_count = 0usize;
                if let Some(pruner) = self.row_group_pruner.as_mut() {
                    let mut kept = VecDeque::with_capacity(self.rg_plan.len());
                    while let Some(entry) = self.rg_plan.pop_front() {
                        if pruner.should_prune(&[entry.rg_index]) {
                            pruned_count += 1;
                            self.row_groups_pruned_dynamic.add(1);
                        } else {
                            kept.push_back(entry);
                        }
                    }
                    self.rg_plan = kept;
                }
                if pruned_count > 0 {
                    if self.rg_plan.is_empty() {
                        return None;
                    }
                    let decoder = self.decoder.take().expect("decoder present");
                    let new_indices: Vec<usize> = self.rg_plan.iter().map(|e| e.rg_index).collect();
                    let rebuilt = match decoder.into_builder() {
                        Ok(b) => b.with_row_groups(new_indices).build(),
                        Err(e) => Err(e),
                    };
                    match rebuilt {
                        Ok(d) => self.decoder = Some(d),
                        Err(e) => {
                            return Some((Err(DataFusionError::from(e)), self));
                        }
                    }
                }
            }

            // Apply speculation only after runtime pruning has chosen the next
            // row group. A pruned group's task is aborted and its bytes discarded.
            if let Some(prefetch) = self.pending_prefetch.take() {
                let decoder = self.decoder.as_mut().expect("decoder present");
                match decoder.peek_next_row_group() {
                    Ok(Some(next)) if next == prefetch.row_group => {
                        let result = {
                            let _timer = self.prefetch_metrics.wait_time.timer();
                            prefetch.task.join_unwind().await
                        };
                        match result {
                            Ok(Ok((data, reservation))) => {
                                if let Err(e) = decoder.push_ranges(prefetch.ranges, data) {
                                    return Some((Err(e.into()), self));
                                }
                                self.prefetch_reservation = Some(reservation);
                            }
                            // A speculative error must not fail a scan that would
                            // not need those bytes. Let demand reads retry normally.
                            Ok(Err(e)) => debug!("Parquet prefetch failed: {e}"),
                            Err(e) => {
                                return Some((Err(DataFusionError::External(Box::new(e))), self));
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => return Some((Err(e.into()), self)),
                }
            }

            // Step 3: drive the decoder.
            let decoder = self.decoder.as_mut().expect("decoder present");
            match decoder.try_next_reader() {
                Ok(DecodeResult::NeedsData(ranges)) => {
                    let data = self
                        .reader
                        .lock()
                        .await
                        .get_byte_ranges(ranges.clone())
                        .await
                        .map_err(DataFusionError::from);
                    match data {
                        Ok(data) => {
                            if let Err(e) = self
                                .decoder
                                .as_mut()
                                .expect("decoder present")
                                .push_ranges(ranges, data)
                            {
                                return Some((Err(DataFusionError::from(e)), self));
                            }
                        }
                        Err(e) => return Some((Err(e), self)),
                    }
                }
                Ok(DecodeResult::Data(reader)) => {
                    // Pop the RG this reader is for (we already filtered
                    // pruned ones in step 2, so `rg_plan.front()` is the RG
                    // the decoder is about to read).
                    self.rg_plan.pop_front();
                    self.active_reader = Some(reader);
                    // The extracted reader now owns required bytes. Release any
                    // unused speculation (e.g. pages removed by a row filter).
                    if let Some(reservation) = self.prefetch_reservation.take() {
                        decoder.clear_all_ranges();
                        drop(reservation);
                    }
                    if let Some(options) = &self.row_group_prefetch {
                        match decoder.peek_next_row_group() {
                            Ok(Some(next)) => {
                                self.pending_prefetch = PrefetchedRowGroup::start(
                                    next,
                                    &self.parquet_metadata,
                                    self.decoder_projection.projection_mask(),
                                    options,
                                    Arc::clone(&self.reader),
                                    &self.prefetch_metrics,
                                );
                            }
                            Ok(None) => {}
                            Err(e) => return Some((Err(e.into()), self)),
                        }
                    }
                }
                Ok(DecodeResult::Finished) => return None,
                Err(e) => {
                    return Some((Err(DataFusionError::from(e)), self));
                }
            }
        }
    }

    /// Keep `rg_plan.front()` aligned with the row group the decoder will emit
    /// next. `try_next_reader` silently finishes row groups whose post-predicate
    /// selection is empty (no reader handed back), which would otherwise leave
    /// `rg_plan` trailing the decoder by one — a later prune/rebuild would then
    /// re-include an already-delivered row group (#24352).
    fn sync_rg_plan_to_decoder_frontier(&mut self) -> Result<()> {
        match self
            .decoder
            .as_ref()
            .expect("decoder present")
            .peek_next_row_group()
            .map_err(DataFusionError::from)?
        {
            Some(actual) => Self::advance_rg_plan_to(&mut self.rg_plan, actual)?,
            // Decoder has nothing left to emit — drain our plan so the stream
            // finishes cleanly.
            None => self.rg_plan.clear(),
        }
        Ok(())
    }

    /// Pop entries off `rg_plan` until its front is `target`.
    ///
    /// `target` is the RG the decoder will emit next and must still be in the
    /// plan. A missing `target` means the decoder's frontier and `rg_plan` have
    /// diverged; we surface that as an internal error rather than silently
    /// draining the plan, which would truncate the scan. Kept free-standing on
    /// `rg_plan` (rather than `&mut self`) so the pop/guard logic is
    /// unit-testable without constructing a full stream state.
    fn advance_rg_plan_to(rg_plan: &mut VecDeque<RgPlanEntry>, target: usize) -> Result<()> {
        while let Some(front) = rg_plan.front() {
            if front.rg_index == target {
                return Ok(());
            }
            rg_plan.pop_front();
        }
        internal_err!(
            "push decoder frontier RG {target} is not in rg_plan; \
             decoder and plan have diverged"
        )
    }

    /// Copies metrics from ArrowReaderMetrics (the metrics collected by the
    /// arrow-rs parquet reader) to the parquet file metrics for DataFusion
    fn copy_arrow_reader_metrics(&self) {
        if let Some(v) = self.arrow_reader_metrics.records_read_from_inner() {
            self.predicate_cache_inner_records.set(v);
        }
        if let Some(v) = self.arrow_reader_metrics.records_read_from_cache() {
            self.predicate_cache_records.set(v);
        }
    }

    fn project_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.decoder_projection.map(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use datafusion_pruning::MAX_IN_LIST_SIZE;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataPushDecoder;
    use parquet::file::properties::WriterProperties;

    /// Build a tiny in-memory Parquet file with three row groups whose `v`
    /// column statistics are disjoint: RG0 → 0..1000, RG1 → 1000..2000,
    /// RG2 → 2000..3000. Returns (metadata, schema).
    fn build_three_rg_file() -> (Arc<ParquetMetaData>, SchemaRef) {
        let (_, metadata, schema) = build_three_rg_file_data();
        (metadata, schema)
    }

    fn build_three_rg_file_data() -> (Bytes, Arc<ParquetMetaData>, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mut buf = Vec::new();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1000))
            .build();
        let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).unwrap();
        for rg in 0..3i64 {
            let base = rg * 1000;
            let vals: Vec<i64> = (base..base + 1000).collect();
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap();
            writer.write(&batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();

        let file = Bytes::from(buf);
        let len = file.len() as u64;
        let mut md = ParquetMetaDataPushDecoder::try_new(len).unwrap();
        // One range covering the whole file. Using `expect` rather than
        // `allow` per this crate's `clippy::allow-attributes` lint.
        #[expect(
            clippy::single_range_in_vec_init,
            reason = "we want a single range covering the whole file"
        )]
        let ranges = vec![0..len];
        md.push_ranges(ranges, vec![file.clone()]).unwrap();
        let DecodeResult::Data(meta) = md.try_decode().unwrap() else {
            panic!("decoding metadata");
        };
        assert_eq!(meta.num_row_groups(), 3, "test fixture must have 3 RGs");
        (file, Arc::new(meta), schema)
    }

    #[derive(Debug, Default)]
    struct ReadControl {
        calls: std::sync::atomic::AtomicUsize,
        requested: std::sync::Mutex<Vec<Range<u64>>>,
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        block_second: bool,
        fail_second: bool,
        latency: std::time::Duration,
    }

    #[derive(Debug, Clone)]
    struct TestReader {
        data: Bytes,
        metadata: Arc<ParquetMetaData>,
        control: Arc<ReadControl>,
    }

    impl AsyncFileReader for TestReader {
        fn get_bytes(
            &mut self,
            range: Range<u64>,
        ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Bytes>> {
            use futures::FutureExt;
            async move { Ok(self.data.slice(range.start as usize..range.end as usize)) }.boxed()
        }

        fn get_byte_ranges(
            &mut self,
            ranges: Vec<Range<u64>>,
        ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
            use futures::FutureExt;
            use std::sync::atomic::Ordering;
            async move {
                self.control
                    .requested
                    .lock()
                    .unwrap()
                    .extend(ranges.clone());
                let call = self.control.calls.fetch_add(1, Ordering::SeqCst);
                if call == 1 {
                    self.control.started.notify_one();
                    if self.control.block_second {
                        self.control.release.notified().await;
                    }
                    if self.control.fail_second {
                        return Err(parquet::errors::ParquetError::General(
                            "injected prefetch failure".into(),
                        ));
                    }
                }
                tokio::time::sleep(self.control.latency).await;
                Ok(ranges
                    .into_iter()
                    .map(|range| self.data.slice(range.start as usize..range.end as usize))
                    .collect())
            }
            .boxed()
        }

        fn get_metadata<'a>(
            &'a mut self,
            _options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
        ) -> futures::future::BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
            use futures::FutureExt;
            async move { Ok(Arc::clone(&self.metadata)) }.boxed()
        }
    }

    impl crate::ParquetFileReaderFactory for TestReader {
        fn create_reader(
            &self,
            _partition: usize,
            _file: datafusion_datasource::PartitionedFile,
            _hint: Option<usize>,
            _metrics: &ExecutionPlanMetricsSet,
        ) -> Result<Box<dyn AsyncFileReader + Send>> {
            Ok(Box::new(self.clone()))
        }
    }

    fn prefetch_test_stream(
        budget: usize,
        pool: Arc<dyn MemoryPool>,
        control: Arc<ReadControl>,
        limit: Option<usize>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_execution::SendableRecordBatchStream {
        prefetch_test_stream_with_access(budget, pool, control, limit, predicate, None, None)
    }

    fn prefetch_test_stream_with_access(
        budget: usize,
        pool: Arc<dyn MemoryPool>,
        control: Arc<ReadControl>,
        limit: Option<usize>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        access: Option<datafusion_datasource_parquet::ParquetAccessPlan>,
        selection: Option<crate::ParquetRowSelection>,
    ) -> datafusion_execution::SendableRecordBatchStream {
        use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
        use datafusion_datasource::source::DataSourceExec;
        use datafusion_datasource::{PartitionedFile, file_groups::FileGroup};
        use datafusion_execution::{
            TaskContext, config::SessionConfig, object_store::ObjectStoreUrl,
        };
        use datafusion_physical_plan::ExecutionPlan;
        let (data, metadata, schema) = build_three_rg_file_data();
        let mut file = PartitionedFile::new("prefetch.parquet", data.len() as u64);
        if let Some(access) = access {
            file = file.with_extension(access);
        }
        if let Some(selection) = selection {
            file = file.with_extension(selection);
        }
        let mut source = crate::source::ParquetSource::new(schema)
            .with_row_group_prefetch(budget, pool)
            .with_pushdown_filters(true)
            .with_parquet_file_reader_factory(Arc::new(TestReader {
                data,
                metadata,
                control,
            }));
        if let Some(predicate) = predicate {
            source = source.with_predicate(predicate);
        }
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
                .with_file_group(FileGroup::new(vec![file]))
                .with_limit(limit)
                .build();
        let task =
            TaskContext::default().with_session_config(SessionConfig::new().with_batch_size(100));
        DataSourceExec::new(Arc::new(config))
            .execute(0, Arc::new(task))
            .unwrap()
    }

    async fn assert_pool_released(pool: &Arc<dyn MemoryPool>) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pool.reserved() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn prefetch_overlaps_decode_and_preserves_order() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let control = Arc::new(ReadControl {
            block_second: true,
            ..Default::default()
        });
        let mut stream =
            prefetch_test_stream(1 << 20, Arc::clone(&pool), Arc::clone(&control), None, None);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 100);
        // No further polling of the scan: next-RG I/O must start independently.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            control.started.notified(),
        )
        .await
        .unwrap();
        assert!(pool.reserved() > 0);
        let mut batches = vec![first];
        // The current reader must keep producing while the next fetch is blocked.
        for _ in 1..10 {
            batches.push(
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
            );
        }
        control.release.notify_one();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, (0..3000).collect::<Vec<_>>());
        assert_pool_released(&pool).await;
    }

    #[tokio::test]
    async fn prefetch_budget_pool_pressure_limit_and_cancellation() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use std::sync::atomic::Ordering;
        for (budget, capacity, limit) in [
            (0, 1 << 20, None),
            (1, 1 << 20, None),
            (1 << 20, 0, None),
            (1 << 20, 1 << 20, Some(100)),
        ] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(capacity));
            let control = Arc::new(ReadControl::default());
            let mut stream =
                prefetch_test_stream(budget, Arc::clone(&pool), Arc::clone(&control), limit, None);
            assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 100);
            tokio::task::yield_now().await;
            assert_eq!(pool.reserved(), 0);
            assert_eq!(control.calls.load(Ordering::SeqCst), 1);
            drop(stream);
        }
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let control = Arc::new(ReadControl {
            block_second: true,
            ..Default::default()
        });
        let mut stream =
            prefetch_test_stream(1 << 20, Arc::clone(&pool), Arc::clone(&control), None, None);
        stream.next().await.unwrap().unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            control.started.notified(),
        )
        .await
        .unwrap();
        assert!(pool.reserved() > 0);
        drop(stream); // The blocked read must be aborted without releasing its gate.
        assert_pool_released(&pool).await;
    }

    #[tokio::test]
    async fn prefetch_failure_retries_on_demand() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use std::sync::atomic::Ordering;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let control = Arc::new(ReadControl {
            fail_second: true,
            ..Default::default()
        });
        let mut stream =
            prefetch_test_stream(1 << 20, Arc::clone(&pool), Arc::clone(&control), None, None);
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        assert_eq!(rows, 3000);
        assert_eq!(control.calls.load(Ordering::SeqCst), 4);
        assert_pool_released(&pool).await;
    }

    #[tokio::test]
    async fn prefetch_cancels_a_row_group_pruned_while_decoding() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let control = Arc::new(ReadControl {
            block_second: true,
            ..Default::default()
        });
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("v", 0))],
            gt_predicate(-1),
        ));
        let mut stream = prefetch_test_stream(
            1 << 20,
            Arc::clone(&pool),
            Arc::clone(&control),
            None,
            Some(Arc::clone(&dynamic) as _),
        );
        stream.next().await.unwrap().unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            control.started.notified(),
        )
        .await
        .unwrap();
        dynamic.update(gt_predicate(2500)).unwrap();
        // RG1 is now prunable. Its blocked prefetch must be cancelled, not awaited.
        let rows = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut rows = 100;
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }
            rows
        })
        .await
        .unwrap();
        assert_eq!(rows, 1499); // RG0 already active; 499 rows in RG2 pass the filter.
        assert_pool_released(&pool).await;
    }

    #[tokio::test]
    async fn prefetch_with_row_filter_matches_demand_reads() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        // The modulo filter empties RG1 without statistics pruning, exercising
        // the decoder advancing past a prefetched group without yielding a reader.
        let modulo = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("v", 0)),
                Operator::Modulo,
                lit(2000i64),
            )),
            Operator::Lt,
            lit(1000i64),
        )) as Arc<dyn PhysicalExpr>;
        for (predicate, expected) in [
            (gt_predicate(1500), (1501..3000).collect::<Vec<i64>>()),
            (modulo, (0..1000).chain(2000..3000).collect()),
        ] {
            for budget in [0, 1 << 20] {
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
                let mut stream = prefetch_test_stream(
                    budget,
                    Arc::clone(&pool),
                    Arc::new(ReadControl::default()),
                    None,
                    Some(Arc::clone(&predicate)),
                );
                let mut values = Vec::new();
                while let Some(batch) = stream.next().await {
                    values.extend_from_slice(
                        batch
                            .unwrap()
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values(),
                    );
                }
                assert_eq!(values, expected);
                assert_pool_released(&pool).await;
            }
        }
    }

    #[tokio::test]
    async fn prefetch_honors_external_access_plans_and_row_selections() {
        use datafusion_datasource_parquet::{ParquetAccessPlan, RowGroupAccess};
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use parquet::arrow::arrow_reader::RowSelector;
        for budget in [0, 1 << 20] {
            for selected_rows in [false, true] {
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
                let control = Arc::new(ReadControl::default());
                let (access, selection, expected) = if selected_rows {
                    (
                        None,
                        Some(crate::ParquetRowSelection::new(
                            vec![
                                RowSelector::skip(500),
                                RowSelector::select(1700),
                                RowSelector::skip(800),
                            ]
                            .into(),
                        )),
                        (500..2200).collect::<Vec<i64>>(),
                    )
                } else {
                    (
                        Some(ParquetAccessPlan::new(vec![
                            RowGroupAccess::Scan,
                            RowGroupAccess::Skip,
                            RowGroupAccess::Scan,
                        ])),
                        None,
                        (0..1000).chain(2000..3000).collect(),
                    )
                };
                let mut stream = prefetch_test_stream_with_access(
                    budget,
                    Arc::clone(&pool),
                    Arc::clone(&control),
                    None,
                    None,
                    access,
                    selection,
                );
                let mut values = Vec::new();
                while let Some(batch) = stream.next().await {
                    values.extend_from_slice(
                        batch
                            .unwrap()
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values(),
                    );
                }
                assert_eq!(values, expected);
                assert_pool_released(&pool).await;
                if !selected_rows {
                    let (_, metadata, _) = build_three_rg_file_data();
                    let (start, len) = metadata.row_group(1).column(0).byte_range();
                    assert!(
                        control
                            .requested
                            .lock()
                            .unwrap()
                            .iter()
                            .all(|r| r.end <= start || r.start >= start + len),
                        "Neither demand nor background I/O may read an excluded row group"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn invalid_external_access_fails_before_data_io() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
        let control = Arc::new(ReadControl::default());
        let mut stream = prefetch_test_stream_with_access(
            1 << 20,
            Arc::clone(&pool),
            Arc::clone(&control),
            None,
            None,
            Some(datafusion_datasource_parquet::ParquetAccessPlan::new_all(2)),
            None,
        );
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Specified 2 row groups, but file has 3")
        );
        assert!(control.requested.lock().unwrap().is_empty());
        drop(stream);
        assert_pool_released(&pool).await;
    }

    /// Controlled scheduling experiment, not a production throughput benchmark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "manual benchmark with simulated I/O and batch processing latency"]
    async fn prefetch_latency_benchmark() {
        use datafusion_common::instant::Instant;
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        use std::time::Duration;
        for budget in [0, 1 << 20] {
            let mut times = Vec::new();
            for _ in 0..5 {
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1 << 20));
                let control = Arc::new(ReadControl {
                    latency: Duration::from_millis(40),
                    ..Default::default()
                });
                let mut stream =
                    prefetch_test_stream(budget, Arc::clone(&pool), control, None, None);
                let start = Instant::now();
                let mut rows = 0;
                while let Some(batch) = stream.next().await {
                    rows += batch.unwrap().num_rows();
                    // Simulate synchronous downstream processing on this worker.
                    std::thread::sleep(Duration::from_millis(4));
                }
                assert_eq!(rows, 3000);
                times.push(start.elapsed());
                assert_pool_released(&pool).await;
            }
            times.sort();
            println!("prefetch budget={budget}, median={:?}", times[2]);
        }
    }

    /// Create a fresh `(creation_errors, evaluation_errors)` counter pair
    /// for tests. The names mirror the two metrics
    /// [`RowGroupPruner::new`] consumes — predicate construction is
    /// accounted separately from per-row-group evaluation.
    fn pruner_error_counters() -> (Count, Count) {
        let metrics = ExecutionPlanMetricsSet::new();
        let creation = MetricBuilder::new(&metrics).counter("num_predicate_creation_errors", 0);
        let evaluation = MetricBuilder::new(&metrics).counter("predicate_evaluation_errors", 0);
        (creation, evaluation)
    }

    /// `v > literal` predicate on a single-column schema.
    fn gt_predicate(threshold: i64) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("v", 0)),
            Operator::Gt,
            lit(ScalarValue::Int64(Some(threshold))),
        ))
    }

    #[test]
    fn row_group_pruner_skips_only_disqualified_row_groups() {
        let (meta, schema) = build_three_rg_file();
        let (creation, evaluation) = pruner_error_counters();
        let mut pruner = RowGroupPruner::new(
            gt_predicate(1500),
            Arc::clone(&schema),
            Arc::clone(&meta),
            creation,
            evaluation,
            MAX_IN_LIST_SIZE,
        );

        // RG0 (0..1000) is entirely below threshold → fully prunable.
        assert!(pruner.should_prune(&[0]), "RG0 should be pruned");
        // RG1 (1000..2000) straddles the threshold → not safe to prune.
        assert!(!pruner.should_prune(&[1]), "RG1 must NOT be pruned");
        // RG2 (2000..3000) is entirely above threshold → keep.
        assert!(!pruner.should_prune(&[2]), "RG2 must NOT be pruned");
        // Run covering both RG0 and RG1 cannot be skipped — RG1 is alive.
        assert!(
            !pruner.should_prune(&[0, 1]),
            "mixed run with a live RG must NOT be pruned"
        );
        // Empty input is a no-op (defensive guard).
        assert!(!pruner.should_prune(&[]));
    }

    #[test]
    fn row_group_pruner_tracks_dynamic_filter_updates() {
        let (meta, schema) = build_three_rg_file();
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("v", 0))],
            gt_predicate(500),
        ));
        let (creation, evaluation) = pruner_error_counters();
        let mut pruner = RowGroupPruner::new(
            Arc::clone(&dynamic) as Arc<dyn PhysicalExpr>,
            Arc::clone(&schema),
            Arc::clone(&meta),
            creation,
            evaluation,
            MAX_IN_LIST_SIZE,
        );

        // Initial threshold 500 → only the lower half of RG0 fails, so RG0
        // (0..1000) straddles the threshold and stays alive.
        assert!(!pruner.should_prune(&[0]));
        assert!(!pruner.should_prune(&[1]));

        // Tighten the threshold via the dynamic filter — TopK fills its
        // heap and updates the threshold to 2500.
        dynamic
            .update(gt_predicate(2500))
            .expect("update threshold");

        // After the update the pruner must rebuild its `PruningPredicate`
        // (driven by the `DynamicFilterTracker`'s change notification) and
        // re-evaluate. RG0 and RG1 are both entirely below 2500 now.
        assert!(
            pruner.should_prune(&[0]),
            "RG0 must be pruned after threshold tightens to 2500"
        );
        assert!(
            pruner.should_prune(&[1]),
            "RG1 must be pruned after threshold tightens to 2500"
        );
        assert!(
            !pruner.should_prune(&[2]),
            "RG2 (2000..3000) still straddles 2500"
        );
    }

    #[test]
    fn row_group_pruner_falls_back_to_conservative_when_predicate_has_no_bounds() {
        // A predicate the pruning analyzer can't decompose (e.g. a bare
        // column reference of bool type would normally be valid, but a
        // non-binary expression on a non-bool column doesn't yield bounds).
        // We use `lit(true)` which produces no column references, so
        // `build_pruning_predicate` will return None.
        let (meta, schema) = build_three_rg_file();
        let (creation, evaluation) = pruner_error_counters();
        let mut pruner = RowGroupPruner::new(
            lit(true) as Arc<dyn PhysicalExpr>,
            Arc::clone(&schema),
            Arc::clone(&meta),
            creation,
            evaluation,
            MAX_IN_LIST_SIZE,
        );
        // No pruning predicate could be built → conservatively keep RGs.
        assert!(!pruner.should_prune(&[0]));
        assert!(!pruner.should_prune(&[1]));
        assert!(!pruner.should_prune(&[2]));
    }

    #[test]
    fn advance_rg_plan_to_pops_up_to_target() {
        let mut plan: VecDeque<RgPlanEntry> = [0usize, 1, 2, 3]
            .into_iter()
            .map(|rg_index| RgPlanEntry { rg_index })
            .collect();
        PushDecoderStreamState::advance_rg_plan_to(&mut plan, 2).unwrap();
        assert_eq!(
            plan.iter().map(|e| e.rg_index).collect::<Vec<_>>(),
            vec![2, 3],
            "must pop the entries before `target` and stop at it",
        );
    }

    #[test]
    fn advance_rg_plan_to_errors_when_target_absent() {
        let mut plan: VecDeque<RgPlanEntry> = [0usize, 1, 2]
            .into_iter()
            .map(|rg_index| RgPlanEntry { rg_index })
            .collect();
        let err = PushDecoderStreamState::advance_rg_plan_to(&mut plan, 5)
            .expect_err("a target absent from the plan must be an internal error");
        assert!(
            err.to_string().contains("diverged"),
            "expected a divergence internal error, got: {err}",
        );
    }
}
