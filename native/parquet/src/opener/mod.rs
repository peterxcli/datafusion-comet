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

//! [`ParquetMorselizer`] state machines for opening Parquet files

mod early_stop;
mod encryption;

use self::early_stop::EarlyStoppingStream;
#[cfg(feature = "parquet_encryption")]
use self::encryption::EncryptionContext;
use crate::access_plan::PreparedAccessPlan;
use crate::decoder_projection::DecoderProjection;
use crate::page_filter::PagePruningAccessPlanFilter;
use crate::push_decoder::{
    DecoderBuilderConfig, PushDecoderStreamState, RgPlanEntry, RowGroupPrefetchOptions,
    RowGroupPruner,
};
use crate::row_filter::RowFilterGenerator;
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::{
    BloomFilterStatistics, Int96Coercer, ParquetAccessPlan, ParquetFileMetrics,
    ParquetFileReaderFactory, ParquetRowSelection, ParquetVirtualColumn,
    apply_file_schema_type_coercions,
};
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr_adapter::replace_columns_with_literals;
use datafusion_physical_expr_adapter::rewrite::rewrite_input_file_name_in_projection;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::mem;
use std::sync::Arc;

use arrow::datatypes::{FieldRef, Schema, SchemaRef, TimeUnit};
#[cfg(feature = "parquet_encryption")]
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{
    ColumnStatistics, HashSet, Result, ScalarValue, Statistics, exec_err, internal_err,
};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_physical_expr::expressions::{Column, DynamicFilterTracking};
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
};
use datafusion_pruning::{FilePruner, PruningPredicate, PruningPredicateBuilder};

#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::BoxStream};
use log::debug;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::parquet_column;
use parquet::basic::Type;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader, RowGroupMetaData};

/// Morselizer-level state for virtual columns, precomputed once per scan
/// partition so each file skips the validator walks, `null_replacements`
/// rebuild, and one of the `append_fields` allocations.
///
/// Only constructed when the scan actually requests virtual columns;
/// [`ParquetMorselizer`] and [`PreparedParquetOpen`] hold
/// `Option<Arc<VirtualColumnsState>>` so the zero-virtual-column path (the
/// common case) pays nothing.
pub(crate) struct VirtualColumnsState {
    /// Shared list of virtual column fields. Cloned as a `Vec` only at the
    /// arrow-rs `with_virtual_columns` call site, which takes it by value.
    virtual_columns: Arc<Vec<FieldRef>>,
    /// Null-literal substitutions keyed by virtual column name, used to strip
    /// virtual-column references from the projection fed into
    /// `build_projection_read_plan` (which only understands file columns).
    null_replacements: HashMap<String, ScalarValue>,
    /// `logical_file_schema` with the virtual columns appended. Fed into the
    /// per-file expression rewriter so virtual-column references
    /// identity-rewrite instead of being replaced with null literals.
    logical_schema_with_virtual: SchemaRef,
}

impl VirtualColumnsState {
    /// Validate each field carries a supported arrow virtual extension type
    /// and precompute the per-scan derived state.
    fn try_new(virtual_columns: Vec<FieldRef>, logical_file_schema: &SchemaRef) -> Result<Self> {
        // Gate which extension types we forward to arrow-rs. Adding a new
        // supported virtual column means adding a `ParquetVirtualColumn`
        // variant — not editing a stringly-typed allowlist here.
        for field in &virtual_columns {
            ParquetVirtualColumn::try_from(field)?;
        }
        let null_replacements = virtual_columns
            .iter()
            .map(|f| ScalarValue::try_from(f.data_type()).map(|v| (f.name().clone(), v)))
            .collect::<Result<HashMap<String, ScalarValue>>>()?;
        let logical_schema_with_virtual = append_fields(logical_file_schema, &virtual_columns);
        Ok(Self {
            virtual_columns: Arc::new(virtual_columns),
            null_replacements,
            logical_schema_with_virtual,
        })
    }

    /// Validated virtual column fields, in declaration order.
    pub(crate) fn virtual_columns(&self) -> &[FieldRef] {
        &self.virtual_columns
    }

    /// Null-literal substitutions keyed by virtual column name. Used to strip
    /// virtual-column references from a projection before it is fed into the
    /// parquet `ProjectionMask` (which only understands file columns).
    pub(crate) fn null_replacements(&self) -> &HashMap<String, ScalarValue> {
        &self.null_replacements
    }
}

/// Build the per-scan virtual-column state.
///
/// Two checks run here:
/// - Extension-type allowlist via [`VirtualColumnsState::try_new`]: returns
///   `Err` for unsupported virtual extension types.
/// - Predicate-reference check (when pushdown is enabled): returns `Err` if
///   the predicate references a virtual column. The contract is that callers
///   route filters through
///   [`ParquetSource::try_pushdown_filters`](crate::source::ParquetSource),
///   which classifies virtual-col filters as `PushedDown::No`. Erroring here
///   prevents silent wrong results for callers that bypass that path and set
///   the predicate directly on `ParquetSource`.
///
/// Returns `None` when the scan has no virtual columns, so callers avoid
/// allocating the shared state on the common path.
pub(crate) fn build_virtual_columns_state(
    virtual_columns: &[FieldRef],
    logical_file_schema: &SchemaRef,
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    pushdown_filters: bool,
) -> Result<Option<Arc<VirtualColumnsState>>> {
    if virtual_columns.is_empty() {
        return Ok(None);
    }
    if pushdown_filters && let Some(predicate) = predicate {
        validate_predicate_does_not_reference_virtual_columns(predicate, virtual_columns)?;
    }
    let state = VirtualColumnsState::try_new(virtual_columns.to_vec(), logical_file_schema)?;
    Ok(Some(Arc::new(state)))
}

/// Return `base` unchanged when `extra` is empty; otherwise build a new schema
/// with `extra` appended to `base`'s fields.
pub(crate) fn append_fields(base: &SchemaRef, extra: &[FieldRef]) -> SchemaRef {
    if extra.is_empty() {
        return Arc::clone(base);
    }
    let fields = base
        .fields()
        .iter()
        .cloned()
        .chain(extra.iter().cloned())
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

/// Reject predicates that reference a virtual column.
///
/// arrow-rs's `RowFilter` evaluates predicates against a `ProjectionMask` that
/// addresses parquet leaves only; virtual columns (e.g. `row_number`) are
/// synthesized by the reader *after* filter evaluation and cannot be referenced
/// inside a row filter. Silently dropping such a predicate would produce wrong
/// results.
fn validate_predicate_does_not_reference_virtual_columns(
    predicate: &Arc<dyn PhysicalExpr>,
    virtual_columns: &[FieldRef],
) -> Result<()> {
    if virtual_columns.is_empty() {
        return Ok(());
    }
    let virtual_names: HashSet<&str> = virtual_columns.iter().map(|f| f.name().as_str()).collect();
    let mut offender: Option<String> = None;
    predicate.apply(|node: &Arc<dyn PhysicalExpr>| {
        if let Some(column) = node.downcast_ref::<Column>()
            && virtual_names.contains(column.name())
        {
            offender = Some(column.name().to_string());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(name) = offender {
        return internal_err!(
            "Predicate references virtual column '{name}'; route via \
             ParquetSource::try_pushdown_filters."
        );
    }
    Ok(())
}

/// Stateless Parquet morselizer implementation.
///
/// Reading a Parquet file is a multi-stage process, with multiple CPU-intensive
/// steps interspersed with I/O steps. The code in this module implements the steps
/// as an explicit state machine -- see [`ParquetOpenState`] for details.
#[derive(Clone)]
pub(super) struct ParquetMorselizer {
    pub(crate) row_group_prefetch: Option<RowGroupPrefetchOptions>,
    /// Execution partition index
    pub(crate) partition_index: usize,
    /// Projection to apply on top of the table schema (i.e. can reference partition columns).
    pub projection: ProjectionExprs,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub(crate) limit: Option<usize>,
    /// If should keep the output rows in order
    pub preserve_order: bool,
    /// Optional predicate to apply during the scan
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Table schema, including partition columns.
    pub table_schema: TableSchema,
    /// Optional hint for how large the initial request to read parquet metadata
    /// should be
    pub metadata_size_hint: Option<usize>,
    /// Metrics for reporting
    pub metrics: ExecutionPlanMetricsSet,
    /// Factory for instantiating parquet reader
    pub parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Should the filters be evaluated during the parquet scan using
    /// [`DatafusionArrowPredicate`](crate::row_filter::DatafusionArrowPredicate)?
    pub pushdown_filters: bool,
    /// Should the filters be reordered to optimize the scan?
    pub reorder_filters: bool,
    /// Should we force the reader to use RowSelections for filtering
    pub force_filter_selections: bool,
    /// Should the page index be read from parquet files, if present, to skip
    /// data pages
    pub enable_page_index: bool,
    /// Should the bloom filter be read from parquet, if present, to skip row
    /// groups
    pub enable_bloom_filter: bool,
    /// Should row group pruning be applied
    pub enable_row_group_stats_pruning: bool,
    /// Coerce INT96 timestamps to specific TimeUnit
    pub coerce_int96: Option<TimeUnit>,
    /// Optional timezone applied to INT96-coerced timestamps. When `Some`, the
    /// coerced column type becomes `Timestamp(<coerce_int96>, Some(<tz>))`.
    /// No effect when `coerce_int96` is `None`.
    pub coerce_int96_tz: Option<Arc<str>>,
    /// Optional parquet FileDecryptionProperties
    #[cfg(feature = "parquet_encryption")]
    pub file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    /// Rewrite expressions in the context of the file schema
    pub(crate) expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    /// Optional factory to create file decryption properties dynamically
    #[cfg(feature = "parquet_encryption")]
    pub encryption_factory: Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
    /// Maximum size of the predicate cache, in bytes. If none, uses
    /// the arrow-rs default.
    pub max_predicate_cache_size: Option<usize>,
    /// Maximum `IN (...)` list size that the pruning predicate will rewrite
    /// into per-value statistics checks. Lists longer than this skip
    /// container-level pruning. Sourced from
    /// `datafusion.execution.parquet.max_in_list_size`.
    pub max_in_list_size: usize,
    /// Whether to read row groups in reverse order
    pub reverse_row_groups: bool,
    /// Optional sort order used to reorder row groups by their min/max statistics.
    pub sort_order_for_reorder: Option<LexOrdering>,
    /// Per-scan virtual-column state (validation already performed). `None`
    /// when no virtual columns are requested — the common path.
    pub(crate) virtual_state: Option<Arc<VirtualColumnsState>>,
}

impl fmt::Debug for ParquetMorselizer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetMorselizer")
            .field("partition_index", &self.partition_index)
            .field("preserve_order", &self.preserve_order)
            .field("enable_page_index", &self.enable_page_index)
            .field("enable_bloom_filter", &self.enable_bloom_filter)
            .finish()
    }
}

impl Morselizer for ParquetMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        Ok(Box::new(ParquetMorselPlanner::try_new(self, file)?))
    }
}

/// States for [`ParquetMorselPlanner`]
///
/// These states correspond to the steps required to read and apply various
/// filter operations.
///
/// States whose names beginning with `Load` represent waiting on IO to resolve
///
/// ```text
///      Start
///        |
///        v
/// [LoadEncryption]?
///        |
///        v
///    PruneFile
///        |
///        v
///   LoadMetadata
///        |
///        v
///  PrepareFilters
///        |
///        v
/// PruneWithStatistics
///        |
///        v
///   LoadPageIndex?   (skipped when all surviving row groups are fully matched)
///        |
///        v
///  LoadBloomFilters
///        |
///        v
/// PruneWithBloomFilters
///        |
///        v
///   BuildStream
///        |
///        v
///       Done
/// ```
///
/// Note: `LoadEncryption` is only present when the `parquet_encryption` feature is
/// enabled. All other states are always visited in the order shown above,
/// though any async state may return `Poll::Pending` and then resume later.
enum ParquetOpenState {
    Start {
        prepared: Box<PreparedParquetOpen>,
        #[cfg(feature = "parquet_encryption")]
        encryption_context: Arc<EncryptionContext>,
    },
    /// Loading encryption footers
    #[cfg(feature = "parquet_encryption")]
    LoadEncryption(BoxFuture<'static, Result<Box<PreparedParquetOpen>>>),
    /// Try to prune file using only file-level statistics and partition
    /// values before loading any parquet metadata
    PruneFile(Box<PreparedParquetOpen>),
    /// Loading Parquet metadata (in footer)
    LoadMetadata(BoxFuture<'static, Result<MetadataLoadedParquetOpen>>),
    /// Specialize any filters for the actual file schema (only known after
    /// metadata is loaded)
    PrepareFilters(Box<MetadataLoadedParquetOpen>),
    /// Pruning Row Groups
    PruneWithStatistics(Box<FiltersPreparedParquetOpen>),
    /// Loading [Parquet Page Index](https://parquet.apache.org/docs/file-format/pageindex/)
    LoadPageIndex(BoxFuture<'static, Result<RowGroupsPrunedParquetOpen>>),
    /// Loading bloom filters required for row-group pruning
    LoadBloomFilters(BoxFuture<'static, Result<BloomFiltersLoadedParquetOpen>>),
    /// Pruning with preloaded Bloom Filters
    PruneWithBloomFilters(Box<BloomFiltersLoadedParquetOpen>),
    /// Builds the final reader stream
    ///
    /// TODO: split state as this currently does both I/O and CPU work.
    BuildStream(Box<RowGroupsPrunedParquetOpen>),
    /// Terminal state: the final opened stream is ready to return.
    Ready(BoxStream<'static, Result<RecordBatch>>),
    /// Terminal state: reading complete
    Done,
}

impl fmt::Debug for ParquetOpenState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = match self {
            ParquetOpenState::Start { .. } => "Start",
            #[cfg(feature = "parquet_encryption")]
            ParquetOpenState::LoadEncryption(_) => "LoadEncryption",
            ParquetOpenState::PruneFile(_) => "PruneFile",
            ParquetOpenState::LoadMetadata(_) => "LoadMetadata",
            ParquetOpenState::PrepareFilters(_) => "PrepareFilters",
            ParquetOpenState::LoadPageIndex(_) => "LoadPageIndex",
            ParquetOpenState::PruneWithStatistics(_) => "PruneWithStatistics",
            ParquetOpenState::LoadBloomFilters(_) => "LoadBloomFilters",
            ParquetOpenState::PruneWithBloomFilters(_) => "PruneWithBloomFilters",
            ParquetOpenState::BuildStream(_) => "BuildStream",
            ParquetOpenState::Ready(_) => "Ready",
            ParquetOpenState::Done => "Done",
        };
        f.write_str(state)
    }
}

struct PreparedParquetOpen {
    row_group_prefetch: Option<RowGroupPrefetchOptions>,
    partition_index: usize,
    partitioned_file: PartitionedFile,
    file_range: Option<datafusion_datasource::FileRange>,
    extensions: datafusion_datasource::FileExtensions,
    file_name: String,
    file_metrics: ParquetFileMetrics,
    baseline_metrics: BaselineMetrics,
    file_pruner: Option<FilePruner>,
    metadata_size_hint: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    parquet_file_reader_factory: Arc<dyn ParquetFileReaderFactory>,
    async_file_reader: Box<dyn AsyncFileReader>,
    batch_size: usize,
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    output_schema: SchemaRef,
    projection: ProjectionExprs,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Per-scan virtual-column state, Arc-cloned from [`ParquetMorselizer`] so
    /// each file shares validated fields, precomputed null replacements, and
    /// the logical-with-virtual schema. `None` when no virtual columns were
    /// requested.
    virtual_state: Option<Arc<VirtualColumnsState>>,
    reorder_predicates: bool,
    pushdown_filters: bool,
    force_filter_selections: bool,
    enable_page_index: bool,
    enable_bloom_filter: bool,
    enable_row_group_stats_pruning: bool,
    limit: Option<usize>,
    coerce_int96: Option<TimeUnit>,
    coerce_int96_tz: Option<Arc<str>>,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    predicate_creation_errors: Count,
    max_predicate_cache_size: Option<usize>,
    max_in_list_size: usize,
    reverse_row_groups: bool,
    sort_order_for_reorder: Option<LexOrdering>,
    preserve_order: bool,
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
}

/// State of [`ParquetOpenState`]
///
/// Result of loading parquet metadata after file-level pruning is complete.
struct MetadataLoadedParquetOpen {
    prepared: PreparedParquetOpen,
    reader_metadata: ArrowReaderMetadata,
    options: ArrowReaderOptions,
}

/// State of [`ParquetOpenState`]
///
/// Pruning Predicate and DataPage pruning information
/// specialized for the files specific schema.
struct FiltersPreparedParquetOpen {
    loaded: MetadataLoadedParquetOpen,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
}

/// State of [`ParquetOpenState`]
///
/// Result of CPU-only row-group pruning before optional bloom-filter I/O.
struct RowGroupsPrunedParquetOpen {
    prepared: FiltersPreparedParquetOpen,
    row_groups: RowGroupAccessPlanFilter,
}

/// State of [`ParquetOpenState`]
///
/// Result of loading bloom filters needed for row-group pruning.
struct BloomFiltersLoadedParquetOpen {
    prepared: RowGroupsPrunedParquetOpen,
    /// Bloom filters loaded for each row group that remains under consideration.
    ///
    /// indexed by parquet row-group index
    row_group_bloom_filters: Vec<BloomFilterStatistics>,
}

impl ParquetOpenState {
    /// Applies one CPU-only state transition.
    ///
    /// `Load*` states do not transition here and are returned unchanged so the
    /// driver loop can poll their inner futures separately.
    ///
    /// Implements state machine described in [`ParquetOpenState`]
    fn transition(self) -> Result<ParquetOpenState> {
        match self {
            ParquetOpenState::Start {
                prepared,
                #[cfg(feature = "parquet_encryption")]
                encryption_context,
            } => {
                #[cfg(feature = "parquet_encryption")]
                {
                    let mut prepared = *prepared;
                    let future = async move {
                        let file_location = &prepared.partitioned_file.object_meta.location;
                        prepared.file_decryption_properties = encryption_context
                            .get_file_decryption_properties(file_location)
                            .await?;
                        Ok(Box::new(prepared))
                    }
                    .boxed();
                    Ok(ParquetOpenState::LoadEncryption(future))
                }
                #[cfg(not(feature = "parquet_encryption"))]
                {
                    Ok(ParquetOpenState::PruneFile(prepared))
                }
            }
            #[cfg(feature = "parquet_encryption")]
            ParquetOpenState::LoadEncryption(future) => {
                Ok(ParquetOpenState::LoadEncryption(future))
            }
            ParquetOpenState::PruneFile(prepared) => {
                let Some(prepared) = (*prepared).prune_file()? else {
                    return Ok(ParquetOpenState::Done);
                };
                Ok(ParquetOpenState::LoadMetadata(prepared.load().boxed()))
            }
            ParquetOpenState::LoadMetadata(future) => Ok(ParquetOpenState::LoadMetadata(future)),
            ParquetOpenState::PrepareFilters(loaded) => {
                let prepared_filters = loaded.prepare_filters()?;
                Ok(ParquetOpenState::PruneWithStatistics(Box::new(
                    prepared_filters,
                )))
            }
            ParquetOpenState::PruneWithStatistics(prepared) => {
                let prepared_row_groups = (*prepared).prune_row_groups()?;
                if prepared_row_groups.should_load_page_index() {
                    Ok(ParquetOpenState::LoadPageIndex(
                        prepared_row_groups.load_page_index().boxed(),
                    ))
                } else {
                    if prepared_row_groups
                        .prepared
                        .page_pruning_predicate
                        .is_some()
                        && !prepared_row_groups.row_groups.is_empty()
                    {
                        let prepared = &prepared_row_groups.prepared.loaded.prepared;
                        crate::metrics::add_page_index_load_skipped(
                            &prepared.metrics,
                            prepared.partition_index,
                            &prepared.file_name,
                            1,
                        );
                    }
                    Ok(ParquetOpenState::LoadBloomFilters(
                        prepared_row_groups.load_bloom_filters().boxed(),
                    ))
                }
            }
            ParquetOpenState::LoadPageIndex(future) => Ok(ParquetOpenState::LoadPageIndex(future)),
            ParquetOpenState::LoadBloomFilters(future) => {
                Ok(ParquetOpenState::LoadBloomFilters(future))
            }
            ParquetOpenState::PruneWithBloomFilters(loaded) => Ok(ParquetOpenState::BuildStream(
                Box::new(loaded.prune_bloom_filters()),
            )),
            ParquetOpenState::BuildStream(prepared) => {
                Ok(ParquetOpenState::Ready(prepared.build_stream()?))
            }
            ParquetOpenState::Ready(stream) => Ok(ParquetOpenState::Ready(stream)),
            ParquetOpenState::Done => {
                panic!("ParquetOpenFuture polled after completion");
            }
        }
    }
}

/// Implements the Morsel API
struct ParquetStreamMorsel {
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl ParquetStreamMorsel {
    fn new(stream: BoxStream<'static, Result<RecordBatch>>) -> Self {
        Self { stream }
    }
}

impl fmt::Debug for ParquetStreamMorsel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetStreamMorsel")
            .finish_non_exhaustive()
    }
}

impl Morsel for ParquetStreamMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }
}

/// Per-file planner that owns the current [`ParquetOpenState`].
struct ParquetMorselPlanner {
    /// Ready to perform CPU-only planning work.
    state: ParquetOpenState,
}

impl fmt::Debug for ParquetMorselPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ParquetMorselPlanner::Ready")
            .field(&self.state)
            .finish()
    }
}

impl ParquetMorselPlanner {
    fn try_new(morselizer: &ParquetMorselizer, file: PartitionedFile) -> Result<Self> {
        let prepared = morselizer.prepare_open_file(file)?;
        #[cfg(feature = "parquet_encryption")]
        let state = ParquetOpenState::Start {
            prepared: Box::new(prepared),
            encryption_context: Arc::new(morselizer.get_encryption_context()),
        };
        #[cfg(not(feature = "parquet_encryption"))]
        let state = ParquetOpenState::Start {
            prepared: Box::new(prepared),
        };
        Ok(Self { state })
    }

    /// Schedule an I/O future that resolves to the next planner to run.
    ///
    /// This helper
    ///
    /// 1. drives one I/O phase to completion
    /// 2. wraps the resulting state in a new [`ParquetMorselPlanner`]
    /// 3. returns a [`MorselPlan`] containing the boxed future for the caller
    ///    to poll
    ///
    fn schedule_io<F>(future: F) -> MorselPlan
    where
        F: Future<Output = Result<ParquetOpenState>> + Send + 'static,
    {
        let io_future = async move {
            let next_state = future.await?;
            Ok(Box::new(ParquetMorselPlanner { state: next_state }) as _)
        };
        MorselPlan::new().with_pending_planner(io_future)
    }
}

impl MorselPlanner for ParquetMorselPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        if let ParquetOpenState::Done = self.state {
            return Ok(None);
        }

        let state = self.state.transition()?;

        match state {
            #[cfg(feature = "parquet_encryption")]
            ParquetOpenState::LoadEncryption(future) => Ok(Some(Self::schedule_io(async move {
                Ok(ParquetOpenState::PruneFile(future.await?))
            }))),
            ParquetOpenState::LoadMetadata(future) => Ok(Some(Self::schedule_io(async move {
                Ok(ParquetOpenState::PrepareFilters(Box::new(future.await?)))
            }))),
            ParquetOpenState::LoadPageIndex(future) => Ok(Some(Self::schedule_io(async move {
                Ok(ParquetOpenState::LoadBloomFilters(
                    future.await?.load_bloom_filters().boxed(),
                ))
            }))),
            ParquetOpenState::LoadBloomFilters(future) => Ok(Some(Self::schedule_io(async move {
                Ok(ParquetOpenState::PruneWithBloomFilters(Box::new(
                    future.await?,
                )))
            }))),
            ParquetOpenState::Ready(stream) => {
                let morsels: Vec<Box<dyn Morsel>> =
                    vec![Box::new(ParquetStreamMorsel::new(stream))];
                Ok(Some(MorselPlan::new().with_morsels(morsels)))
            }
            ParquetOpenState::Done => Ok(None),
            cpu_state => Ok(Some(
                MorselPlan::new().with_planners(vec![Box::new(Self { state: cpu_state })]),
            )),
        }
    }
}

impl ParquetMorselizer {
    /// Perform the CPU-only setup for opening a parquet file.
    fn prepare_open_file(&self, partitioned_file: PartitionedFile) -> Result<PreparedParquetOpen> {
        let file_range = partitioned_file.range.clone();
        let extensions = partitioned_file.extensions.clone();
        let file_name = partitioned_file.object_meta.location.to_string();
        let file_metrics = ParquetFileMetrics::new(self.partition_index, &file_name, &self.metrics);
        let baseline_metrics = BaselineMetrics::new(&self.metrics, self.partition_index);

        let metadata_size_hint = partitioned_file
            .metadata_size_hint
            .or(self.metadata_size_hint);

        let async_file_reader: Box<dyn AsyncFileReader> =
            self.parquet_file_reader_factory.create_reader(
                self.partition_index,
                partitioned_file.clone(),
                metadata_size_hint,
                &self.metrics,
            )?;

        // Calculate the output schema from the original projection (before literal replacement)
        // so we get correct field names from column references
        let logical_file_schema = Arc::clone(self.table_schema.file_schema());
        let output_schema = Arc::new(
            self.projection
                .project_schema(self.table_schema.table_schema())?,
        );

        // Build a combined map for replacing column references with literal values.
        // This includes:
        // 1. Partition column values from the file path (e.g., region=us-west-2)
        // 2. Constant columns detected from file statistics (where min == max)
        //
        // Although partition columns *are* constant columns, we don't want to rely on
        // statistics for them being populated if we can use the partition values
        // (which are guaranteed to be present).
        //
        // For example, given a partition column `region` and predicate
        // `region IN ('us-east-1', 'eu-central-1')` with file path
        // `/data/region=us-west-2/...`, the predicate is rewritten to
        // `'us-west-2' IN ('us-east-1', 'eu-central-1')` which simplifies to FALSE.
        //
        // While partition column optimization is done during logical planning,
        // there are cases where partition columns may appear in more complex
        // predicates that cannot be simplified until we open the file (such as
        // dynamic predicates).
        let mut literal_columns: HashMap<String, ScalarValue> = self
            .table_schema
            .table_partition_cols()
            .iter()
            .zip(partitioned_file.partition_values.iter())
            .map(|(field, value)| (field.name().clone(), value.clone()))
            .collect();
        // Add constant columns from file statistics.
        // Note that if there are statistics for partition columns there will be overlap,
        // but since we use a HashMap, we'll just overwrite the partition values with the
        // constant values from statistics (which should be the same).
        literal_columns.extend(constant_columns_from_stats(
            partitioned_file.statistics.as_deref(),
            &logical_file_schema,
        ));

        let mut projection = self.projection.clone();
        let mut predicate = self.predicate.clone();
        if !literal_columns.is_empty() {
            projection = projection.try_map_exprs(|expr| {
                replace_columns_with_literals(Arc::clone(&expr), &literal_columns)
            })?;
            predicate = predicate
                .map(|p| replace_columns_with_literals(p, &literal_columns))
                .transpose()?;
        }

        // Replace any `input_file_name()` UDFs in the projection with a literal for this file.
        projection = rewrite_input_file_name_in_projection(projection, &file_name)?;

        let predicate_creation_errors = MetricBuilder::new(&self.metrics)
            .with_category(MetricCategory::Rows)
            .global_counter("num_predicate_creation_errors");

        // `FilePruner::try_new` decides whether a pruner is worthwhile (it needs
        // a statistics struct, and either real column statistics or a dynamic
        // filter that can prune via partition-value folding) and returns `None`
        // otherwise. For a static predicate the pruner's tracker reports no
        // changes, so it runs once and adds no ongoing cost.
        let file_pruner = predicate.as_ref().and_then(|p| {
            FilePruner::try_new(
                Arc::clone(p),
                &logical_file_schema,
                &partitioned_file,
                predicate_creation_errors.clone(),
            )
        });

        Ok(PreparedParquetOpen {
            partition_index: self.partition_index,
            partitioned_file,
            file_range,
            extensions,
            file_name,
            file_metrics,
            baseline_metrics,
            file_pruner,
            metadata_size_hint,
            metrics: self.metrics.clone(),
            parquet_file_reader_factory: Arc::clone(&self.parquet_file_reader_factory),
            async_file_reader,
            row_group_prefetch: self.row_group_prefetch.clone(),
            batch_size: self.batch_size,
            logical_file_schema: Arc::clone(&logical_file_schema),
            physical_file_schema: logical_file_schema,
            output_schema,
            projection,
            predicate,
            virtual_state: self.virtual_state.as_ref().map(Arc::clone),
            reorder_predicates: self.reorder_filters,
            pushdown_filters: self.pushdown_filters,
            force_filter_selections: self.force_filter_selections,
            enable_page_index: self.enable_page_index,
            enable_bloom_filter: self.enable_bloom_filter,
            enable_row_group_stats_pruning: self.enable_row_group_stats_pruning,
            limit: self.limit,
            coerce_int96: self.coerce_int96,
            coerce_int96_tz: self.coerce_int96_tz.clone(),
            expr_adapter_factory: Arc::clone(&self.expr_adapter_factory),
            predicate_creation_errors,
            max_predicate_cache_size: self.max_predicate_cache_size,
            max_in_list_size: self.max_in_list_size,
            reverse_row_groups: self.reverse_row_groups,
            sort_order_for_reorder: self.sort_order_for_reorder.clone(),
            preserve_order: self.preserve_order,
            #[cfg(feature = "parquet_encryption")]
            file_decryption_properties: None,
        })
    }
}

impl PreparedParquetOpen {
    /// Attempt file-level pruning before any metadata is loaded.
    ///
    /// Returns `None` if the file can be skipped completely.
    fn prune_file(mut self) -> Result<Option<Self>> {
        // Prune this file using the file level statistics and partition values.
        // Since dynamic filters may have been updated since planning it is
        // possible that we are able to prune files now that we couldn't prune at
        // planning time. The `FilePruner` (built when the predicate is dynamic or
        // the file carries statistics) also watches any still-active dynamic
        // filter, so the
        // `EarlyStoppingStream` wrapping the scan can re-check after each batch
        // and end the stream early once a tightened filter proves the file can
        // be skipped.
        //
        // File-level statistics may prune the file without loading any row
        // groups or metadata. Partition column predicates are already folded to
        // literals (see `replace_columns_with_literals` above), so a dynamic
        // filter that references only partition columns can prune here too even
        // when the file has no column statistics, e.g.
        // `select * from t order by partition_col limit 10`.
        if let Some(file_pruner) = &mut self.file_pruner
            && file_pruner.should_prune()?
        {
            self.file_metrics
                .files_ranges_pruned_statistics
                .add_pruned(1);
            return Ok(None);
        }

        self.file_metrics
            .files_ranges_pruned_statistics
            .add_matched(1);
        Ok(Some(self))
    }

    /// Load parquet metadata after file-level pruning is complete.
    async fn load(mut self) -> Result<MetadataLoadedParquetOpen> {
        // Don't load the page index yet. Since it is not stored inline in
        // the footer, loading the page index if it is not needed will do
        // unnecessary I/O. We decide later if it is needed to evaluate the
        // pruning predicates. Thus default to not requesting it from the
        // underlying reader.
        let mut options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
        if let Some(schema) = self.partitioned_file.arrow_schema.as_ref() {
            options = options.with_schema(Arc::clone(schema));
        }
        #[cfg(feature = "parquet_encryption")]
        let mut options = options;
        #[cfg(feature = "parquet_encryption")]
        if let Some(fd_val) = &self.file_decryption_properties {
            options = options.with_file_decryption_properties(Arc::clone(fd_val));
        }

        let mut metadata_timer = self.file_metrics.metadata_load_time.timer();
        // Begin by loading the metadata from the underlying reader (note
        // the returned metadata may actually include page indexes as some
        // readers may return page indexes even when not requested -- for
        // example when they are cached)
        let reader_metadata =
            ArrowReaderMetadata::load_async(&mut self.async_file_reader, options.clone()).await?;
        metadata_timer.stop();
        drop(metadata_timer);

        Ok(MetadataLoadedParquetOpen {
            prepared: self,
            reader_metadata,
            options,
        })
    }
}

impl MetadataLoadedParquetOpen {
    /// Prepare file-schema coercions and pruning predicates once metadata is
    /// loaded.
    fn prepare_filters(self) -> Result<FiltersPreparedParquetOpen> {
        let MetadataLoadedParquetOpen {
            mut prepared,
            mut reader_metadata,
            mut options,
        } = self;

        // Note about schemas: we are actually dealing with **3 different schemas** here:
        // - The table schema as defined by the TableProvider.
        //   This is what the user sees, what they get when they `SELECT * FROM table`, etc.
        // - The logical file schema: this is the table schema minus any hive partition columns and projections.
        //   This is what the physical file schema is coerced to.
        // - The physical file schema: this is the schema that the arrow-rs
        //   parquet reader will actually produce for the file's columns. Any
        //   virtual columns (see [`crate::TableSchema::virtual_columns`]) are
        //   produced separately by the reader and are not part of this schema.
        let mut physical_file_schema = Arc::clone(reader_metadata.schema());

        // The schema loaded from the file may not be the same as the
        // desired schema (for example if we want to instruct the parquet
        // reader to read strings using Utf8View instead). Update if necessary
        let mut metadata_dirty = false;
        if let Some(merged) =
            apply_file_schema_type_coercions(&prepared.logical_file_schema, &physical_file_schema)
        {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            metadata_dirty = true;
        }

        if let Some(ref coerce) = prepared.coerce_int96
            && let Some(merged) = Int96Coercer::new(
                reader_metadata.parquet_schema(),
                &physical_file_schema,
                coerce,
            )
            .with_timezone(prepared.coerce_int96_tz.clone())
            .coerce()
        {
            physical_file_schema = Arc::new(merged);
            options = options.with_schema(Arc::clone(&physical_file_schema));
            metadata_dirty = true;
        }

        // Arrow-rs appends virtual columns to the supplied schema internally,
        // so any `with_schema` coercion above must stay limited to file columns.
        if let Some(state) = prepared.virtual_state.as_ref() {
            options = options.with_virtual_columns((*state.virtual_columns).clone())?;
            metadata_dirty = true;
        }

        if metadata_dirty {
            reader_metadata = ArrowReaderMetadata::try_new(
                Arc::clone(reader_metadata.metadata()),
                options.clone(),
            )?;
        }

        // Adapt the projection & filter predicate to the physical file schema.
        // This evaluates missing columns and inserts any necessary casts.
        // After rewriting to the file schema, further simplifications may be possible.
        // For example, if `'a' = col_that_is_missing` becomes `'a' = NULL` that can then be simplified to `FALSE`
        // and we can avoid doing any more work on the file (bloom filters, loading the page index, etc.).
        // Additionally, if any casts were inserted we can move casts from the column to the literal side:
        // `CAST(col AS INT) = 5` can become `col = CAST(5 AS <col type>)`, which can be evaluated statically.
        //
        // When the schemas are identical and there is no predicate, the
        // rewriter is a no-op: column indices already match (partition
        // columns are appended after file columns in the table schema),
        // types are the same, and there are no missing columns. Skip the
        // tree walk entirely in that case.
        let needs_rewrite =
            prepared.predicate.is_some() || prepared.logical_file_schema != physical_file_schema;
        if needs_rewrite {
            // When virtual columns are requested, augment the logical and
            // physical schemas passed to the rewriter/simplifier with those
            // fields. The rewriter identity-rewrites references found in both
            // schemas, keeping virtual-column references as `Column` rather
            // than replacing them with null literals; the simplifier needs
            // them present so it can resolve their data types while walking
            // expression trees. We keep `physical_file_schema` itself as the
            // pure file schema so downstream predicate pushdown, pruning, and
            // row filter construction stay unaffected.
            let (logical_for_rewrite, physical_for_rewrite) =
                if let Some(state) = prepared.virtual_state.as_ref() {
                    (
                        Arc::clone(&state.logical_schema_with_virtual),
                        append_fields(&physical_file_schema, &state.virtual_columns),
                    )
                } else {
                    (
                        Arc::clone(&prepared.logical_file_schema),
                        Arc::clone(&physical_file_schema),
                    )
                };
            let rewriter = prepared.expr_adapter_factory.create(
                Arc::clone(&logical_for_rewrite),
                Arc::clone(&physical_for_rewrite),
            )?;
            let simplifier = PhysicalExprSimplifier::new(&physical_for_rewrite);
            prepared.predicate = prepared
                .predicate
                .map(|p| simplifier.simplify(rewriter.rewrite(p)?))
                .transpose()?;
            prepared.projection = prepared
                .projection
                .try_map_exprs(|p| simplifier.simplify(rewriter.rewrite(p)?))?;
        }
        prepared.physical_file_schema = Arc::clone(&physical_file_schema);

        // Build predicates for this specific file
        let pruning_predicate = build_pruning_predicates(
            prepared.predicate.as_ref(),
            &physical_file_schema,
            &prepared.predicate_creation_errors,
            prepared.max_in_list_size,
        );

        // Only build page pruning predicate if page index is enabled
        let page_pruning_predicate = if prepared.enable_page_index {
            prepared.predicate.as_ref().and_then(|predicate| {
                let p = build_page_pruning_predicate(predicate, &physical_file_schema);
                (p.filter_number() > 0).then_some(p)
            })
        } else {
            None
        };

        Ok(FiltersPreparedParquetOpen {
            loaded: MetadataLoadedParquetOpen {
                prepared,
                reader_metadata,
                options,
            },
            pruning_predicate,
            page_pruning_predicate,
        })
    }
}

impl FiltersPreparedParquetOpen {
    /// Prune row groups using file ranges and parquet metadata.
    fn prune_row_groups(self) -> Result<RowGroupsPrunedParquetOpen> {
        let loaded = &self.loaded;
        let prepared = &loaded.prepared;
        let file_metadata = Arc::clone(loaded.reader_metadata.metadata());
        let rg_metadata = file_metadata.row_groups();

        // Determine which row groups to actually read. The idea is to skip
        // as many row groups as possible based on the metadata and query
        let mut row_groups = RowGroupAccessPlanFilter::new(create_initial_plan(
            &prepared.file_name,
            &prepared.extensions,
            rg_metadata,
        )?);

        // If there is a range restricting what parts of the file to read
        if let Some(range) = prepared.file_range.as_ref() {
            row_groups.prune_by_range(rg_metadata, range);
        }

        // If there is a predicate that can be evaluated against the metadata
        if let Some(predicate) = self.pruning_predicate.as_ref().map(|p| p.as_ref()) {
            if prepared.enable_row_group_stats_pruning {
                row_groups.prune_by_statistics(
                    &prepared.physical_file_schema,
                    loaded.reader_metadata.parquet_schema(),
                    rg_metadata,
                    predicate,
                    &prepared.file_metrics,
                );
            } else {
                // Update metrics: statistics unavailable, so all row groups are
                // matched (not pruned)
                prepared
                    .file_metrics
                    .row_groups_pruned_statistics
                    .add_matched(row_groups.remaining_row_group_count());
            }

            if !prepared.enable_bloom_filter || row_groups.is_empty() {
                // Update metrics: bloom filter unavailable, so all row groups are
                // matched (not pruned)
                prepared
                    .file_metrics
                    .row_groups_pruned_bloom_filter
                    .add_matched(row_groups.remaining_row_group_count());
            }
        } else {
            // Update metrics: no predicate, so all row groups are matched (not pruned)
            let remaining = row_groups.remaining_row_group_count();
            prepared
                .file_metrics
                .row_groups_pruned_statistics
                .add_matched(remaining);
            prepared
                .file_metrics
                .row_groups_pruned_bloom_filter
                .add_matched(remaining);
        }

        Ok(RowGroupsPrunedParquetOpen {
            prepared: self,
            row_groups,
        })
    }
}

impl RowGroupsPrunedParquetOpen {
    /// Returns true if the reader would benefit from a page index load, given
    /// the current pruning predicate and row group access plan.
    ///
    /// The page index is used for data page pruning, and it is only useful
    /// when:
    ///
    /// 1. There is at least one row group that may have filtered rows
    ///    (if it is fully matched we know no rows will be filtered)
    ///
    /// 2. There is a page index for at least one predicate column (some
    ///    parquet writers do not write the page index).
    fn should_load_page_index(&self) -> bool {
        let Some(page_pruning_predicate) = self.prepared.page_pruning_predicate.as_ref() else {
            return false;
        };
        let row_groups = &self.row_groups;
        let fully_matched = row_groups.is_fully_matched();
        // if all row groups are fully matched, nothing can be pruned
        if row_groups.row_group_indexes().all(|idx| fully_matched[idx]) {
            return false;
        }

        // Check the file's footer metadata to see if a page index was written
        // for at least one predicate column in a surviving row group.
        //
        // Note: offsets are recorded in the footer, so we can determine if a
        // page index exists before attempting to read it.
        let parquet_metadata = self.prepared.loaded.reader_metadata.metadata();
        let arrow_schema = &self.prepared.loaded.prepared.physical_file_schema;
        let parquet_schema = parquet_metadata.file_metadata().schema_descr();
        page_pruning_predicate.predicate_column_names().any(|name| {
            let Some((leaf_idx, _)) = parquet_column(parquet_schema, arrow_schema, name) else {
                return false;
            };
            row_groups.row_group_indexes().any(|rg_idx| {
                let column = parquet_metadata.row_group(rg_idx).column(leaf_idx);
                column.column_index_offset().is_some() && column.offset_index_offset().is_some()
            })
        })
    }

    /// Load the page index if pruning requires it and metadata did not include it.
    async fn load_page_index(mut self) -> Result<Self> {
        self.prepared.loaded.reader_metadata = load_page_index(
            self.prepared.loaded.reader_metadata.clone(),
            &mut self.prepared.loaded.prepared.async_file_reader,
            self.prepared
                .loaded
                .options
                .clone()
                .with_page_index_policy(PageIndexPolicy::Optional),
        )
        .await?;

        Ok(self)
    }

    /// Load bloom filters needed for pruning when enabled and a pruning predicate exists.
    async fn load_bloom_filters(mut self) -> Result<BloomFiltersLoadedParquetOpen> {
        let num_row_groups = self
            .prepared
            .loaded
            .reader_metadata
            .metadata()
            .num_row_groups();
        let mut row_group_bloom_filters = vec![BloomFilterStatistics::new(); num_row_groups];

        if let Some(predicate) = self.prepared.pruning_predicate.as_ref().map(|p| p.as_ref())
            && self.prepared.loaded.prepared.enable_bloom_filter
            && !self.row_groups.is_empty()
        {
            // Use the existing reader for bloom filter I/O;
            // replace with a fresh reader for decoding below.
            let reader_metadata = self.prepared.loaded.reader_metadata.clone();
            let replacement_reader = {
                let prepared = &self.prepared.loaded.prepared;
                prepared.parquet_file_reader_factory.create_reader(
                    prepared.partition_index,
                    prepared.partitioned_file.clone(),
                    prepared.metadata_size_hint,
                    &prepared.metrics,
                )?
            };

            let prepared = &mut self.prepared.loaded.prepared;
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                mem::replace(&mut prepared.async_file_reader, replacement_reader),
                reader_metadata,
            );
            let parquet_columns: Vec<(String, usize, Type, i32)> = predicate
                .literal_columns()
                .into_iter()
                .filter_map(|column_name| {
                    let parquet_schema = builder.parquet_schema();
                    let (column_idx, _) = parquet_column(
                        parquet_schema,
                        &prepared.physical_file_schema,
                        &column_name,
                    )?;
                    Some((
                        column_name,
                        column_idx,
                        parquet_schema.column(column_idx).physical_type(),
                        parquet_schema.column(column_idx).type_length(),
                    ))
                })
                .collect();

            for idx in self.row_groups.row_group_indexes() {
                let mut row_group_filters =
                    BloomFilterStatistics::with_capacity(parquet_columns.len());
                for (column_name, column_idx, physical_type, type_length) in &parquet_columns {
                    let bf: Sbbf = match builder
                        .get_row_group_column_bloom_filter(idx, *column_idx)
                        .await
                    {
                        Ok(Some(bf)) => bf,
                        Ok(None) => continue,
                        Err(e) => {
                            debug!("Ignoring error reading bloom filter: {e}");
                            prepared.file_metrics.predicate_evaluation_errors.add(1);
                            continue;
                        }
                    };
                    row_group_filters.insert(column_name, bf, *physical_type, *type_length);
                }
                row_group_bloom_filters[idx] = row_group_filters;
            }
        }

        Ok(BloomFiltersLoadedParquetOpen {
            prepared: self,
            row_group_bloom_filters,
        })
    }
}

impl BloomFiltersLoadedParquetOpen {
    /// Apply bloom filter pruning using already loaded bloom filters.
    fn prune_bloom_filters(mut self) -> RowGroupsPrunedParquetOpen {
        let bloom_filter_eval_time = self
            .prepared
            .prepared
            .loaded
            .prepared
            .file_metrics
            .bloom_filter_eval_time
            .clone();
        let _timer_guard = bloom_filter_eval_time.timer();
        if let Some(predicate) = self
            .prepared
            .prepared
            .pruning_predicate
            .as_ref()
            .map(|p| p.as_ref())
            && self.prepared.prepared.loaded.prepared.enable_bloom_filter
            && !self.prepared.row_groups.is_empty()
        {
            self.prepared.row_groups.prune_by_bloom_filters(
                predicate,
                &self.prepared.prepared.loaded.prepared.file_metrics,
                &self.row_group_bloom_filters,
            );
        }

        self.prepared
    }
}

impl RowGroupsPrunedParquetOpen {
    /// Build the final parquet stream once all pruning work is complete.
    fn build_stream(self) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let RowGroupsPrunedParquetOpen {
            prepared,
            mut row_groups,
        } = self;
        let FiltersPreparedParquetOpen {
            loaded,
            pruning_predicate: _,
            page_pruning_predicate,
        } = prepared;
        let MetadataLoadedParquetOpen {
            prepared,
            reader_metadata,
            options: _,
        } = loaded;

        let file_metadata = Arc::clone(reader_metadata.metadata());
        let rg_metadata = file_metadata.row_groups();

        // Prune by limit if limit is set and limit order is not sensitive
        if let (Some(limit), false) = (prepared.limit, prepared.preserve_order) {
            row_groups.prune_by_limit(limit, rg_metadata, &prepared.file_metrics);
        }

        // Build the access plan. Fully matched row groups have all rows
        // satisfying the predicate, so page pruning and row filter evaluation
        // can be skipped for them.
        let mut access_plan = row_groups.build();

        // Page index pruning: if all data on individual pages can
        // be ruled using page metadata, rows from other columns
        // with that range can be skipped as well.
        if prepared.enable_page_index
            && !access_plan.is_empty()
            && let Some(page_pruning_predicate) = page_pruning_predicate
        {
            let page_pruning_result = page_pruning_predicate
                .prune_plan_with_page_index_and_metrics(
                    access_plan,
                    &prepared.physical_file_schema,
                    reader_metadata.parquet_schema(),
                    file_metadata.as_ref(),
                    &prepared.file_metrics,
                );
            access_plan = page_pruning_result.access_plan;
            crate::metrics::add_page_index_pages_skipped_by_fully_matched(
                &prepared.metrics,
                prepared.partition_index,
                &prepared.file_name,
                page_pruning_result.pages_skipped_by_fully_matched,
            );
        }

        // Prepare access plans, then apply row-group ordering tweaks per
        // run. Two composable steps:
        //
        // 1. `reorder_by_statistics`: sort row groups by `min(col)` ASC.
        //    Fixes out-of-order row groups (e.g. append-heavy workloads).
        //    Skipped gracefully when statistics aren't available or the
        //    sort expression isn't a plain column.
        //
        // 2. `reverse`: flip the iteration order for DESC requests, applied
        //    AFTER any reorder so the reversed order is correct whether or
        //    not reorder changed anything. Also handles `row_selection`
        //    remapping.
        //
        // For sorted data: reorder is a no-op, reverse gives perfect DESC.
        // For unsorted data: reorder fixes the order, reverse flips for DESC.
        //
        // Both inputs come from the sort-pushdown channel —
        // `ParquetSource::try_pushdown_sort` sets `sort_order_for_reorder`
        // and/or `reverse_row_groups`.
        let prepare_access_plan = |plan: ParquetAccessPlan| -> Result<PreparedAccessPlan> {
            let mut prepared_plan = plan.prepare(rg_metadata)?;
            if let Some(sort_order) = prepared.sort_order_for_reorder.as_ref() {
                prepared_plan = prepared_plan.reorder_by_statistics(
                    sort_order,
                    file_metadata.as_ref(),
                    &prepared.physical_file_schema,
                )?;
            }
            if prepared.reverse_row_groups {
                prepared_plan = prepared_plan.reverse(file_metadata.as_ref())?;
            }
            Ok(prepared_plan)
        };

        let arrow_reader_metrics = ArrowReaderMetrics::enabled();

        // Build the decoder projection (mask + per-batch transform) in a
        // single call. Encapsulating it behind `DecoderProjection` keeps the
        // opener's orchestration body focused on filter / decoder / stream
        // wiring.
        let decoder_projection = DecoderProjection::try_new(
            &prepared.projection,
            &prepared.physical_file_schema,
            reader_metadata.parquet_schema(),
            &prepared.output_schema,
            prepared.virtual_state.as_deref(),
        )?;

        let (decoder, rg_plan, has_row_selection) = {
            let pushdown_predicate = prepared
                .pushdown_filters
                .then_some(prepared.predicate.as_ref())
                .flatten();
            let mut row_filter_generator = RowFilterGenerator::new(
                pushdown_predicate,
                &prepared.physical_file_schema,
                file_metadata.as_ref(),
                prepared.reorder_predicates,
                &prepared.file_metrics,
            );

            // Build the prepared access plan first — `prepare_access_plan` may
            // call `reorder_by_statistics` (for `sort_order_for_reorder`) and
            // `reverse` (for `reverse_row_groups`), both of which mutate
            // `row_group_indexes` to the physical scan order the decoder will
            // actually read. We MUST build our `rg_plan` from this reordered
            // list, otherwise our per-RG pruner check would consult the
            // metadata of a different RG than the decoder is about to yield.
            let decoder_config = DecoderBuilderConfig {
                projection_mask: decoder_projection.projection_mask(),
                batch_size: prepared.batch_size,
                arrow_reader_metrics: &arrow_reader_metrics,
                force_filter_selections: prepared.force_filter_selections,
                decoder_limit: prepared.limit,
            };

            let prepared_access_plan = prepare_access_plan(access_plan)?;
            // #24355: a row selection (from page-index pruning, or an externally
            // supplied `ParquetRowSelection`) is carried by the decoder as one
            // flat selection over the concatenation of the remaining row groups.
            // The runtime pruner's `into_builder().with_row_groups(...)` rebuild
            // drops row groups without slicing that selection to match, so record
            // whether a selection is present and disable runtime pruning below
            // when it is (mirroring `reorder_by_statistics`, which also bails when
            // a row selection is present). The proper fix that keeps pruning
            // under a live selection is tracked in
            // https://github.com/apache/arrow-rs/issues/10624 /
            // https://github.com/apache/datafusion/issues/24358.
            let has_row_selection = prepared_access_plan.row_selection.is_some();
            let rg_plan: VecDeque<RgPlanEntry> = prepared_access_plan
                .row_group_indexes
                .iter()
                .copied()
                .map(|rg_index| RgPlanEntry { rg_index })
                .collect();

            let mut builder = decoder_config.build(prepared_access_plan, reader_metadata.clone());
            if let Some(row_filter) = row_filter_generator.next_filter() {
                builder = builder.with_row_filter(row_filter);
                if let Some(max_predicate_cache_size) = prepared.max_predicate_cache_size {
                    builder = builder.with_max_predicate_cache_size(max_predicate_cache_size);
                }
            }

            (builder.build()?, rg_plan, has_row_selection)
        };

        let predicate_cache_inner_records =
            prepared.file_metrics.predicate_cache_inner_records.clone();
        let predicate_cache_records = prepared.file_metrics.predicate_cache_records.clone();

        let files_ranges_pruned_statistics =
            prepared.file_metrics.files_ranges_pruned_statistics.clone();

        // Build a dynamic row-group pruner only when all three conditions hold:
        //   1) the scan has a predicate (so there is something to evaluate),
        //   2) the predicate has at least one not-yet-complete dynamic filter
        //      (`DynamicFilterTracking::Watching`) — static or already-complete
        //      predicates were fully consumed by `prune_by_statistics` at file
        //      open, so re-evaluating them per RG boundary would be wasted work,
        //   3) there is at least one pending RG that could be skipped.
        // The pruner subscribes once to every still-incomplete dynamic filter
        // via the `DynamicFilterTracker` watch channel (#22460), so detecting
        // a threshold change is a single atomic load — not a tree walk per
        // RG check.
        // Also disabled when a row selection is live (#24355) — page-index
        // pruning is the common source: the pruner rebuilds the decoder via
        // `with_row_groups(...)`, which drops row groups without slicing the
        // carried selection to match, so pruning under a live selection returns
        // wrong results. Decline to prune in that case.
        let row_group_pruner = match (&prepared.predicate, rg_plan.len() > 1, has_row_selection) {
            (Some(predicate), true, false)
                if matches!(
                    DynamicFilterTracking::classify(predicate),
                    DynamicFilterTracking::Watching(_)
                ) =>
            {
                Some(RowGroupPruner::new(
                    Arc::clone(predicate),
                    Arc::clone(&prepared.physical_file_schema),
                    Arc::clone(reader_metadata.metadata()),
                    prepared.predicate_creation_errors.clone(),
                    prepared.file_metrics.predicate_evaluation_errors.clone(),
                    prepared.max_in_list_size,
                ))
            }
            _ => None,
        };
        let row_groups_pruned_dynamic = prepared
            .file_metrics
            .row_groups_pruned_dynamic_filter
            .clone();

        let stream = PushDecoderStreamState {
            decoder: Some(decoder),
            active_reader: None,
            rg_plan,
            reader: Arc::new(tokio::sync::Mutex::new(prepared.async_file_reader)),
            row_group_prefetch: prepared.row_group_prefetch,
            parquet_metadata: Arc::clone(reader_metadata.metadata()),
            pending_prefetch: None,
            prefetch_metrics: crate::metrics::PrefetchMetrics::new(
                &prepared.metrics,
                prepared.partition_index,
            ),
            prefetch_reservation: None,
            decoder_projection,
            arrow_reader_metrics,
            predicate_cache_inner_records,
            predicate_cache_records,
            baseline_metrics: prepared.baseline_metrics,
            row_group_pruner,
            row_groups_pruned_dynamic,
        }
        .into_stream();

        // Wrap the stream so a dynamic filter can stop the file scan early, but
        // only when the pruner is still watching a filter that can change
        // mid-scan. For a static (or already-complete) predicate the up-front
        // `prune_file` check already captured everything that can be pruned, so
        // per-batch re-checking would only add overhead.
        match prepared.file_pruner {
            Some(file_pruner) if file_pruner.is_watching() => {
                Ok(
                    EarlyStoppingStream::new(stream, file_pruner, files_ranges_pruned_statistics)
                        .boxed(),
                )
            }
            _ => Ok(stream),
        }
    }
}

type ConstantColumns = HashMap<String, ScalarValue>;

/// Extract constant column values from statistics, keyed by column name in the logical file schema.
fn constant_columns_from_stats(
    statistics: Option<&Statistics>,
    file_schema: &SchemaRef,
) -> ConstantColumns {
    let mut constants = HashMap::new();
    let Some(statistics) = statistics else {
        return constants;
    };

    let num_rows = match statistics.num_rows {
        Precision::Exact(num_rows) => Some(num_rows),
        _ => None,
    };

    for (idx, column_stats) in statistics
        .column_statistics
        .iter()
        .take(file_schema.fields().len())
        .enumerate()
    {
        let field = file_schema.field(idx);
        if let Some(value) = constant_value_from_stats(column_stats, num_rows, field.data_type()) {
            constants.insert(field.name().clone(), value);
        }
    }

    constants
}

fn constant_value_from_stats(
    column_stats: &ColumnStatistics,
    num_rows: Option<usize>,
    data_type: &DataType,
) -> Option<ScalarValue> {
    if let (Precision::Exact(min), Precision::Exact(max)) =
        (&column_stats.min_value, &column_stats.max_value)
        && min == max
        && !min.is_null()
        && matches!(column_stats.null_count, Precision::Exact(0))
    {
        // Cast to the expected data type if needed (e.g., Utf8 -> Dictionary)
        if min.data_type() != *data_type {
            return min.cast_to(data_type).ok();
        }
        return Some(min.clone());
    }

    if let (Some(num_rows), Precision::Exact(nulls)) = (num_rows, &column_stats.null_count)
        && *nulls == num_rows
    {
        return ScalarValue::try_new_null(data_type).ok();
    }

    None
}

/// Return the initial [`ParquetAccessPlan`]
///
/// If the user has supplied a parquet access extension, use that; otherwise
/// return a plan that scans all row groups.
///
/// Returns an error if an invalid parquet access extension is provided.
///
/// Note: file_name is only used for error messages
fn create_initial_plan(
    file_name: &str,
    extensions: &datafusion_datasource::FileExtensions,
    rg_metadata: &[RowGroupMetaData],
) -> Result<ParquetAccessPlan> {
    let row_group_count = rg_metadata.len();
    let external_plan = extensions
        .get::<datafusion_datasource_parquet::ParquetAccessPlan>()
        .map(|plan| ParquetAccessPlan::new(plan.inner().to_vec()));
    match (
        extensions
            .get::<ParquetAccessPlan>()
            .or(external_plan.as_ref()),
        extensions.get::<ParquetRowSelection>(),
    ) {
        (Some(_), Some(_)) => exec_err!(
            "Invalid parquet access extensions for {file_name}. \
            Specify either ParquetAccessPlan or ParquetRowSelection, not both"
        ),
        (Some(access_plan), None) => {
            let plan_len = access_plan.len();
            if plan_len != row_group_count {
                return exec_err!(
                    "Invalid ParquetAccessPlan for {file_name}. Specified {plan_len} row groups, but file has {row_group_count}"
                );
            }
            Ok(access_plan.clone())
        }
        (None, Some(row_selection)) => ParquetAccessPlan::try_new_from_overall_row_selection(
            row_selection.selection().clone(),
            rg_metadata,
        ),
        // default to scanning all row groups
        (None, None) => Ok(ParquetAccessPlan::new_all(row_group_count)),
    }
}

/// Build a page pruning predicate from an optional predicate expression.
/// If the predicate is None or the predicate cannot be converted to a page pruning
/// predicate, return None.
pub(crate) fn build_page_pruning_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
) -> Arc<PagePruningAccessPlanFilter> {
    Arc::new(PagePruningAccessPlanFilter::new(
        predicate,
        Arc::clone(file_schema),
    ))
}

pub(crate) fn build_pruning_predicates(
    predicate: Option<&Arc<dyn PhysicalExpr>>,
    file_schema: &SchemaRef,
    predicate_creation_errors: &Count,
    max_in_list_size: usize,
) -> Option<Arc<PruningPredicate>> {
    let predicate = predicate.as_ref()?;
    PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(file_schema))
        .with_error_counter(predicate_creation_errors)
        .with_max_in_list_size(max_in_list_size)
        .build(Arc::clone(predicate))
}

/// Returns a `ArrowReaderMetadata` with the page index loaded, loading
/// it from the underlying `AsyncFileReader` if necessary.
async fn load_page_index<T: AsyncFileReader>(
    reader_metadata: ArrowReaderMetadata,
    input: &mut T,
    options: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata> {
    let parquet_metadata = reader_metadata.metadata();
    let missing_column_index = parquet_metadata.column_index().is_none();
    let missing_offset_index = parquet_metadata.offset_index().is_none();
    // You may ask yourself: why are we even checking if the page index is already loaded here?
    // Didn't we explicitly *not* load it above?
    // Well it's possible that a custom implementation of `AsyncFileReader` gives you
    // the page index even if you didn't ask for it (e.g. because it's cached)
    // so it's important to check that here to avoid extra work.
    if missing_column_index || missing_offset_index {
        let m =
            Arc::try_unwrap(Arc::clone(parquet_metadata)).unwrap_or_else(|e| e.as_ref().clone());
        let mut reader = ParquetMetaDataReader::new_with_metadata(m)
            .with_page_index_policy(PageIndexPolicy::Optional);
        reader.load_page_index(input).await?;
        let new_parquet_metadata = reader.finish()?;
        let new_arrow_reader =
            ArrowReaderMetadata::try_new(Arc::new(new_parquet_metadata), options)?;
        Ok(new_arrow_reader)
    } else {
        // No need to load the page index again, just return the existing metadata
        Ok(reader_metadata)
    }
}
