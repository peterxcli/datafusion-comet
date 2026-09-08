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

pub use datafusion_datasource_parquet::ParquetFileMetrics;
use datafusion_physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricCategory, MetricType,
};

/// Record pages whose page-index pruning was skipped because the containing
/// row group was fully matched by row-group statistics.
///
/// The counter is only registered when there is a non-zero value. This keeps
/// [`ParquetFileMetrics::new`] from cloning the filename and metrics set for
/// files that never use this metric.
pub(crate) fn add_page_index_pages_skipped_by_fully_matched(
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
    filename: &str,
    n: usize,
) {
    if n == 0 {
        return;
    }

    let count = MetricBuilder::new(metrics)
        .with_new_label("filename", filename.to_string())
        .with_type(MetricType::Summary)
        .with_category(MetricCategory::Rows)
        .counter("page_index_pages_skipped_by_fully_matched", partition);
    count.add(n);
}

/// Record that page index I/O was skipped because row-group statistics
/// already proved page index could not prune further.
pub(crate) fn add_page_index_load_skipped(
    metrics: &ExecutionPlanMetricsSet,
    partition: usize,
    filename: &str,
    n: usize,
) {
    if n == 0 {
        return;
    }

    let count = MetricBuilder::new(metrics)
        .with_new_label("filename", filename.to_string())
        .with_type(MetricType::Summary)
        .counter("page_index_load_skipped", partition);
    count.add(n);
}

/// Per-stream counters registered in the scan's existing metric set.
#[derive(Clone)]
pub(crate) struct PrefetchMetrics {
    pub bytes: datafusion_physical_plan::metrics::Count,
    pub row_groups: datafusion_physical_plan::metrics::Count,
    pub budget_skips: datafusion_physical_plan::metrics::Count,
    pub wait_time: datafusion_physical_plan::metrics::Time,
}

impl PrefetchMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            bytes: MetricBuilder::new(metrics).counter("prefetch_bytes", partition),
            row_groups: MetricBuilder::new(metrics).counter("prefetch_row_groups", partition),
            budget_skips: MetricBuilder::new(metrics).counter("prefetch_budget_skips", partition),
            wait_time: MetricBuilder::new(metrics).subset_time("prefetch_wait_time", partition),
        }
    }
}
