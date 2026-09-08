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

//! Real local-file scan benchmark. No artificial I/O or downstream delays.
use std::{sync::Arc, time::Instant};

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion_comet_parquet::ParquetSource;
use datafusion_datasource::{
    PartitionedFile, file_groups::FileGroup, file_scan_config::FileScanConfigBuilder,
    source::DataSourceExec,
};
use datafusion_execution::{
    TaskContext,
    config::SessionConfig,
    memory_pool::{GreedyMemoryPool, MemoryPool},
    object_store::ObjectStoreUrl,
};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, lit};
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let groups: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "64".into())
        .parse()?;
    assert!(groups > 0);
    let group_rows = 131_072;
    let schema = Arc::new(Schema::new(
        (0..8)
            .map(|i| Field::new(format!("c{i}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("scan.parquet");
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path)?,
        Arc::clone(&schema),
        Some(props),
    )?;
    let mut random = 42u64;
    println!(
        "Generating {} rows, {} row groups, eight Int64 columns",
        groups * group_rows,
        groups
    );
    for group in 0..groups {
        let columns = (0..8)
            .map(|column| {
                let values = (0..group_rows)
                    .map(|row| {
                        random ^= random << 13;
                        random ^= random >> 7;
                        random ^= random << 17;
                        if column == 0 {
                            (group * group_rows + row) as i64
                        } else {
                            (random % 1_000_000) as i64
                        }
                    })
                    .collect::<Vec<_>>();
                Arc::new(Int64Array::from(values)) as _
            })
            .collect();
        writer.write(&RecordBatch::try_new(Arc::clone(&schema), columns)?)?;
        writer.flush()?;
    }
    writer.close()?;
    let size = std::fs::metadata(&path)?.len();
    println!(
        "file_bytes={size}, logical_bytes={}, budget_bytes={}",
        groups * group_rows * 64,
        16 << 20
    );
    println!(
        "scenario,round,budget_bytes,elapsed_ms,rows,checksum,bytes_scanned,prefetch_bytes,prefetch_row_groups,prefetch_budget_skips,sampled_reserved_peak_bytes"
    );
    for scenario in ["wide", "narrow", "selective"] {
        let mut expected = None;
        // Round zero warms both paths. Alternate order to reduce cache/order bias.
        for round in 0..4 {
            let budgets = if round % 2 == 0 {
                [0, 16 << 20]
            } else {
                [16 << 20, 0]
            };
            for budget in budgets {
                let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(16 << 20));
                let mut source = ParquetSource::new(Arc::clone(&schema))
                    .with_row_group_prefetch(budget, Arc::clone(&pool))
                    .with_pushdown_filters(true);
                if scenario == "selective" {
                    source = source.with_predicate(Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("c1", 1)),
                        Operator::Lt,
                        lit(10_000i64),
                    )));
                }
                let config = FileScanConfigBuilder::new(
                    ObjectStoreUrl::local_filesystem(),
                    Arc::new(source),
                )
                .with_file_group(FileGroup::new(vec![PartitionedFile::new(
                    path.to_str().unwrap(),
                    size,
                )]))
                .with_projection_indices(if scenario == "narrow" {
                    Some(vec![0, 1])
                } else {
                    None
                })?
                .build();
                let plan = DataSourceExec::new(Arc::new(config));
                let task = TaskContext::default()
                    .with_session_config(SessionConfig::new().with_batch_size(8192));
                let start = Instant::now();
                let mut stream = plan.execute(0, Arc::new(task))?;
                let (mut rows, mut checksum, mut peak) = (0usize, 0i64, 0usize);
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    rows += batch.num_rows();
                    // Touch every decoded value; keep identical downstream CPU work.
                    for column in batch.columns() {
                        for value in column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .values()
                        {
                            checksum = checksum.wrapping_add(*value);
                        }
                    }
                    peak = peak.max(pool.reserved());
                }
                drop(stream);
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                let metrics = plan.metrics().unwrap();
                let metric = |name| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
                assert_eq!(*expected.get_or_insert((rows, checksum)), (rows, checksum));
                if scenario != "selective" {
                    assert_eq!(rows, groups * group_rows);
                }
                assert_eq!(pool.reserved(), 0);
                println!(
                    "{scenario},{round},{budget},{elapsed:.3},{rows},{checksum},{},{},{},{},{peak}",
                    metric("bytes_scanned"),
                    metric("prefetch_bytes"),
                    metric("prefetch_row_groups"),
                    metric("prefetch_budget_skips")
                );
            }
        }
    }
    Ok(())
}
