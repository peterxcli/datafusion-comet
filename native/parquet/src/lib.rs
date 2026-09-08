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

//! Comet-owned Parquet planning and I/O/decode orchestration.
//!
//! Adapted from Apache DataFusion 55.0.0. Public DF helpers and Arrow's decoder
//! remain dependencies; private scan orchestration is maintained here.

mod access_plan;
mod decoder_projection;
mod metrics;
mod nested_schema_pruning;
mod opener;
mod page_filter;
mod projection_read_plan;
mod push_decoder;
mod row_filter;
mod row_group_filter;
mod sort;
pub mod source;

pub use datafusion_datasource_parquet::{
    BloomFilterStatistics, CachedParquetFileReaderFactory, DefaultParquetFileReaderFactory,
    Int96Coercer, ParquetFileMetrics, ParquetFileReaderFactory, ParquetRowSelection,
    ParquetVirtualColumn, RowGroupAccess, apply_file_schema_type_coercions,
};
pub use source::ParquetSource;

pub use access_plan::ParquetAccessPlan;
