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

use crate::ParquetFileMetrics;
use arrow::datatypes::SchemaRef;
pub use datafusion_datasource_parquet::{build_row_filter, can_expr_be_pushed_down_with_schemas};
use datafusion_physical_expr::PhysicalExpr;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::file::metadata::ParquetMetaData;
use std::sync::Arc;

pub(crate) struct RowFilterGenerator<'a> {
    predicate: Option<&'a Arc<dyn PhysicalExpr>>,
    physical_file_schema: &'a SchemaRef,
    file_metadata: &'a ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &'a ParquetFileMetrics,
    first_row_filter: Option<RowFilter>,
}

impl<'a> RowFilterGenerator<'a> {
    pub(crate) fn new(
        predicate: Option<&'a Arc<dyn PhysicalExpr>>,
        physical_file_schema: &'a SchemaRef,
        file_metadata: &'a ParquetMetaData,
        reorder_predicates: bool,
        file_metrics: &'a ParquetFileMetrics,
    ) -> Self {
        let mut generator = Self {
            predicate,
            physical_file_schema,
            file_metadata,
            reorder_predicates,
            file_metrics,
            first_row_filter: None,
        };
        generator.first_row_filter = generator.build();
        generator
    }

    pub(crate) fn next_filter(&mut self) -> Option<RowFilter> {
        self.first_row_filter.take().or_else(|| self.build())
    }

    fn build(&self) -> Option<RowFilter> {
        let predicate = self.predicate?;
        match build_row_filter(
            predicate,
            self.physical_file_schema,
            self.file_metadata,
            self.reorder_predicates,
            self.file_metrics,
        ) {
            Ok(Some(filter)) => Some(filter),
            Ok(None) => None,
            Err(e) => {
                log::debug!("Ignoring error building row filter for '{predicate:?}': {e}");
                None
            }
        }
    }
}
