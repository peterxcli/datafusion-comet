<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Comet Parquet scan

This crate owns native Parquet scan preparation and I/O/decode scheduling in Comet.
It was adapted from `datafusion-datasource-parquet` 55.0.0. It uses published
DataFusion and Arrow crates; no patched DataFusion dependency is required.

The private source, opener, access-plan state, projection planning, page/row-group
pruning, and push-decoder orchestration are maintained here. Public DataFusion
reader factories, metrics, schema coercion, bloom statistics, and row-filter
construction remain dependencies. Both DataFusion access-plan extensions and
file-level row selections are accepted. Spark schema adaptation, encryption,
object stores, and metadata caching remain wired through Comet's existing scan.

## Scheduling

Set `spark.comet.parquet.prefetchBytes=8m` to allow up to 8 MiB of additional
compressed data per file stream. Zero (the default) uses sequential demand reads.
Once Arrow hands out the current row-group reader, Comet starts an async fetch
for the next row group selected by the decoder. Current-group batch decoding
continues while that fetch runs. There is at most one pending fetch per stream;
this does not introduce work stealing or change output ordering.

The execution memory pool must reserve the projected column-chunk bytes before
I/O starts. Oversized groups and failed reservations use ordinary demand reads.
The reservation stays alive while the background task or decoder holds speculative
bytes. Required bytes become part of the current reader when it is extracted;
the budget does not cover that reader's ordinary memory or decoded batches.
Dropping the stream aborts pending I/O. At row-group boundaries, dynamic pruning
is checked before adopting the prefetched data. Failed speculative reads retry
through the demand path.

Whole projected column chunks may include pages or rows that filters later discard.
Predicate-only columns not in the output projection continue to use demand reads.
This is planned row-group read-ahead, not exact page-level prediction. Cancellation
cannot undo completed reads. The initial implementation uses Arrow's
`peek_next_row_group` to respect scan order and limits; its cost should be checked
on files with very many row groups before increasing scheduling complexity.

## Validation and metrics

`cargo test -p datafusion-comet-parquet --lib` covers overlap using a blocked read,
ordered results, external selections, dynamic pruning, cancellation, I/O errors,
and budget/pool-pressure fallbacks. The existing Comet native-reader and schema
adapter tests exercise Spark semantics through this source. A manual scheduling
experiment is available with:

```sh
cargo test -p datafusion-comet-parquet prefetch_latency_benchmark -- --ignored --nocapture
```

The Spark scan reports `prefetch_bytes` and `prefetch_row_groups` for successful
background fetches (including data later discarded), `prefetch_budget_skips`, and
`prefetch_wait_time`. Compare total `bytes_scanned`, task elapsed time, and execution
memory usage with prefetch disabled. Simulated latency is not a production
throughput benchmark.

For a larger local-file benchmark with real decoding and no injected delays:

```sh
cargo run --profile ci -p datafusion-comet-parquet --example prefetch_benchmark -- 64
```

The argument is the number of 131,072-row groups, each containing eight Int64
columns written with Snappy compression. The default is 8,388,608 rows (512 MiB
of logical values). Wide, narrow, and approximately 1%-selective scans alternate
prefetch disabled and a 16 MiB budget. Round zero is warmup; rounds 1–3 are timed
comparisons. Every run checks matching row counts and checksums. CSV output
includes bytes scanned, prefetch metrics, and sampled speculative reservations
(not total process memory). This measures warm local-file scans through the
native source, without Spark or remote storage.

When changing the DF/Arrow dependencies, review this crate's imported scan logic
alongside upstream fixes and rerun native scan, pruning, schema, and encryption
coverage. Comet owns these changes independently of upstream release timing.
