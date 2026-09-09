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

# Parquet I/O policy POC

This branch pins DataFusion 55.0.0 to implementation commit
[`8411b35ba`](https://github.com/peterxcli/datafusion/commit/8411b35ba36cbeed13bd26bbec05c5b3ba206e61)
in [fork PR #3](https://github.com/peterxcli/datafusion/pull/3). Comet passes
execution options to `ParquetSource` and exposes the prefetch metrics in Spark.
The six direct DataFusion dependencies use the same fork revision.

On a Comet-enabled Spark session, compare these configurations, each with
`spark.comet.parquet.prefetchBytes` set to `0` and `16m`:

| Case | `spark.comet.parquet.rowFilterPushdown.enabled` | `spark.comet.parquet.upfrontIO.enabled` |
| --- | --- | --- |
| Pushdown off | `false` | `false` |
| Progressive reads with pushdown | `true` | `false` |
| Upfront reads with pushdown | `true` | `true` |

Prefetch and upfront I/O are disabled by default. Both preserve the page selection
made at file open, including dictionary pages. Adjacent ranges are merged so
whole-chunk decoder requests reuse the fetched bytes. Without an offset index,
they fetch complete column chunks. Upfront reads fetch the current group's
selected output and predicate pages together before row filtering; prefetch
fetches at most one future group's selected pages while decoding the current one.
Both can read pages that later row filtering would skip. The prefetch budget and
execution memory pool bound its additional compressed bytes.

Compare progressive and upfront reads with prefetch off to isolate the I/O
policy. Compare upfront reads with prefetch off/on separately to assess overlap.
DataFusion also exposes the policy as `datafusion.execution.parquet.progressive_io`;
Comet's upfront flag sets it after applying the table options.

The native reader regression test checks every setting, a tiny prefetch budget,
and a nested output with a predicate-only column. It verifies Spark-equivalent
results, native execution, and nonzero prefetch counters when the budget fits.
It also checks that enabling upfront I/O increases reader bytes when the row
filter empties entire groups.

See the [DataFusion benchmark method and results](https://github.com/peterxcli/datafusion/blob/codex/parquet-io-policy-df55/datafusion/datasource-parquet/IO_POLICY_BENCHMARK.md).

The updated 2 GiB synthetic matrix passed all 192 scans with matching results
and byte counts across every policy. Indexed, clustered wide output now requests
28.9 MB instead of the earlier full-chunk revision's 1.29 GB; upfront demand
reads reduce reader calls from 513 to 257. A fresh unmodified DF55 control is
included in the report.

The one-million-row ClickBench sample still showed slower upfront results for
Q11, Q22, Q24, Q25, and Q26 in both passes. This fixes the experimental reader's
page-pruning regression; full-dataset measurements and profiling are still needed
before proposing new defaults.

## Validation

Validated on Spark 4.1 / JDK 17 with the pinned native library:

```sh
cd native
cargo build --profile ci -p datafusion-comet --locked
cargo fmt --all -- --check
cd ..
SPARK_LOCAL_IP=127.0.0.1 ./mvnw test -Dtest=none \
  -Dsuites=org.apache.comet.CometConfSuite,org.apache.comet.exec.CometNativeReaderSuite \
  -Pspark-4.1 -Pjdk17 -Djni.dir="$PWD/native/target/ci"
```

The two Spark suites passed 79 tests; one existing compatibility test was
canceled. The new reader test passed all 12 configuration combinations and the
upfront-versus-progressive byte assertion. Rust formatting, Scala formatting,
and Scala style checks passed. The dependency lock contains 32 DataFusion 55
crates, all from the same fork commit, with no registry duplicates.
