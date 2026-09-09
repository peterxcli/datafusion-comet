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
[`943ddfc20`](https://github.com/peterxcli/datafusion/commit/943ddfc209147ebd8022853482009c2827d77b59)
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

Prefetch is disabled by default. Upfront I/O is also disabled by default. Prefetch
fetches at most one future row group's full output and predicate column chunks,
subject to its byte budget and the execution memory pool. Upfront demand reads
fetch those chunks together for the current group. Both can read extra bytes that
selective filters would otherwise avoid. Compare upfront reads with prefetch
off/on to isolate overlap while holding fetched chunks constant.

The native reader regression test checks every setting, a tiny prefetch budget,
and a nested output with a predicate-only column. It verifies Spark-equivalent
results, native execution, and nonzero prefetch counters when the budget fits.
It also checks that enabling upfront I/O increases reader bytes when the row
filter empties entire groups.

See the [DataFusion benchmark method and results](https://github.com/peterxcli/datafusion/blob/codex/parquet-io-policy-df55/datafusion/datasource-parquet/IO_POLICY_BENCHMARK.md).

The two DataFusion runs found 6.1–9.7% lower elapsed time from prefetch for random,
wide, unindexed output when upfront reads held fetched chunks constant. Indexed,
clustered data exposed a regression: full-chunk fetching read 44.6 times as many
bytes and took roughly 2.4–3.0 times as long. Disabled-option controls also varied
against unmodified DataFusion 55. These local results do not establish a stable
overall speedup; the report includes both matrices and the baseline comparison.

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
