# Larger Parquet prefetch benchmark

Measured locally on macOS arm64 with Rust 1.96.0, published DataFusion 55.0.0 and Parquet 59.3.0. The native source was built with the optimized `ci` profile (no LTO). This is not an end-to-end Spark benchmark.

## Method

- Deterministic generated data: eight non-null Int64 columns, Snappy, dictionaries disabled, 131,072 rows per row group. Column zero is sequential; the others are pseudorandom values from 0 to 999,999.
- 64 groups: 8,388,608 rows, 512 MiB logical data, 323,652,220-byte Parquet file.
- 256 groups: 33,554,432 rows, 2 GiB logical data, 1,294,607,342-byte Parquet file.
- One file and one scan stream, two Tokio workers. Warm local filesystem/cache; no injected delays, cache eviction, remote storage, or Spark.
- Compare zero prefetch with a 16 MiB per-stream budget. One warmup per setting, then three measured runs, alternating off/on order.
- Wide reads all eight columns; narrow reads two; selective reads all eight with `c1 < 10000` and row-filter pushdown (approximately 1% of rows returned).
- All decoded output values contribute to a checksum. Every run verifies identical row counts/checksums for its scenario and that speculative reservations return to zero. File generation is outside timings.

## Elapsed time

| Logical data | Scan | Off median (range), ms | On median (range), ms | Time reduction |
|---|---|---:|---:|---:|
| 512 MiB | wide | 386.9 (386.0–391.8) | 351.9 (350.8–360.6) | 9.0% |
| 512 MiB | narrow | 88.7 (88.2–89.7) | 81.2 (80.4–83.1) | 8.5% |
| 512 MiB | selective | 394.1 (388.6–394.6) | 358.9 (353.6–360.2) | 8.9% |
| 2 GiB | wide | 1542.6 (1531.0–1545.8) | 1387.5 (1385.4–1389.1) | 10.1% |
| 2 GiB | narrow | 355.6 (348.9–377.2) | 326.8 (326.7–329.0) | 8.1% |
| 2 GiB | selective | 1599.7 (1598.8–1614.3) | 1428.4 (1417.4–1440.4) | 10.7% |

## I/O and memory

| Logical data | Scan | Bytes scanned, either setting | Maximum sampled speculative reservation, bytes |
|---|---|---:|---:|
| 512 MiB | wide | 323,463,659 | 5,054,711 |
| 512 MiB | narrow | 74,998,079 | 1,172,373 |
| 512 MiB | selective | 323,593,740 | 5,054,711 |
| 2 GiB | wide | 1,293,850,076 | 5,055,036 |
| 2 GiB | narrow | 299,994,315 | 1,172,455 |
| 2 GiB | selective | 1,294,374,909 | 5,055,036 |

Prefetch completed for all 63 or 255 upcoming row groups in every enabled run, with zero budget skips. Disabled runs reported zero prefetch bytes/groups and zero speculative reservations. Reservation sampling occurs after each output batch; it is not an exact peak or total process RSS. The configured pool ceiling was 16 MiB.

These layouts showed an 8–11% elapsed-time reduction with unchanged requested data bytes. The selective predicate is distributed throughout the file, so this does not establish the overread cost for page-prunable data. Cold storage, wider/nested schemas, remote latency, concurrent Spark tasks, and production workloads remain unmeasured.

## Reproduce

From `native/`:

```sh
cargo build --profile ci -p datafusion-comet-parquet --example prefetch_benchmark
target/ci/examples/prefetch_benchmark 64
target/ci/examples/prefetch_benchmark 256
```

The example prints each timing, checksum, byte counter, and reservation sample in CSV form. Temporary Parquet files are deleted at completion.
