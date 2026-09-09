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

# Native Shuffle

This document describes Comet's native shuffle implementation (`CometNativeShuffle`), which performs
shuffle operations entirely in Rust code for maximum performance. For the JVM-based alternative,
see [JVM Shuffle](jvm_shuffle.md).

## Overview

Native shuffle takes columnar input directly from Comet native operators and performs partitioning,
encoding, and writing in native Rust code. This avoids the columnar-to-row-to-columnar conversion
overhead that JVM shuffle incurs.

```
Comet Native (columnar) → Native Shuffle → Arrow IPC → columnar
```

Compare this to JVM shuffle's data path:

```
Comet Native (columnar) → ColumnarToRowExec → rows → JVM Shuffle → Arrow IPC → columnar
```

## When Native Shuffle is Used

Native shuffle (`CometExchange`) is selected when all of the following conditions are met:

1. **Shuffle mode allows native**: `spark.comet.shuffle.mode` is `native` or `auto`.

2. **Child plan is a Comet native operator**: The child must be a `CometPlan` that produces
   columnar output. Row-based Spark operators require JVM shuffle.

3. **Supported partitioning type**: Native shuffle supports:
   - `HashPartitioning`
   - `RangePartitioning`
   - `SinglePartition`
   - `RoundRobinPartitioning`, which is disabled by default because Comet assigns round robin
     partitions differently from Spark. Enable it with
     `spark.comet.shuffle.native.partitioning.roundrobin.enabled`.

4. **Supported partition key types**: For `HashPartitioning` and `RangePartitioning`, partition
   keys must be primitive types. Complex types (struct, array, map) as partition keys require
   JVM shuffle. Note that complex types are fully supported as data columns in native shuffle.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CometShuffleManager                                │
│  - Routes to CometNativeShuffleWriter for CometNativeShuffleHandle           │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CometNativeShuffleWriter                             │
│  - Builds protobuf operator plan: ShuffleWriter(child = childNativeOp)       │
│  - Reads per-partition leaf iterators from CometNativeShuffleInputIterator   │
│  - Drives one CometExecIterator per partition                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ (JNI)
┌─────────────────────────────────────────────────────────────────────────────┐
│                         ShuffleWriterExec (Rust)                             │
│  - DataFusion ExecutionPlan                                                  │
│  - Orchestrates partitioning and writing                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                    │                                     │
                    ▼                                     ▼
┌───────────────────────────────────┐   ┌───────────────────────────────────┐
│ MultiPartitionShuffleRepartitioner │   │ SinglePartitionShufflePartitioner │
│ (hash/range/round-robin)           │   │ (single partition case)           │
└───────────────────────────────────┘   └───────────────────────────────────┘
                    │
                    ▼
┌───────────────────────────────────┐
│ ShuffleBlockWriter                 │
│ (Arrow IPC + compression)          │
└───────────────────────────────────┘
                    │
            ┌───────┴───────────────────┐
            ▼                           ▼
┌──────────────────────┐  ┌──────────────────────────┐
│ LocalPartitionWriter │  │ RssPartitionWriter       │
│ (data + index files) │  │ (push to remote shuffle) │
└──────────────────────┘  └──────────────────────────┘
```

## Key Classes

### Scala Side

| Class                          | Location                                         | Description                                                                                                                                         |
| ------------------------------ | ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CometShuffleExchangeExec`     | `.../shuffle/CometShuffleExchangeExec.scala`     | Physical plan node. Validates types and partitioning, creates `CometShuffleDependency`.                                                             |
| `CometNativeShuffleWriter`     | `.../shuffle/CometNativeShuffleWriter.scala`     | Implements `ShuffleWriter`. Builds the unified `ShuffleWriter(child = childNativeOp)` plan and runs it in one `CometExecIterator` per partition.    |
| `CometShuffleDependency`       | `.../shuffle/CometShuffleDependency.scala`       | Extends `ShuffleDependency`. Holds shuffle type, schema, range partition bounds, and (native shuffle only) a `NativeShuffleSpec`.                   |
| `CometNativeShuffleInputRDD`   | `.../shuffle/CometNativeShuffleInputRDD.scala`   | Thin scheduling-anchor RDD on the native-shuffle path. `compute` returns a `CometNativeShuffleInputIterator` carrying per-partition leaf iterators. |
| `CometBlockStoreShuffleReader` | `.../shuffle/CometBlockStoreShuffleReader.scala` | Reads shuffle blocks via `ShuffleBlockFetcherIterator`. Decodes Arrow IPC to `ColumnarBatch`.                                                       |
| `NativeBatchDecoderIterator`   | `.../shuffle/NativeBatchDecoderIterator.scala`   | Reads compressed Arrow IPC from input stream. Calls native decode via JNI.                                                                          |

### Rust Side

Native shuffle lives in its own crate, `datafusion-comet-shuffle`, rooted at `native/shuffle/`.

| File                        | Location                            | Description                                                                                                         |
| --------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `shuffle_writer.rs`         | `native/shuffle/src/`               | `ShuffleWriterExec` plan. Chooses the partitioner and the output destination (`ShuffleWriterDestination`).          |
| `comet_partitioning.rs`     | `native/shuffle/src/`               | `CometPartitioning` enum defining partition schemes (`SinglePartition`, `Hash`, `RangePartitioning`, `RoundRobin`). |
| `ipc.rs`                    | `native/shuffle/src/`               | `read_ipc_compressed`, the decode side called over JNI by `Native.decodeShuffleBlock`.                              |
| `multi_partition.rs`        | `native/shuffle/src/partitioners/`  | `MultiPartitionShuffleRepartitioner`. Assigns partition ids, buffers rows per partition, and spills.                |
| `single_partition.rs`       | `native/shuffle/src/partitioners/`  | `SinglePartitionShufflePartitioner`. Streams batches straight to the writer.                                        |
| `empty_schema.rs`           | `native/shuffle/src/partitioners/`  | `EmptySchemaShufflePartitioner`. Row counts only, for zero-column batches such as `COUNT(*)`.                       |
| `shuffle_block_writer.rs`   | `native/shuffle/src/writers/`       | `ShuffleBlockWriter` and `CompressionCodec`. Arrow IPC encoding with compression.                                   |
| `local_partition_writer.rs` | `native/shuffle/src/writers/local/` | `LocalPartitionWriter`. Writes the data file plus the index file of partition offsets.                              |
| `rss_partition_writer.rs`   | `native/shuffle/src/writers/rss/`   | `RssPartitionWriter`. Pushes complete encoded blocks to a remote shuffle service.                                   |

## Data Flow

### Write Path

1. **Plan construction**: `CometNativeShuffleWriter` builds a protobuf operator tree with a
   `ShuffleWriter` operator at the root and `childNativeOp` as its child. `childNativeOp` takes
   one of two shapes:
   - The child plan's `nativeOp` directly, when `CometShuffleExchangeExec`'s child is a
     `CometNativeExec` subtree. The upstream operators run inside the same `CometExecIterator`
     as the writer, with no JVM-to-native batch boundary between them.
   - A synthetic `Scan("ShuffleWriterInput")` placeholder, when the dep was built via the
     convenience `prepareShuffleDependency(rdd, ...)` overload (used by
     `CometCollectLimitExec` and `CometTakeOrderedAndProjectExec`, or when the
     exchange's child is a non-native `CometPlan` such as `CometSparkToColumnarExec`). Native
     code reads `ColumnarBatch`es from the JVM input iterator via Arrow C Stream Interface.

2. **Native execution**: A single `CometExecIterator` per partition runs the unified plan.

3. **Partitioning**: `ShuffleWriterExec` receives batches and routes to the appropriate partitioner:
   - `MultiPartitionShuffleRepartitioner`: For hash/range/round-robin partitioning
   - `SinglePartitionShufflePartitioner`: For single partition (simpler path)
   - `EmptySchemaShufflePartitioner`: For zero-column input, which carries a row count but no
     data columns. It overrides whatever partitioning was requested.

4. **Buffering and spilling**: The partitioner buffers rows per partition. When memory pressure
   exceeds the threshold, partitions spill to temporary files.

5. **Encoding**: `ShuffleBlockWriter` encodes each partition's data as compressed Arrow IPC:
   - Writes compression type header
   - Writes field count header
   - Writes compressed IPC stream

6. **Output**: The encoded blocks go to one of two destinations, selected by
   `ShuffleWriterDestination`. `LocalPartitionWriter` produces two files:
   - **Data file**: Concatenated partition data
   - **Index file**: Array of 8-byte little-endian offsets marking partition boundaries

   `RssPartitionWriter` instead pushes each complete encoded block to a task-owned callback that
   forwards it to a remote shuffle service, writing no local files. `CometCelebornShuffleManager`
   wires this path on the JVM side. Block boundaries are never split across pushes, so the service
   can concatenate payloads without repairing partial blocks. A batch whose encoded frame would
   exceed `spark.comet.shuffle.rss.maxFrameBytes` is halved and pushed as several complete blocks;
   the write fails only when a single row still does not fit.

7. **Commit**: Back in JVM, `CometNativeShuffleWriter` reads the index file to get partition
   lengths and commits via Spark's `IndexShuffleBlockResolver`. The remote path has no index file:
   it takes partition lengths from the pusher's `finish()` and builds the `MapStatus` from those.

### Read Path

1. `CometBlockStoreShuffleReader` fetches shuffle blocks via `ShuffleBlockFetcherIterator`.

2. For each block, `NativeBatchDecoderIterator`:
   - Reads the 8-byte compressed length header
   - Reads the 8-byte field count header
   - Reads the compressed IPC data
   - Calls `Native.decodeShuffleBlock()` via JNI

3. Native code decompresses and deserializes the Arrow IPC stream.

4. Arrow FFI transfers the `RecordBatch` to JVM as a `ColumnarBatch`.

When `spark.comet.shuffle.directRead.enabled` is set (the default) and the shuffle output feeds a
native operator through an AQE shuffle stage, the exchange serializes as a `ShuffleScan` instead.
Native code then reads the compressed blocks itself and the Arrow FFI hand-off in step 4 is
skipped. Types the native scan cannot handle fall back to the path above.

## Partitioning

### Hash Partitioning

Native shuffle implements Spark-compatible hash partitioning:

- Uses Murmur3 hash function with seed 42 (matching Spark)
- Computes hash of partition key columns
- Applies Spark's positive modulo by partition count: `partition_id = pmod(hash, num_partitions)`.
  The hash is read as a signed 32-bit value, so `pmod` folds negative remainders back into range.

### Range Partitioning

For range partitioning:

1. Spark's `RangePartitioner` samples data and computes partition boundaries on the driver.
2. Boundaries are serialized to the native plan.
3. Native code converts sort key columns to comparable row format.
4. Binary search (`partition_point`) determines which partition each row belongs to.

### Single Partition

The simplest case: all rows go to partition 0. Uses `SinglePartitionShufflePartitioner`, which
streams each batch straight to the writer, whose `BatchCoalescer` combines small batches up to the
configured batch size. Batches already at least that size pass through unchanged, so a large input
batch is written as a single block that may exceed the batch size.

### Round Robin Partitioning

Comet implements round robin partitioning using hash-based assignment for determinism:

1. Computes a Murmur3 hash of columns (using seed 42). By default every column is hashed;
   `spark.comet.shuffle.native.partitioning.roundrobin.maxHashColumns` limits it to the first N.
2. Assigns partitions directly using the hash: `partition_id = pmod(hash, num_partitions)`

This approach guarantees determinism across retries, which is critical for fault tolerance.
However, unlike true round robin which cycles through partitions row-by-row, hash-based
assignment only provides even distribution when the data has sufficient variation in the
hashed columns. Data with low cardinality or identical values may result in skewed partition
sizes.

## Memory Management

Native shuffle uses DataFusion's memory management with spilling support:

- **Memory pool**: Tracks memory usage across the shuffle operation.
- **Spill triggers**: Partitions spill to disk when the memory pool denies an allocation, or
  when the buffered bytes reach `spark.comet.shuffle.native.maxBufferBytes`. That config defaults to
  0, which disables the fixed limit and leaves memory pressure as the only trigger.
- **Per-partition spilling**: Each partition has its own spill file. Multiple spills for a
  partition are concatenated when writing the final output.
- **Scratch space**: Reusable buffers for partition ID computation to reduce allocations.

The `MultiPartitionShuffleRepartitioner` holds:

- `buffered_batches`: input batches kept in memory, with `partition_indices` recording which rows
  of each batch belong to which output partition
- `reservation`: a DataFusion `MemoryReservation` registered as a spillable `MemoryConsumer`
- `scratch`: the reusable partition ID buffers described above

Spilling runs through the partition writer. `LocalPartitionWriter` keeps one `SpillWriter` per
output partition and merges its spill file into the data file during `finish_partition`.

## Compression

Native shuffle supports multiple compression codecs configured via
`spark.comet.shuffle.compression.codec`:

| Codec    | Description                                            |
| -------- | ------------------------------------------------------ |
| `zstd`   | Zstandard compression. Best ratio, configurable level. |
| `lz4`    | LZ4 compression. Fast with good ratio.                 |
| `snappy` | Snappy compression. Fastest, lower ratio.              |

`spark.shuffle.compress=false` turns compression off entirely; `none` is not a value the codec
config accepts.

The compression codec is applied uniformly to all partitions. Each partition's data is
independently compressed, allowing parallel decompression during reads.

## Configuration

| Config                                                              | Default | Description                                                                    |
| ------------------------------------------------------------------- | ------- | ------------------------------------------------------------------------------ |
| `spark.comet.shuffle.enabled`                                       | `true`  | Enable Comet shuffle                                                           |
| `spark.comet.shuffle.mode`                                          | `auto`  | Shuffle mode: `native`, `jvm`, or `auto`                                       |
| `spark.comet.shuffle.native.partitioning.hash.enabled`              | `true`  | Allow native shuffle for `HashPartitioning`                                    |
| `spark.comet.shuffle.native.partitioning.range.enabled`             | `true`  | Allow native shuffle for `RangePartitioning`                                   |
| `spark.comet.shuffle.native.partitioning.roundrobin.enabled`        | `false` | Allow native shuffle for `RoundRobinPartitioning`                              |
| `spark.comet.shuffle.native.partitioning.roundrobin.maxHashColumns` | `0`     | Leading columns to hash for round robin; `0` hashes all columns                |
| `spark.comet.shuffle.compression.codec`                             | `lz4`   | Compression codec: `zstd`, `lz4`, or `snappy`                                  |
| `spark.comet.shuffle.compression.zstd.level`                        | `1`     | Zstd compression level                                                         |
| `spark.comet.shuffle.native.writeBufferSize`                        | `1MB`   | Write buffer size for local shuffle files                                      |
| `spark.comet.shuffle.native.maxBufferBytes`                         | `0`     | Bytes buffered before spilling; `0` leaves memory pressure as the only trigger |
| `spark.comet.shuffle.directRead.enabled`                            | `true`  | Read shuffle blocks in native code, bypassing Arrow FFI                        |
| `spark.comet.shuffle.rss.maxFrameBytes`                             | `64MB`  | Largest encoded block pushed to a remote shuffle service                       |
| `spark.comet.shuffle.rss.maxInFlightBytes`                          | `512MB` | Shuffle bytes admitted concurrently per executor-side remote shuffle client    |
| `spark.comet.batchSize`                                             | `8192`  | Target rows per batch in native shuffle output                                 |

## Comparison with JVM Shuffle

| Aspect              | Native Shuffle                         | JVM Shuffle                       |
| ------------------- | -------------------------------------- | --------------------------------- |
| Input format        | Columnar (direct from Comet operators) | Row-based (via ColumnarToRowExec) |
| Partitioning logic  | Rust implementation                    | Spark's partitioner               |
| Supported schemes   | Hash, Range, Single, RoundRobin        | Hash, Range, Single, RoundRobin   |
| Partition key types | Primitives only (Hash, Range)          | Any type                          |
| Performance         | Higher (no format conversion)          | Lower (columnar→row→columnar)     |
| Writer variants     | Local files or remote shuffle push     | Bypass (hash) and sort-based      |

See [JVM Shuffle](jvm_shuffle.md) for details on the JVM-based implementation.
