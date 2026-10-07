# Spark 3.5.9 vs 4.1.3: differences for the facts in source_notes.md

Source: tag `v3.5.9` of https://github.com/apache/spark (commit
`7c14a3c28b141cc97a330c4d0f5d2a6da7267f85`, "Preparing Spark release
v3.5.9-rc1", 2026-07-12), the version of the image
`apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` used in batch 4, step 3.
Compared file by file with tag `v4.1.3` (commit `77bbf77e`, the reference of
`docs/source_notes.md`). Line numbers below refer to v3.5.9.

Only the three facts checked in `source_notes.md` are covered: (a) the skew
condition and splitting, (b) the HighlyCompressedMapStatus threshold and the
accurate-block configs, (c) which sizes AQE uses.

## Result: no functional difference in (a), (b), (c)

### a. OptimizeSkewedJoin

- `getSkewThreshold` (`OptimizeSkewedJoin.scala:65-68`), `targetSize` (75-83),
  `canSplitLeftSide`/`canSplitRightSide` (85-92), the median over the original
  `bytesByPartitionId` (119-125) and the strict `size > threshold` test
  (146-150): identical code. The whole file differs only in the log message
  formatting (`FileUtils.byteCountToDisplaySize` vs `Utils.bytesToString`) and
  the exception type thrown by `doExecute`.
- `createSkewPartitionSpecs` (`ShufflePartitionsUtil.scala:381-...`),
  `getMapSizesForReduceId` (370) and `splitSizeListByTargetSize` (321-...):
  identical, `SMALL_PARTITION_FACTOR = 0.2`, `MERGED_PARTITION_FACTOR = 1.2`
  (27-28). So in 3.5.9, too, the number of splits is at most the number of map
  tasks.
- Rule order: `OptimizeSkewedJoin(ensureRequirements)` is in
  `queryStagePreparationRules` (`AdaptiveSparkPlanExec.scala:132`),
  `CoalesceShufflePartitions` in `queryStageOptimizerRules` (142): same order.
- `Utils.median` (`Utils.scala:3082-3090`): identical.
- SQLConf defaults (line in 3.5.9 `SQLConf.scala`): `skewedPartitionFactor`
  5.0 (796), `skewedPartitionThresholdInBytes` 256MB (806),
  `advisoryPartitionSizeInBytes` fallback to `targetPostShuffleInputSize` 64MB
  (701, 656), `coalescePartitions.minPartitionSize` 1MB (731),
  `parallelismFirst` true (718), `forceOptimizeSkewedJoin` false (877): same
  values as 4.1.3. The effective configuration recorded by a 3.5.9 test run
  showed no `spark.sql.adaptive.*` key with a different value from a 4.1.3 run.

### b. MapStatus

- `MapStatus.apply` (`MapStatus.scala:78`): same strict condition
  `uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus`; the
  4.1.3 version only passes an additional checksum value through.
- `compressSize` (92-...): same log-1.1 encoding; 4.1.3 adds an explicit
  `.toDouble`.
- HighlyCompressedMapStatus threshold (265-285) and the `size < threshold`
  test (294-298): same logic. 4.1.3 adds explicit `.toDouble` conversions where
  3.5.9 relies on Scala's implicit Long-to-Double widening in
  `Math.max(Double, Long)` / `Math.min(Long, Double)`; the computed value is
  the same. `getSizeForBlock` (198-207) returns `avgSize` for non-huge blocks,
  as in 4.1.3.
- Configs (`core/.../internal/config/package.scala`):

| config | default | 3.5.9 lines | difference |
|---|---|---|---|
| `spark.shuffle.minNumPartitionsToHighlyCompress` | 2000 (internal) | 1627-... | none |
| `spark.shuffle.accurateBlockThreshold` | 100 MiB | 1258-1265 | none |
| `spark.shuffle.accurateBlockSkewedFactor` | -1.0 | 1267-1277 | marked `.internal()` in 3.5.9, public in 4.1.3; same doc text, same default, same use |
| `spark.shuffle.maxAccurateSkewedBlockNumber` | 100 (internal) | 1279-... | none |

The only difference: `spark.shuffle.accurateBlockSkewedFactor` is an internal
(undocumented) config in 3.5.9. It is read and applied the same way.

### c. Which sizes AQE uses

- `MapOutputTrackerMaster.getStatistics` (`MapOutputTracker.scala:992-...`):
  sums `getSizeForBlock` over all map statuses as in 4.1.3 (4.1.3 only changes
  how the shuffle status is looked up).
- Shuffle writers: `BypassMergeSortShuffleWriter.java:143-144, 181-182` and
  `UnsafeShuffleWriter.java:233-234` build the MapStatus from the written
  partition lengths (4.1.3 additionally passes a checksum).
  `SortShuffleWriter.shouldBypassMergeSort` (`SortShuffleWriter.scala:102-110`)
  and `spark.shuffle.sort.bypassMergeThreshold` = 200 (`config` 1554): same.
- `DiskBlockObjectWriter.scala:171-172` (`wrapStream` then `serializeStream`),
  `SerializerManager.shouldCompress` (110-...), `spark.shuffle.compress` = true
  (1503), `spark.io.compression.codec` = lz4 (1852): same.
- Runtime broadcast decision: `ShuffleExchangeExec.runtimeStatistics`
  (`ShuffleExchangeExec.scala:161-166`), `UnsafeRowSerializer.writeValue`
  (63-67), `QueryStageExec.computeStats` (74-...) and
  `JoinSelectionHelper.canBroadcastBySize` (`joins.scala:361-370`): identical
  to 4.1.3.

So in 3.5.9, as in 4.1.3, skew detection, splitting and coalescing use the
compressed, serialized shuffle block bytes as encoded by MapStatus, and the
runtime broadcast decision uses uncompressed UnsafeRow bytes.

## Outside the three facts (for reference only)

`CoalesceShufflePartitions` in 4.1.3 has an additional case: for a coalesce
group containing a `BroadcastNestedLoopJoinExec` or `CartesianProductExec`
("exploding join") the advisory size is replaced by
`coalescePartitions.minPartitionSize` (4.1.3 `CoalesceShufflePartitions.scala:131-134,
189-193`). 3.5.9 has no such case. None of the queries in this project contains
these operators.
