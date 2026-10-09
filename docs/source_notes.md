# Spark 4.1.3 source notes: skew join, coalescing, MapStatus, shuffle sizes

Source: `git clone --branch v4.1.3 https://github.com/apache/spark`
(commit `77bbf77e86ad48f58b5dfbc6ac882b3e70cf1989`, 2026-07-11).
All line numbers refer to that commit. Code is quoted verbatim; `...` marks
omitted lines.

Abbreviations: `adaptive/` = `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/`,
`SQLConf` = `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`,
`config` = `core/src/main/scala/org/apache/spark/internal/config/package.scala`.

---

## a. OptimizeSkewedJoin

### Skew condition

`adaptive/OptimizeSkewedJoin.scala:64-67`

```scala
  def getSkewThreshold(medianSize: Long): Long = {
    conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_THRESHOLD).max(
      (medianSize * conf.getConf(SQLConf.SKEW_JOIN_SKEWED_PARTITION_FACTOR)).toLong)
  }
```

`adaptive/OptimizeSkewedJoin.scala:143-147`

```scala
    for (partitionIndex <- 0 until numPartitions) {
      val leftSize = leftSizes(partitionIndex)
      val isLeftSkew = canSplitLeft && leftSize > leftSkewThreshold
      val rightSize = rightSizes(partitionIndex)
      val isRightSkew = canSplitRight && rightSize > rightSkewThreshold
```

A partition of one join side is skewed iff
`size > max(skewedPartitionThresholdInBytes, median * skewedPartitionFactor)`
(strictly greater). Each side is tested against its own median and threshold.
Which sides may be split depends on the join type
(`OptimizeSkewedJoin.scala:84-91`):

```scala
  private def canSplitLeftSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == LeftSemi ||
      joinType == LeftAnti || joinType == LeftOuter
  }

  private def canSplitRightSide(joinType: JoinType) = {
    joinType == Inner || joinType == Cross || joinType == RightOuter
  }
```

So for `facts LEFT JOIN dim` only the left (facts) side can be split.

A partition counts as skewed (`numSkewedLeft += 1`) only if it could actually
be split (`OptimizeSkewedJoin.scala:153-165`): `createSkewPartitionSpecs`
returns `None` when it would produce a single split, and the partition is then
left as is.

If the rewritten plan would need an extra shuffle, the optimization is dropped
unless `spark.sql.adaptive.forceOptimizeSkewedJoin=true`
(`OptimizeSkewedJoin.scala:229-245`).

### Which joins can be optimized at all (plan shape), and forceOptimizeSkewedJoin

Added in batch 5c. `adaptive/OptimizeSkewedJoin.scala:200-217`:

```scala
  def optimizeSkewJoin(plan: SparkPlan): SparkPlan = plan.transformUp {
    case smj @ SortMergeJoinExec(_, _, joinType, _,
        s1 @ SortExec(_, _, ShuffleStage(left: ShuffleQueryStageExec), _),
        s2 @ SortExec(_, _, ShuffleStage(right: ShuffleQueryStageExec), _), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          smj.copy(
            left = s1.copy(child = newLeft), right = s2.copy(child = newRight), isSkewJoin = true)
      }.getOrElse(smj)

    case shj @ ShuffledHashJoinExec(_, _, joinType, _, _,
        ShuffleStage(left: ShuffleQueryStageExec),
        ShuffleStage(right: ShuffleQueryStageExec), false) =>
      tryOptimizeJoinChildren(left, right, joinType).map {
        case (newLeft, newRight) =>
          shj.copy(left = newLeft, right = newRight, isSkewJoin = true)
      }.getOrElse(shj)
  }
```

`adaptive/OptimizeSkewedJoin.scala:248-255`:

```scala
  object ShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec if s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS =>
        Some(s)
      case _ => None
    }
  }
```

A sort-merge join is a candidate only if **each** child is a `SortExec` whose
child is a materialized shuffle query stage (origin ENSURE_REQUIREMENTS). If
an operator such as `HashAggregateExec` sits between the `Sort` and the
shuffle stage on either side (a per-key table computed with GROUP BY in the
same query), the pattern does not match and the join is left unchanged; the
partition sizes are never examined. (`AQEShuffleRead`, seen between `Sort` and
the stage in final plans, is inserted later by `CoalesceShufflePartitions`,
which runs after `OptimizeSkewedJoin`, see the rule order above.)

`adaptive/OptimizeSkewedJoin.scala:219-246` (`apply`):

```scala
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKEW_JOIN_ENABLED)) {
      return plan
    }
    ...
    val optimized = optimizeSkewJoin(plan)
    val requirementSatisfied = if (ensureRequirements.requiredDistribution.isDefined) {
      ValidateRequirements.validate(optimized, ensureRequirements.requiredDistribution.get)
    } else {
      ValidateRequirements.validate(optimized)
    }
    if (requirementSatisfied) {
      optimized.transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else if (conf.getConf(SQLConf.ADAPTIVE_FORCE_OPTIMIZE_SKEWED_JOIN)) {
      ensureRequirements.apply(optimized).transform {
        case SkewJoinChildWrapper(child) => child
      }
    } else {
      plan
    }
  }
```

`spark.sql.adaptive.forceOptimizeSkewedJoin` (`SQLConf.scala:1231-1236`, default
false, doc: "When true, force enable OptimizeSkewedJoin even if it introduces
extra shuffle.") is consulted only after `optimizeSkewJoin` has rewritten a
join and the rewritten plan fails the distribution check; it keeps that
rewrite and adds the shuffles. It does not change the pattern match above, so
it has no effect on a join whose children do not match.

### Which sizes, and is the median taken before or after coalescing

`adaptive/OptimizeSkewedJoin.scala:118-124`

```scala
    val leftSizes = left.mapStats.get.bytesByPartitionId
    val rightSizes = right.mapStats.get.bytesByPartitionId
    assert(leftSizes.length == rightSizes.length)
    val numPartitions = leftSizes.length
    // We use the median size of the original shuffle partitions to detect skewed partitions.
    val leftMedSize = Utils.median(leftSizes, false)
    val rightMedSize = Utils.median(rightSizes, false)
```

The median is computed over `bytesByPartitionId` of the **original** shuffle
partitions (all `spark.sql.shuffle.partitions` of them, empty ones included),
i.e. **before coalescing**. This also follows from the rule order in
`adaptive/AdaptiveSparkPlanExec.scala`: `OptimizeSkewedJoin` is the last of
the `queryStagePreparationRules` (line 131), while `CoalesceShufflePartitions`
is in `queryStageOptimizerRules` (line 141), which run afterwards on the
already skew-split plan:

```scala
110  @transient private val queryStagePreparationRules: Seq[Rule[SparkPlan]] = {
...
131      OptimizeSkewedJoin(ensureRequirements)
...
137  @transient private val queryStageOptimizerRules: Seq[Rule[SparkPlan]] = Seq(
138    PlanAdaptiveDynamicPruningFilters(this),
139    ReuseAdaptiveSubquery(context.subqueryCache),
140    OptimizeSkewInRebalancePartitions,
141    CoalesceShufflePartitions(context.session),
```

`Utils.median` (`core/src/main/scala/org/apache/spark/util/Utils.scala:3140-3148`):

```scala
  def median(sizes: Array[Long], alreadySorted: Boolean): Long = {
    val len = sizes.length
    val sortedSize = if (alreadySorted) sizes else sizes.sorted
    len match {
      case _ if (len % 2 == 0) =>
        math.max((sortedSize(len / 2) + sortedSize(len / 2 - 1)) / 2, 1)
      case _ => math.max(sortedSize(len / 2), 1)
    }
  }
```

### Number of splits

Target split size, `adaptive/OptimizeSkewedJoin.scala:74-82`:

```scala
  private def targetSize(sizes: Array[Long], skewThreshold: Long): Long = {
    val advisorySize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES)
    val nonSkewSizes = sizes.filter(_ <= skewThreshold)
    if (nonSkewSizes.isEmpty) {
      advisorySize
    } else {
      math.max(advisorySize, nonSkewSizes.sum / nonSkewSizes.length)
    }
  }
```

i.e. `max(advisoryPartitionSizeInBytes, mean size of the non-skewed partitions)`.

Splitting, `adaptive/ShufflePartitionsUtil.scala:372-410`:

```scala
  private def getMapSizesForReduceId(shuffleId: Int, partitionId: Int): Array[Long] = {
    val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTracker.shuffleStatuses(shuffleId).withMapStatuses(_.map { stat =>
      if (stat == null) -1 else stat.getSizeForBlock(partitionId)
    })
  }
...
  def createSkewPartitionSpecs(
      shuffleId: Int,
      reducerId: Int,
      targetSize: Long,
      smallPartitionFactor: Double = SMALL_PARTITION_FACTOR)
  : Option[Seq[PartialReducerPartitionSpec]] = {
    val mapPartitionSizes = getMapSizesForReduceId(shuffleId, reducerId)
    if (mapPartitionSizes.exists(_ < 0)) return None
    val mapStartIndices = splitSizeListByTargetSize(
      mapPartitionSizes, targetSize, smallPartitionFactor)
    if (mapStartIndices.length > 1) {
      Some(mapStartIndices.indices.map { i =>
        val startMapIndex = mapStartIndices(i)
        val endMapIndex = if (i == mapStartIndices.length - 1) {
          mapPartitionSizes.length
        } else {
          mapStartIndices(i + 1)
        }
        ...
        PartialReducerPartitionSpec(reducerId, startMapIndex, endMapIndex, dataSize)
      })
    } else {
      None
```

The skewed reduce partition is described as one block size per **map task**
(`getSizeForBlock(partitionId)` of each map output), and split into
contiguous ranges of map indices (`startMapIndex until endMapIndex`). A split
never divides a single map task's block. Therefore:

**number of splits <= number of map tasks of that shuffle.**

`splitSizeListByTargetSize` (`ShufflePartitionsUtil.scala:323-365`) packs
consecutive map blocks greedily until adding the next one would exceed
`targetSize`, then merges a split into its neighbour if either is smaller than
`targetSize * 0.2` (`SMALL_PARTITION_FACTOR`, line 27) or both together are
below `targetSize * 1.2` (`MERGED_PARTITION_FACTOR`, line 28):

```scala
    def tryMergePartitions() = {
      ...
      val shouldMergePartitions = lastPartitionSize > -1 &&
        ((currentPartitionSize + lastPartitionSize) < targetSize * MERGED_PARTITION_FACTOR ||
        (currentPartitionSize < targetSize * smallPartitionFactor ||
          lastPartitionSize < targetSize * smallPartitionFactor))
```

There is no separate maximum-split config in 4.1.3.

### Coalescing (for reference)

`adaptive/ShufflePartitionsUtil.scala:58-61`:

```scala
    val totalPostShuffleInputSize = mapOutputStatistics.flatMap(_.map(_.bytesByPartitionId.sum)).sum
    val maxTargetSize = math.ceil(totalPostShuffleInputSize / minNumPartitions.toDouble).toLong
    // It's meaningless to make target size smaller than minPartitionSize.
    val targetSize = maxTargetSize.min(advisoryTargetSize).max(minPartitionSize)
```

With `parallelismFirst=true`, `minNumPartitions` is
`sparkContext.defaultParallelism` (`adaptive/CoalesceShufflePartitions.scala:59-63`),
which is 9 (total executor cores) on this cluster. Coalescing keeps skew-split
specs intact and only merges partitions outside them
(`ShufflePartitionsUtil.scala:31-35, 109-...`).

---

## b. MapStatus

### When HighlyCompressedMapStatus is used

`core/src/main/scala/org/apache/spark/scheduler/MapStatus.scala:76-90`

```scala
  private lazy val minPartitionsToUseHighlyCompressMapStatus = Option(SparkEnv.get)
    .map(_.conf.get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS))
    .getOrElse(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.defaultValue.get)

  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      mapTaskId: Long,
      checksumVal: Long = 0): MapStatus = {
    if (uncompressedSizes.length > minPartitionsToUseHighlyCompressMapStatus) {
      HighlyCompressedMapStatus(loc, uncompressedSizes, mapTaskId, checksumVal)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes, mapTaskId, checksumVal)
    }
  }
```

`config:1808-1815`

```scala
  private[spark] val SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS =
    ConfigBuilder("spark.shuffle.minNumPartitionsToHighlyCompress")
      .internal()
      ...
      .createWithDefault(2000)
```

Condition is strictly greater: **2000 partitions -> CompressedMapStatus,
2001 -> HighlyCompressedMapStatus.** (`uncompressedSizes` is the name of the
parameter; it means "not yet encoded into a byte", see c.)

### What each stores

`CompressedMapStatus` stores one byte per reduce partition, the size encoded as
`ceil(log_1.1(size))` (`MapStatus.scala:92-118, 141-157`):

```scala
  private[this] val LOG_BASE = 1.1
  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    ...
      math.min(255, math.ceil(math.log(size.toDouble) / math.log(LOG_BASE)).toInt).toByte
```

So every block size AQE sees is rounded **up** to the next power of 1.1
(at most +10%).

`HighlyCompressedMapStatus` (`MapStatus.scala:181-226, 265-338`) stores:
a bitmap of empty blocks, the exact (log-1.1 encoded) size of "huge" blocks,
and **one average size for all other non-empty blocks**:

```scala
  override def getSizeForBlock(reduceId: Int): Long = {
    assert(hugeBlockSizes != null)
    if (emptyBlocks.contains(reduceId)) {
      0
    } else {
      hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => avgSize
      }
    }
```

Which blocks are "huge" (`MapStatus.scala:282-327`):

```scala
    val threshold =
      if (accurateBlockSkewedFactor > 0) {
        val sortedSizes = uncompressedSizes.sorted
        val medianSize: Long = Utils.median(sortedSizes, true)
        val maxAccurateSkewedBlockNumber =
          Math.min(
            Option(SparkEnv.get)
              .map(_.conf.get(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER))
              .getOrElse(config.SHUFFLE_MAX_ACCURATE_SKEWED_BLOCK_NUMBER.defaultValue.get),
            totalNumBlocks
          )
        val skewSizeThreshold =
          Math.max(
            medianSize * accurateBlockSkewedFactor,
            sortedSizes(totalNumBlocks - maxAccurateSkewedBlockNumber).toDouble
          )
        Math.min(shuffleAccurateBlockThreshold.toDouble, skewSizeThreshold)
      } else {
        // Disable skew detection if accurateBlockSkewedFactor <= 0
        shuffleAccurateBlockThreshold.toDouble
      }
    ...
        if (size < threshold) {
          totalSmallBlockSize += size
          numSmallBlocks += 1
        } else {
          hugeBlockSizes(i) = MapStatus.compressSize(uncompressedSizes(i))
        }
```

The threshold is per map task, over that task's own blocks.

### Configs (`config`)

| config | default | lines |
|---|---|---|
| `spark.shuffle.minNumPartitionsToHighlyCompress` (internal) | `2000` | 1808-1815 |
| `spark.shuffle.accurateBlockThreshold` | `100 * 1024 * 1024` (100 MiB) | 1403-1410 |
| `spark.shuffle.accurateBlockSkewedFactor` | `-1.0` (disabled) | 1412-1421 |
| `spark.shuffle.maxAccurateSkewedBlockNumber` (internal) | `100` | 1423-1433 |

`config:1412-1421`:

```scala
  private[spark] val SHUFFLE_ACCURATE_BLOCK_SKEWED_FACTOR =
    ConfigBuilder("spark.shuffle.accurateBlockSkewedFactor")
      .doc("A shuffle block is considered as skewed and will be accurately recorded in " +
        "HighlyCompressedMapStatus if its size is larger than this factor multiplying " +
        "the median shuffle block size or SHUFFLE_ACCURATE_BLOCK_THRESHOLD. It is " +
        "recommended to set this parameter to be the same as SKEW_JOIN_SKEWED_PARTITION_FACTOR." +
        "Set to -1.0 to disable this feature by default.")
      .version("3.3.0")
      .doubleConf
      .createWithDefault(-1.0)
```

So with default settings and more than 2000 partitions, a map task's block
for a skewed reduce partition is reported as the average block size unless
that block alone is at least 100 MiB.

---

## c. Which sizes AQE uses

### Skew detection and coalescing: compressed, serialized shuffle block bytes

Both use `MapOutputStatistics.bytesByPartitionId` (skew:
`OptimizeSkewedJoin.scala:118-119`; coalescing: `ShufflePartitionsUtil.scala:58`)
and, for splitting, the per-map-task `getSizeForBlock`
(`ShufflePartitionsUtil.scala:372-377`). `bytesByPartitionId` is the sum of
`getSizeForBlock` over all map statuses
(`core/src/main/scala/org/apache/spark/MapOutputTracker.scala:1013-1043`):

```scala
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    ...
        statuses.filter(_ != null).foreach { s =>
          for (i <- totalSizes.indices) {
            totalSizes(i) += s.getSizeForBlock(i)
          }
        }
    ...
      new MapOutputStatistics(dep.shuffleId, totalSizes)
```

The sizes put into the MapStatus are the partition lengths of the shuffle
data file. For `<= 200` partitions SQL shuffles use
`BypassMergeSortShuffleWriter` (`SortShuffleWriter.scala:118-125`,
`SortShuffleManager.scala:93-104`; default
`spark.shuffle.sort.bypassMergeThreshold=200`, `config:1735-1741`), otherwise
`UnsafeShuffleWriter`. Both build the MapStatus from the bytes written per
partition:

`core/src/main/java/org/apache/spark/shuffle/sort/BypassMergeSortShuffleWriter.java:169-170, 197-199, 230-231, 264-265`

```java
        DiskBlockObjectWriter writer =
          blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
...
      partitionLengths = writePartitionedData(mapOutputWriter);
      mapStatus = MapStatus$.MODULE$.apply(
        blockManager.shuffleServerId(), partitionLengths, mapId, getAggregatedChecksumValue());
...
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
...
    return mapOutputWriter.commitAllPartitions(getChecksumValues(partitionChecksums))
      .getPartitionLengths();
```

`UnsafeShuffleWriter.java:250-251` does the same with its merged spill lengths.

The bytes are written through the serializer and then the compression codec:
`core/src/main/scala/org/apache/spark/storage/DiskBlockObjectWriter.scala:172-173`

```scala
    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
```

`core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala:69, 110-121, 133-135`

```scala
  private[this] val compressShuffle = conf.get(config.SHUFFLE_COMPRESS)
...
  private def shouldCompress(blockId: BlockId): Boolean = {
    blockId match {
      case _: ShuffleBlockId => compressShuffle
...
      case _: TempShuffleBlockId => compressShuffle
...
  def wrapStream(blockId: BlockId, s: OutputStream): OutputStream = {
    wrapForCompression(blockId, wrapForEncryption(s))
  }
```

`spark.shuffle.compress` defaults to `true` (`config:1684-1690`) and the codec
is `spark.io.compression.codec`, default `lz4` (`config:2093-2101`; there is
no separate shuffle codec config).

**Answer: coalescing, skew detection and skew splitting use the size of each
shuffle block as written to disk: UnsafeRow-serialized, then compressed with
`spark.io.compression.codec` (and encrypted if enabled), then encoded by
MapStatus (log-1.1 rounding up, or the average for non-huge blocks in
HighlyCompressedMapStatus).**

### Runtime broadcast decision: uncompressed UnsafeRow bytes

`adaptive/QueryStageExec.scala:84-89, 237` and
`sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala:200, 207, 229-233`:

```scala
  def computeStats(): Option[Statistics] = if (isMaterialized) {
    val runtimeStats = getRuntimeStatistics
    val dataSize = runtimeStats.sizeInBytes.max(0)
...
  override def getRuntimeStatistics: Statistics = shuffle.runtimeStatistics
```

```scala
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
...
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))
...
  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }
```

`sql/core/src/main/scala/org/apache/spark/sql/execution/UnsafeRowSerializer.scala:63-66`:

```scala
    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val row = value.asInstanceOf[UnsafeRow]
      if (dataSize != null) {
        dataSize.add(row.getSizeInBytes)
```

The threshold check, `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala:367-375`:

```scala
  def canBroadcastBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
    val autoBroadcastJoinThreshold = if (plan.stats.isRuntime) {
      conf.getConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD)
        .getOrElse(conf.autoBroadcastJoinThreshold)
    } else {
      conf.autoBroadcastJoinThreshold
    }
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= autoBroadcastJoinThreshold
  }
```

**Answer: the runtime broadcast decision uses the sum of UnsafeRow sizes
(`row.getSizeInBytes`) written to the shuffle, i.e. uncompressed in-memory row
bytes, not the compressed block bytes.** (Not exercised in these experiments:
broadcast is disabled.)

---

## d. Effective AQE parameters in our runs

Defaults from `SQLConf` (line of the `buildConf`) and the value recorded in the
runs (`conf` in every run JSON; from `SET -v`, `SET` and explicit keys, see
`jobs/spark_conf.py`). "set" = set explicitly by `jobs/spark_conf.py` to the
default value; "exp" = changed by an experiment.

| key | SQLConf line | default | value in runs |
|---|---|---|---|
| spark.sql.adaptive.enabled | 951 | true | per mode (exp) |
| spark.sql.adaptive.advisoryPartitionSizeInBytes | 995 | fallback to `spark.sql.adaptive.shuffle.targetPostShuffleInputSize` = 64MB (943) | 67108864 (set) |
| spark.sql.adaptive.coalescePartitions.enabled | 1003 | true | true (set); false in mode `aqe_on_nocoalesce` |
| spark.sql.adaptive.coalescePartitions.parallelismFirst | 1012 | true | true (set) |
| spark.sql.adaptive.coalescePartitions.minPartitionSize | 1026 | 1MB | 1048576 (set) |
| spark.sql.adaptive.coalescePartitions.minPartitionNum (internal) | 1035 | unset | unset |
| spark.sql.adaptive.coalescePartitions.initialPartitionNum | 1048 | unset | unset |
| spark.sql.adaptive.fetchShuffleBlocksInBatch (internal) | 1119 | true | default |
| spark.sql.adaptive.localShuffleReader.enabled | 1133 | true | true (set) |
| spark.sql.adaptive.skewJoin.enabled | 1142 | true | true (set) |
| spark.sql.adaptive.skewJoin.skewedPartitionFactor | 1151 | 5.0 | 5.0 (set) |
| spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes | 1161 | 256MB | 268435456 (set); 67108864 in mode `aqe_on_skew64m` |
| spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin (internal) | 1171 | 0.2 | default |
| spark.sql.adaptive.optimizer.excludedRules | 1183 | unset | unset |
| spark.sql.adaptive.autoBroadcastJoinThreshold | 1192 | unset (falls back to autoBroadcastJoinThreshold) | -1 in all pilot/batch-2 runs (exp) |
| spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold | 1202 | 0 | 0b |
| spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled | 1213 | true | true |
| spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor | 1223 | 0.2 | 0.2 |
| spark.sql.adaptive.forceOptimizeSkewedJoin | 1232 | false | false |
| spark.sql.adaptive.applyFinalStageShuffleOptimizations (internal) | 978 | true | default |
| spark.sql.autoBroadcastJoinThreshold | 861 | 10MB | -1 in all pilot/batch-2 runs (exp) |
| spark.sql.shuffle.partitions | 912 | 200 | 200; varied in experiment B (exp) |
| spark.sql.join.preferSortMergeJoin (internal) | 812 | true | true |
| spark.sql.files.maxPartitionBytes | 2452 | 128MB | 128MB |
| spark.sql.files.openCostInBytes (internal) | 2460 | 4MB | 4MB |

Core settings that determine block sizes (`config`, recorded since batch 2):
`spark.io.compression.codec=lz4` (2093; `zstd` in experiment C),
`spark.io.compression.zstd.level=1` (2131), `spark.io.compression.lz4.blockSize=32k` (2084),
`spark.shuffle.compress=true` (1684), `spark.shuffle.sort.bypassMergeThreshold=200` (1735),
`spark.shuffle.minNumPartitionsToHighlyCompress=2000` (1808),
`spark.shuffle.accurateBlockThreshold=100MiB` (1403),
`spark.shuffle.accurateBlockSkewedFactor=-1.0` (1412; `5.0` in the experiment-B control),
`spark.shuffle.maxAccurateSkewedBlockNumber=100` (1423).

---

## Other facts used in the analysis

**Number of map tasks of a file scan.**
`sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala:118-127`:

```scala
  def maxSplitBytes(sparkSession: SparkSession, calculateTotalBytes: => Long): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)
    val totalBytes = calculateTotalBytes
    val bytesPerCore = totalBytes / minPartitionNum

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }
```

For the 55M-row facts table (~5.7 GB Parquet, 9 cores) this gives 128 MB
splits; the pilot recorded 45 map tasks for the facts scan. By (a), 45 is
also the maximum number of skew splits of the facts side.

**Spill metrics.**
`core/src/main/java/org/apache/spark/util/collection/unsafe/sort/UnsafeExternalSorter.java:232-258`:

```java
    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      MDC.of(LogKeys.THREAD_ID, Thread.currentThread().getId()),
      MDC.of(LogKeys.MEMORY_SIZE, Utils.bytesToString(getMemoryUsage())),
...
    final long spillSize = freeMemory();
    // Note that this is more-or-less going to be a multiple of the page size, so wasted space in
    // pages will currently be counted as memory spilled even though that space isn't actually
    // written to disk. This also counts the space needed to store the sorter's pointer array.
    inMemSorter.freeMemory();
...
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    taskContext.taskMetrics().incDiskBytesSpilled(writeMetrics.bytesWritten());
```

"Memory bytes spilled" is the amount of memory (data pages) released by the
spill, not the size of the data spilled; "disk bytes spilled" is the bytes
written to the spill file. The log line reports `getMemoryUsage()`, which
includes the pointer array, so it is larger than the metric.

---

## Consequences for the batch-2 experiments

- Experiment A: no conflict. Note that a smaller facts table also has fewer
  map tasks (~1.4 GB Parquet / 128 MB splits), which by (a) caps the number
  of skew splits at that map-task count.
- Experiment B: no conflict; the control config exists
  (`spark.shuffle.accurateBlockSkewedFactor`). Two confounds follow from the
  source and cannot be separated in this design: (1) at 200 partitions the
  shuffle uses `BypassMergeSortShuffleWriter`, above 200 `UnsafeShuffleWriter`;
  (2) up to 2000 partitions every block size is rounded up by up to 10 %
  (CompressedMapStatus). For the control the factor is set to 5.0, the value
  the config documentation recommends (equal to skewedPartitionFactor).
- Experiment C: no conflict; skew detection and coalescing use compressed
  bytes (c), so compressibility and codec can change them.
