"""Baseline Spark SQL configuration shared by all jobs.

Every value is set explicitly (all equal to the Spark 4.1.3 defaults) so that
each run records exactly which parameters it used. CBO is disabled and
ANALYZE TABLE is never used: the static planner only sees file-size based
estimates, and AQE only sees runtime statistics.
"""

import platform

from pyspark.sql import SparkSession

DATA_DIR = "/opt/spark-data"
RESULTS_DIR = "/opt/project/results"

BASELINE_CONF = {
    # Static planning
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    "spark.sql.cbo.enabled": "false",
    "spark.sql.cbo.joinReorder.enabled": "false",
    # Adaptive Query Execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst": "true",
    "spark.sql.adaptive.coalescePartitions.minPartitionSize": str(1024 * 1024),
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(64 * 1024 * 1024),
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5.0",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(256 * 1024 * 1024),
}

# Internal (non-public) confs that `SET -v` does not list but that affect
# join planning; their resolved values are recorded explicitly.
EXTRA_RECORDED_KEYS = [
    "spark.sql.join.preferSortMergeJoin",
    "spark.sql.adaptive.forceOptimizeSkewedJoin",
    "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold",
    "spark.sql.adaptive.coalescePartitions.initialPartitionNum",
    "spark.sql.adaptive.optimizer.excludedRules",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.files.openCostInBytes",
]

# Core (SparkConf) settings that determine shuffle block sizes as seen by AQE.
# Values are the Spark 4.1.3 defaults (see docs/source_notes.md); the
# effective value is recorded, falling back to the default when unset.
CORE_DEFAULTS = {
    "spark.io.compression.codec": "lz4",
    "spark.io.compression.zstd.level": "1",
    "spark.io.compression.lz4.blockSize": "32k",
    "spark.shuffle.compress": "true",
    "spark.shuffle.sort.bypassMergeThreshold": "200",
    "spark.shuffle.minNumPartitionsToHighlyCompress": "2000",
    "spark.shuffle.accurateBlockThreshold": str(100 * 1024 * 1024),
    "spark.shuffle.accurateBlockSkewedFactor": "-1.0",
    "spark.shuffle.maxAccurateSkewedBlockNumber": "100",
}


def build_session(app_name, overrides=None):
    """Create a SparkSession with the baseline config plus per-run overrides."""
    conf = dict(BASELINE_CONF)
    conf.update(overrides or {})
    builder = SparkSession.builder.appName(app_name)
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    return spark, conf


def effective_conf(spark):
    """Full effective configuration of the running session.

    Contains the SparkConf (cluster/application settings), every public SQL
    conf with its resolved value (`SET -v`, defaults included), every SQL conf
    set in the session, and the internal keys from EXTRA_RECORDED_KEYS.
    """
    props = dict(spark.sparkContext.getConf().getAll())
    props.update({r.key: r.value for r in spark.sql("SET -v").collect()})
    props.update({r.key: r.value for r in spark.sql("SET").collect()})
    for key in EXTRA_RECORDED_KEYS:
        try:
            props[key] = spark.conf.get(key)
        except Exception:  # key unknown to this Spark version
            props[key] = None
    return dict(sorted(props.items()))


def environment_info(spark):
    return {
        "spark_version": spark.version,
        "python_version": platform.python_version(),
        "machine": platform.machine(),
        "application_id": spark.sparkContext.applicationId,
    }
