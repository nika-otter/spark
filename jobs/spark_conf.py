"""Baseline Spark SQL configuration shared by all jobs.

Every value is set explicitly (even where it equals the Spark default) so that
each run records exactly which parameters it used. CBO is disabled and
ANALYZE TABLE is never used: the static planner only sees file-size based
estimates, and AQE only sees runtime statistics.
"""

import json
import os
import platform
import time
import urllib.request

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
    """Return every explicitly set Spark/SQL property of the running session."""
    props = dict(spark.sparkContext.getConf().getAll())
    for key in BASELINE_CONF:
        props[key] = spark.conf.get(key)
    return dict(sorted(props.items()))


def rest_get(spark, path):
    """Query the driver's monitoring REST API (/api/v1/...)."""
    app_id = spark.sparkContext.applicationId
    url = "http://localhost:4040/api/v1/applications/{}{}".format(app_id, path)
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


def last_sql_execution(spark):
    """Latest completed SQL execution, including its final physical plan."""
    executions = rest_get(spark, "/sql?details=true&planDescription=true&length=10000")
    completed = [e for e in executions if e["status"] == "COMPLETED"]
    return max(completed, key=lambda e: e["id"])


def executor_task_summary(spark):
    """Completed tasks per executor host (the driver entry is excluded)."""
    return {
        e["hostPort"]: e["completedTasks"]
        for e in rest_get(spark, "/allexecutors")
        if e["id"] != "driver"
    }


def write_run_record(run_name, record):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.join(RESULTS_DIR, "{}_{}".format(run_name, stamp))
    with open(base + ".json", "w") as f:
        json.dump(record, f, indent=2, sort_keys=True)
    with open(base + "_plan.txt", "w") as f:
        f.write(record["final_physical_plan"])
    return base


def environment_info(spark):
    return {
        "spark_version": spark.version,
        "python_version": platform.python_version(),
        "machine": platform.machine(),
        "application_id": spark.sparkContext.applicationId,
    }
