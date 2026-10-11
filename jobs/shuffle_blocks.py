"""Per-map-task shuffle blocks of the hottest key (batch 6, step 4a).

For hits at 2001 shuffle partitions and a key (CounterID or RegionID):
  1. runs the batch-5c query (fact LEFT JOIN materialized key table, broadcast
     disabled) once with AQE off, so the fact shuffle is written with plain
     hash partitioning into 2001 partitions;
  2. takes the hottest key value from the dataset manifest and its reduce
     partition pmod(hash(key), 2001);
  3. per map task of the fact shuffle:
       - rows of the hottest key read by that task (spark_partition_id() of the
         same scan = map index),
       - the exact length of the block for that reduce partition, read from
         the shuffle index file on the executor's local disk
         (shuffle_<shuffleId>_<mapId>_0.index: numPartitions + 1 offsets),
       - the size the driver's MapStatus reports for that block
         (getSizeForBlock; what AQE uses), and the MapStatus class;
  4. writes results/step4a_<key>_blocks.csv and a JSON summary.

Usage: spark-submit shuffle_blocks.py <key>
"""

import csv
import json
import os
import struct
import sys

from pyspark.sql import functions as F

from experiments import MODES, build_scenario
from generate import data_path, load_manifest
from spark_conf import RESULTS_DIR, build_session

MIB = 1024 * 1024
LOCAL_DIRS = "/opt/spark-local"
ACCURATE_BLOCK_THRESHOLD = 100 * MIB


def list_blocks(shuffle_id, reduce_id):
    """Runs on executors: (host, mapId, block length) from local shuffle index files."""
    def scan(_):
        host = os.uname()[1]
        prefix = "shuffle_{}_".format(shuffle_id)
        for root, _, files in os.walk(LOCAL_DIRS):
            for name in files:
                if name.startswith(prefix) and name.endswith("_0.index"):
                    map_id = int(name[len(prefix):-len("_0.index")])
                    with open(os.path.join(root, name), "rb") as f:
                        f.seek(8 * reduce_id)
                        start, end = struct.unpack(">qq", f.read(16))
                    yield (host, map_id, end - start)
    return scan


def main():
    key = sys.argv[1]
    scenario = build_scenario("exp_real_kt", ["dataset=hits", "key=" + key, "shuffle_partitions=2001"],
                              "aqe_off")
    conf = dict(scenario.conf)
    conf.update(MODES["aqe_off"])
    spark, _ = build_session("shuffle-blocks-hits-" + key, conf)
    sc = spark.sparkContext
    jvm = sc._jvm
    tables = {name: spark.read.parquet(data_path(spec)) for name, spec in scenario.tables.items()}
    manifest = load_manifest(scenario.tables["facts"])
    hot_value = manifest["keys"][key]["top5"][0]["key"][0]
    facts = tables["facts"]
    hot = facts.where(F.col(key) == F.lit(hot_value).cast(facts.schema[key].dataType))
    reduce_id = hot.select(F.pmod(F.hash(F.col(key)), F.lit(2001)).alias("p")).first()["p"]

    df = scenario.query(tables, "aqe_off")
    df.write.format("noop").mode("overwrite").save()     # keep df referenced: shuffle files stay

    tracker = jvm.org.apache.spark.SparkEnv.get().mapOutputTracker()
    statuses = jvm.scala.collection.JavaConverters.mapAsJavaMap(tracker.shuffleStatuses())
    shuffles = {sid: statuses.get(sid).mapStatuses() for sid in statuses.keySet()}
    fact_sid = max(shuffles, key=lambda s: len(shuffles[s]))
    arr = shuffles[fact_sid]
    reported = {}
    for index in range(len(arr)):
        st = arr[index]
        reported[st.mapId()] = (index, st.getSizeForBlock(reduce_id), st.getClass().getSimpleName(),
                                st.location().host())

    exact = {}
    for host, map_id, length in sc.parallelize(range(90), 90).mapPartitions(
            list_blocks(fact_sid, reduce_id)).collect():
        exact[map_id] = (host, length)

    rows_per_task = {r["t"]: r["n"] for r in facts.select(F.spark_partition_id().alias("t"), F.col(key))
                     .where(F.col(key) == F.lit(hot_value).cast(facts.schema[key].dataType))
                     .groupBy("t").count().withColumnRenamed("count", "n").collect()}

    out_rows = []
    for map_id, (index, rep, cls, host) in sorted(reported.items(), key=lambda kv: kv[1][0]):
        ex = exact.get(map_id)
        out_rows.append({"map_index": index, "map_id": map_id, "host": host,
                         "hot_key_rows": rows_per_task.get(index, 0),
                         "block_bytes_exact": ex[1] if ex else None,
                         "block_bytes_reported": rep, "status_class": cls,
                         "exact_ge_100mib": (ex[1] >= ACCURATE_BLOCK_THRESHOLD) if ex else None})
    path = "{}/step4a_{}_blocks.csv".format(RESULTS_DIR, key)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0]))
        w.writeheader()
        w.writerows(out_rows)
    exact_sizes = [r["block_bytes_exact"] for r in out_rows if r["block_bytes_exact"] is not None]
    summary = {
        "key": key, "hot_value": hot_value, "hot_rows": sum(rows_per_task.values()),
        "reduce_partition": reduce_id, "fact_shuffle_id": fact_sid, "map_tasks": len(out_rows),
        "map_tasks_with_hot_rows": sum(1 for r in out_rows if r["hot_key_rows"] > 0),
        "blocks_found_on_disk": len(exact_sizes),
        "blocks_ge_100mib": sum(1 for s in exact_sizes if s >= ACCURATE_BLOCK_THRESHOLD),
        "block_exact_max_mib": max(exact_sizes) / MIB if exact_sizes else None,
        "block_exact_sum_mib": sum(exact_sizes) / MIB if exact_sizes else None,
        "block_reported_sum_mib": sum(r["block_bytes_reported"] for r in out_rows) / MIB,
        "status_classes": sorted({r["status_class"] for r in out_rows}),
        "hosts": sorted({e[0] for e in exact.values()}),
    }
    with open("{}/step4a_{}_summary.json".format(RESULTS_DIR, key), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
