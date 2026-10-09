"""Materialize per-key tables and compute shuffle partition sizes (batch 5c).

For a facts dataset and a key column:
  1. writes the per-key table (key, key_rows, one aggregate; experiments.
     key_table_aggs) to /opt/spark-data/datasets/<facts_id>_keys_<key>/data,
     in a job of its own, so the measured query reads it with a plain scan;
  2. computes, for 200 and 2001 shuffle partitions, the fact-side partition of
     every row exactly as Spark's HashPartitioning does
     (pmod(murmur3(key, seed 42), n) = pmod(hash(key), n)) and the UnsafeRow
     size of every fact row (8 x null-bitset words + 8 x columns + each
     variable-length value rounded up to 8 bytes), and records per n: rows and
     bytes of the largest partition, the median partition size and the total.
     These are uncompressed shuffle "data size" bytes of the fact side; the
     total can be checked against the Exchange "data size" metric.
The manifest goes to <...>_keys_<key>/manifest.json.

Usage: spark-submit prepare_keytables.py real <taxi|hits|pageviews> <key>
       spark-submit prepare_keytables.py synthetic <experiment> --param k=v ...
"""

import json
import os
import sys
import time

from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType

from experiments import build_scenario, key_table_aggs, key_table_id
from generate import RealSpec, data_path, dataset_dir
from spark_conf import build_session


def unsafe_row_size(schema):
    n = len(schema.fields)
    size = F.lit(8 * ((n + 63) // 64) + 8 * n)
    for f in schema.fields:
        if isinstance(f.dataType, StringType):
            var = F.octet_length(F.col(f.name))
        elif isinstance(f.dataType, BinaryType):
            var = F.length(F.col(f.name))
        else:
            continue
        size = size + F.coalesce(F.ceil(var / 8) * 8, F.lit(0))
    return size


def partition_stats(facts, key, n):
    per = (facts.select(F.pmod(F.hash(F.col(key)), F.lit(n)).alias("pid"),
                        unsafe_row_size(facts.schema).alias("size"))
           .groupBy("pid").agg(F.count(F.lit(1)).alias("rows"), F.sum("size").alias("bytes")))
    rows = per.collect()
    sizes = sorted(r["bytes"] for r in rows) + [0] * (n - len(rows))
    sizes.sort()
    top = max(rows, key=lambda r: r["bytes"])
    median = (sizes[n // 2] + sizes[(n - 1) // 2]) / 2
    return {"partitions": n, "non_empty": len(rows), "largest_pid": top["pid"],
            "largest_rows": top["rows"], "largest_bytes": top["bytes"],
            "median_bytes": median, "total_bytes": sum(r["bytes"] for r in rows)}


def main():
    kind = sys.argv[1]
    if kind == "real":
        name, key = sys.argv[2], sys.argv[3]
        facts_spec, agg_kind = RealSpec(name), name
    else:
        pairs = [a for a in sys.argv[3:] if a != "--param"]
        scenario = build_scenario(sys.argv[2], pairs)
        facts_spec, key, agg_kind = scenario.tables["facts"], "k", "synthetic"
    spark, _ = build_session("prepare-keytable-{}-{}".format(facts_spec.dataset_id()[:40], key))
    facts = spark.read.parquet(data_path(facts_spec))
    out_id = key_table_id(facts_spec.dataset_id(), key)
    target = os.path.join(os.path.dirname(dataset_dir(facts_spec)), out_id)
    start = time.time()
    keys = facts.groupBy(key).agg(*key_table_aggs(agg_kind))
    keys.write.mode("overwrite").parquet(target + "/data")
    written = spark.read.parquet(target + "/data")
    manifest = {
        "dataset_id": out_id, "table": "keys", "path": target + "/data",
        "facts_dataset_id": facts_spec.dataset_id(), "key": key,
        "row_count": written.count(), "schema": written.schema.simpleString(),
        "fact_rows": facts.count(),
        "fact_partitions": {str(n): partition_stats(facts, key, n) for n in (200, 2001)},
        "spark_version": spark.version, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    manifest["seconds"] = round(time.time() - start, 1)
    with open(target + "/manifest.json.tmp", "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(target + "/manifest.json.tmp", target + "/manifest.json")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
