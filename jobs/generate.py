"""Deterministic, parameterized data generator.

Every column is derived from the row id with xxhash64(seed, salt, id), never
from rand(), so the content does not depend on partitioning or on the number
of tasks. Each dataset lives in

    /opt/spark-data/datasets/<dataset_id>/data/          (Parquet)
    /opt/spark-data/datasets/<dataset_id>/manifest.json  (written last)

where <dataset_id> encodes all generation parameters. A dataset whose manifest
exists is considered complete and is not regenerated.

Tables:
  facts: id, one column per KeySpec (currently only `k`), payload
  dim:   one row per key in [0, n_keys): the key column, filter_bucket, payload

Usage (inside the master container):
  spark-submit generate.py facts --rows N --n-keys N [--hot-share F]
      [--null-share F] [--payload-bytes N] [--seed N]
  spark-submit generate.py dim --n-keys N [--match-rate F] [--payload-bytes N]
      [--seed N]
"""

import argparse
import json
import math
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Tuple

from pyspark.sql import Column, functions as F

from spark_conf import DATA_DIR, build_session

DATASETS_DIR = DATA_DIR + "/datasets"
HOT_KEY = 0
FILTER_BUCKETS = 1000
_U53 = (1 << 53) - 1


def _hash(seed, salt, col):
    return F.xxhash64(F.lit(seed), F.lit(salt), col)


def _uniform(seed, salt, col):
    """Deterministic pseudo-uniform double in [0, 1) derived from `col`."""
    return _hash(seed, salt, col).bitwiseAND(F.lit(_U53)).cast("double") / float(1 << 53)


def _payload(seed, payload_bytes, col):
    """Hex string of exactly payload_bytes characters derived from `col`."""
    if payload_bytes <= 0:
        return F.lit("")
    chunks = [
        F.sha2(F.concat_ws(":", F.lit(seed), F.lit("payload"), F.lit(i), col.cast("string")), 512)
        for i in range(math.ceil(payload_bytes / 128))
    ]
    return F.substring(F.concat(*chunks), 1, payload_bytes)


def _fmt(x):
    return "{:g}".format(x)


@dataclass(frozen=True)
class KeySpec:
    """One join-key column of the facts table."""

    name: str
    n_keys: int
    hot_share: float = 0.0
    null_share: float = 0.0

    def tag(self):
        return "{}-n{}-h{}-null{}".format(
            self.name, self.n_keys, _fmt(self.hot_share), _fmt(self.null_share))

    def column(self, seed, id_col) -> Column:
        # One uniform draw decides NULL / hot / regular, so the three shares
        # are disjoint: [0, null) -> NULL, [null, null+hot) -> HOT_KEY.
        u = _uniform(seed, "share:" + self.name, id_col)
        regular = F.pmod(_hash(seed, "key:" + self.name, id_col), F.lit(self.n_keys))
        return (
            F.when(u < self.null_share, F.lit(None).cast("long"))
            .when(u < self.null_share + self.hot_share, F.lit(HOT_KEY).cast("long"))
            .otherwise(regular)
        )


@dataclass(frozen=True)
class FactsSpec:
    rows: int
    keys: Tuple[KeySpec, ...]
    payload_bytes: int = 100
    seed: int = 42
    table: str = field(default="facts", init=False)

    def dataset_id(self):
        parts = ["facts", "rows-{}".format(self.rows)]
        parts += [k.tag() for k in self.keys]
        parts += ["payload-{}".format(self.payload_bytes), "seed-{}".format(self.seed)]
        return "_".join(parts)

    def build(self, spark):
        id_col = F.col("id")
        cols = [id_col]
        cols += [k.column(self.seed, id_col).alias(k.name) for k in self.keys]
        cols.append(_payload(self.seed, self.payload_bytes, id_col).alias("payload"))
        return spark.range(self.rows).select(*cols)

    def stats(self, df):
        out = {}
        total = df.count()
        out["row_count"] = total
        for k in self.keys:
            nulls = df.where(F.col(k.name).isNull()).count()
            top = (df.where(F.col(k.name).isNotNull())
                   .groupBy(k.name).count()
                   .orderBy(F.desc("count"), F.asc(k.name)).first())
            out[k.name] = {
                "null_count": nulls,
                "actual_null_share": nulls / total,
                "most_frequent_key": top[k.name],
                "most_frequent_key_count": top["count"],
                "actual_most_frequent_key_share": top["count"] / total,
                "distinct_non_null_keys": df.select(k.name).where(F.col(k.name).isNotNull())
                .distinct().count(),
            }
        return out


@dataclass(frozen=True)
class DimSpec:
    """Dimension table for one facts key.

    Holds one row per key in [0, n_keys). A key is "matched" (stored as-is)
    with probability match_rate; unmatched keys are shifted by n_keys, outside
    the facts key range, so the dim size does not depend on match_rate.
    """

    key: str
    n_keys: int
    match_rate: float = 1.0
    payload_bytes: int = 100
    seed: int = 42

    @property
    def table(self):
        return "dim_" + self.key

    def dataset_id(self):
        return "{}_n{}_match{}_payload-{}_seed-{}".format(
            self.table, self.n_keys, _fmt(self.match_rate), self.payload_bytes, self.seed)

    def build(self, spark):
        i = F.col("id")
        matched = _uniform(self.seed, "match:" + self.key, i) < self.match_rate
        return spark.range(self.n_keys).select(
            F.when(matched, i).otherwise(i + self.n_keys).alias(self.key),
            F.pmod(_hash(self.seed, "filter:" + self.key, i), F.lit(FILTER_BUCKETS))
            .cast("int").alias("filter_bucket"),
            _payload(self.seed, self.payload_bytes, i).alias("payload"),
        )

    def stats(self, df):
        total = df.count()
        matched = df.where(F.col(self.key) < self.n_keys).count()
        return {
            "row_count": total,
            "matched_keys": matched,
            "actual_match_rate": matched / total,
            "filter_bucket_count": FILTER_BUCKETS,
        }


def dataset_dir(spec):
    return "{}/{}".format(DATASETS_DIR, spec.dataset_id())


def data_path(spec):
    return dataset_dir(spec) + "/data"


def manifest_path(spec):
    return dataset_dir(spec) + "/manifest.json"


def load_manifest(spec):
    with open(manifest_path(spec)) as f:
        return json.load(f)


def _dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        total += sum(os.path.getsize(os.path.join(root, f)) for f in files)
    return total


def ensure_dataset(spark, spec):
    """Generate the dataset unless its manifest already exists."""
    if os.path.exists(manifest_path(spec)):
        print("exists, skipping: " + spec.dataset_id())
        return load_manifest(spec)

    start = time.perf_counter()
    spec.build(spark).write.mode("overwrite").parquet(data_path(spec))
    write_seconds = time.perf_counter() - start

    df = spark.read.parquet(data_path(spec))
    manifest = {
        "dataset_id": spec.dataset_id(),
        "table": spec.table,
        "path": data_path(spec),
        "params": asdict(spec),
        "stats": spec.stats(df),
        "size_on_disk_bytes": _dir_size(data_path(spec)),
        "parquet_compression": spark.conf.get("spark.sql.parquet.compression.codec"),
        "spark_version": spark.version,
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "write_seconds": round(write_seconds, 1),
    }
    tmp = manifest_path(spec) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(tmp, manifest_path(spec))
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="table", required=True)

    facts = sub.add_parser("facts")
    facts.add_argument("--rows", type=int, required=True)
    facts.add_argument("--n-keys", type=int, required=True)
    facts.add_argument("--hot-share", type=float, default=0.0)
    facts.add_argument("--null-share", type=float, default=0.0)
    facts.add_argument("--payload-bytes", type=int, default=100)
    facts.add_argument("--seed", type=int, default=42)

    dim = sub.add_parser("dim")
    dim.add_argument("--n-keys", type=int, required=True)
    dim.add_argument("--match-rate", type=float, default=1.0)
    dim.add_argument("--payload-bytes", type=int, default=100)
    dim.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()
    if args.table == "facts":
        key = KeySpec("k", args.n_keys, args.hot_share, args.null_share)
        spec = FactsSpec(args.rows, (key,), args.payload_bytes, args.seed)
    else:
        spec = DimSpec("k", args.n_keys, args.match_rate, args.payload_bytes, args.seed)

    spark, _ = build_session("generate-" + spec.dataset_id())
    ensure_dataset(spark, spec)
    spark.stop()


if __name__ == "__main__":
    main()
