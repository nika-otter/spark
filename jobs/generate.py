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
      [--null-share F] [--payload-bytes N] [--seed N] [--payload-kind K]
  spark-submit generate.py dim --n-keys N [--match-rate F] [--payload-bytes N]
      [--seed N] [--payload-kind K]

payload kinds: hex (default, string), random (binary, incompressible),
repetitive (binary, highly compressible); see _payload().

Hot keys: KeySpec.hot_share is spread uniformly over hot_keys keys
HOT_KEY .. HOT_KEY + hot_keys - 1 (one hot key by default).
Filter layout of a dim: "hash" (default) draws filter_bucket from a hash of
the key index; "prefix" sets filter_bucket = floor(index * 1000 / n_keys), so
filter_bucket < b keeps exactly the key indices [0, n_keys * b / 1000).
"""

import argparse
import hashlib
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


PAYLOAD_KINDS = ("hex", "random", "repetitive", "mixed")
MIXED_FILLER = 0x41  # constant byte used for the compressible part of "mixed"
MAX_ID_CHARS = 200   # longer dataset ids are shortened with a hash


def _sha512_hex(seed, i, col):
    return F.sha2(F.concat_ws(":", F.lit(seed), F.lit("payload"), F.lit(i), col.cast("string")), 512)


def _payload(seed, payload_bytes, col, kind="hex", random_fraction=1.0):
    """Payload of exactly payload_bytes bytes derived from `col`.

    hex:        string of hex digits from SHA-512 (random content, 4 bits of
                entropy per byte); the original pilot payload
    random:     binary, raw SHA-512 output bytes (8 bits of entropy per byte,
                incompressible)
    repetitive: binary, a 4-byte per-row token repeated to payload_bytes
                (highly compressible)
    mixed:      binary, round(random_fraction * payload_bytes) random bytes
                followed by a constant filler byte; random_fraction in [0, 1]
                sets the compressibility continuously (1 byte granularity)
    """
    if kind not in PAYLOAD_KINDS:
        raise ValueError("unknown payload kind " + kind)
    if payload_bytes <= 0:
        return F.lit("") if kind == "hex" else F.lit(b"")
    if kind == "hex":
        chunks = [_sha512_hex(seed, i, col) for i in range(math.ceil(payload_bytes / 128))]
        return F.substring(F.concat(*chunks), 1, payload_bytes)
    if kind == "random":
        chunks = [F.unhex(_sha512_hex(seed, i, col)) for i in range(math.ceil(payload_bytes / 64))]
        return F.substring(F.concat(*chunks), 1, payload_bytes)
    if kind == "mixed":
        n_random = int(round(random_fraction * payload_bytes))
        filler = F.lit(bytes([MIXED_FILLER]) * (payload_bytes - n_random))
        if n_random == 0:
            return filler
        chunks = [F.unhex(_sha512_hex(seed, i, col)) for i in range(math.ceil(n_random / 64))]
        return F.concat(F.substring(F.concat(*chunks), 1, n_random), filler)
    token = F.substring(_sha512_hex(seed, 0, col), 1, 4)
    return F.substring(F.repeat(token, math.ceil(payload_bytes / 4)), 1, payload_bytes).cast("binary")


def _kind_tag(kind, random_fraction=1.0):
    if kind == "hex":
        return ""
    if kind == "mixed":
        return "_kind-mixed{:g}".format(random_fraction)
    return "_kind-" + kind


def _shorten(dataset_id, spec):
    """Keep ids below the file-name limit: long ids get a hash of the full spec."""
    if len(dataset_id) <= MAX_ID_CHARS:
        return dataset_id
    digest = hashlib.sha1(json.dumps(asdict(spec), sort_keys=True).encode()).hexdigest()[:12]
    return dataset_id[:MAX_ID_CHARS - 14] + "_h" + digest


def _fmt(x):
    return "{:g}".format(x)


@dataclass(frozen=True)
class KeySpec:
    """One join-key column of the facts table."""

    name: str
    n_keys: int
    hot_share: float = 0.0
    null_share: float = 0.0
    hot_keys: int = 1          # hot_share is spread uniformly over this many keys

    def tag(self):
        tag = "{}-n{}-h{}-null{}".format(
            self.name, self.n_keys, _fmt(self.hot_share), _fmt(self.null_share))
        return tag + ("-hk{}".format(self.hot_keys) if self.hot_keys > 1 else "")

    def column(self, seed, id_col) -> Column:
        # One uniform draw decides NULL / hot / regular, so the three shares
        # are disjoint: [0, null) -> NULL, [null, null+hot) -> HOT_KEY.
        u = _uniform(seed, "share:" + self.name, id_col)
        regular = F.pmod(_hash(seed, "key:" + self.name, id_col), F.lit(self.n_keys))
        hot = F.lit(HOT_KEY).cast("long")
        if self.hot_keys > 1:
            hot = hot + F.pmod(_hash(seed, "hot:" + self.name, id_col), F.lit(self.hot_keys))
        return (
            F.when(u < self.null_share, F.lit(None).cast("long"))
            .when(u < self.null_share + self.hot_share, hot)
            .otherwise(regular)
        )


@dataclass(frozen=True)
class FactsSpec:
    rows: int
    keys: Tuple[KeySpec, ...]
    payload_bytes: int = 100
    seed: int = 42
    payload_kind: str = "hex"
    payload_random_fraction: float = 1.0   # used by payload_kind "mixed"
    num_files: int = 0                     # > 0: write exactly this many files
    table: str = field(default="facts", init=False)

    def dataset_id(self):
        parts = ["facts", "rows-{}".format(self.rows)]
        parts += [k.tag() for k in self.keys]
        parts += ["payload-{}".format(self.payload_bytes), "seed-{}".format(self.seed)]
        dataset_id = "_".join(parts) + _kind_tag(self.payload_kind, self.payload_random_fraction)
        if self.num_files > 0:
            dataset_id += "_files-{}".format(self.num_files)
        return _shorten(dataset_id, self)

    def build(self, spark):
        id_col = F.col("id")
        cols = [id_col]
        cols += [k.column(self.seed, id_col).alias(k.name) for k in self.keys]
        cols.append(_payload(self.seed, self.payload_bytes, id_col, self.payload_kind,
                             self.payload_random_fraction).alias("payload"))
        # One range partition per output file: each task writes one file.
        if self.num_files > 0:
            return spark.range(0, self.rows, 1, self.num_files).select(*cols)
        return spark.range(self.rows).select(*cols)

    def stats(self, df):
        out = {}
        total = df.count()
        out["row_count"] = total
        for k in self.keys:
            nulls = df.where(F.col(k.name).isNull()).count()
            counts = (df.where(F.col(k.name).isNotNull())
                      .groupBy(k.name).count()
                      .orderBy(F.desc("count"), F.asc(k.name)).limit(max(k.hot_keys, 1)).collect())
            top = counts[0]
            out[k.name] = {
                "null_count": nulls,
                "actual_null_share": nulls / total,
                "most_frequent_key": top[k.name],
                "most_frequent_key_count": top["count"],
                "actual_most_frequent_key_share": top["count"] / total,
                "distinct_non_null_keys": df.select(k.name).where(F.col(k.name).isNotNull())
                .distinct().count(),
            }
            if k.hot_keys > 1:
                out[k.name]["top_keys"] = [[r[k.name], r["count"]] for r in counts]
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
    payload_kind: str = "hex"
    payload_random_fraction: float = 1.0   # used by payload_kind "mixed"
    filter_layout: str = "hash"            # "hash" or "prefix", see module docstring

    @property
    def table(self):
        return "dim_" + self.key

    def dataset_id(self):
        dataset_id = "{}_n{}_match{}_payload-{}_seed-{}".format(
            self.table, self.n_keys, _fmt(self.match_rate), self.payload_bytes,
            self.seed) + _kind_tag(self.payload_kind, self.payload_random_fraction)
        if self.filter_layout != "hash":
            dataset_id += "_filter-" + self.filter_layout
        return dataset_id

    def build(self, spark):
        i = F.col("id")
        matched = _uniform(self.seed, "match:" + self.key, i) < self.match_rate
        if self.filter_layout == "prefix":
            bucket = F.floor(i * FILTER_BUCKETS / self.n_keys)
        elif self.filter_layout == "hash":
            bucket = F.pmod(_hash(self.seed, "filter:" + self.key, i), F.lit(FILTER_BUCKETS))
        else:
            raise ValueError("unknown filter layout " + self.filter_layout)
        return spark.range(self.n_keys).select(
            F.when(matched, i).otherwise(i + self.n_keys).alias(self.key),
            bucket.cast("int").alias("filter_bucket"),
            _payload(self.seed, self.payload_bytes, i, self.payload_kind,
                     self.payload_random_fraction).alias("payload"),
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


@dataclass(frozen=True)
class RealSpec:
    """A real dataset loaded by jobs/prepare_real.py (batch 5, step 1c)."""

    name: str
    table: str = field(default="facts", init=False)

    def dataset_id(self):
        return "real_" + self.name

    def build(self, spark):
        raise RuntimeError("real datasets are loaded with jobs/prepare_real.py " + self.name)


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
    facts.add_argument("--payload-kind", choices=PAYLOAD_KINDS, default="hex")

    dim = sub.add_parser("dim")
    dim.add_argument("--n-keys", type=int, required=True)
    dim.add_argument("--match-rate", type=float, default=1.0)
    dim.add_argument("--payload-bytes", type=int, default=100)
    dim.add_argument("--seed", type=int, default=42)
    dim.add_argument("--payload-kind", choices=PAYLOAD_KINDS, default="hex")

    args = parser.parse_args()
    if args.table == "facts":
        key = KeySpec("k", args.n_keys, args.hot_share, args.null_share)
        spec = FactsSpec(args.rows, (key,), args.payload_bytes, args.seed, args.payload_kind)
    else:
        spec = DimSpec("k", args.n_keys, args.match_rate, args.payload_bytes, args.seed,
                       args.payload_kind)

    spark, _ = build_session("generate-" + spec.dataset_id())
    ensure_dataset(spark, spec)
    spark.stop()


if __name__ == "__main__":
    main()
