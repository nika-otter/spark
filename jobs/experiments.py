"""Experiment definitions: datasets, query and conf overrides per scenario.

A scenario is built from an experiment name plus string parameters (from the
command line), so the runner and the batch scripts share one definition.

null_skew experiments: facts JOIN dim ON k with broadcast joins disabled
(static and adaptive threshold), so the join is always sort-merge.
pipeline experiments: facts joined sequentially to n dims on k1..kn with the
broadcast thresholds at their defaults.
"""

import json
from dataclasses import dataclass
from typing import Callable, Dict

from pyspark.sql import Window, functions as F

from generate import FILTER_BUCKETS, DimSpec, FactsSpec, KeySpec, KeyTableSpec, RealSpec, \
    manifest_path

MIB = 1024 * 1024

# Mode -> conf. Rule-specific values (rule 1: accurate_skew_factor, rule 2:
# rule2_ratio) come from scenario parameters; the batch scripts pass them
# together with the matching mode.
MODES = {
    "aqe_off": {"spark.sql.adaptive.enabled": "false"},
    "aqe_on": {"spark.sql.adaptive.enabled": "true"},
    "aqe_on_nocoalesce": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
    },
    "aqe_on_skew64m": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(64 * MIB),
    },
    "aqe_on_rule1": {"spark.sql.adaptive.enabled": "true"},
    "aqe_on_rule2": {"spark.sql.adaptive.enabled": "true"},
    "aqe_on_rules12": {"spark.sql.adaptive.enabled": "true"},
    "aqe_on_hints": {"spark.sql.adaptive.enabled": "true"},
    # Batch 5
    "aqe_on_fixed8": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(8 * MIB),
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(32 * MIB),
    },
    "aqe_on_fixed16": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(16 * MIB),
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(64 * MIB),
    },
    # Batch 5c
    "aqe_on_force": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
    },
    # rule 2 recomputed between runs from the previous run's r (run_experiment --iterate-rule2)
    "aqe_on_rules12_iter": {"spark.sql.adaptive.enabled": "true"},
}

# Parameters a mode requires (checked in build_scenario).
MODE_REQUIRES = {
    "aqe_on_rule1": {"accurate_skew_factor"},
    "aqe_on_rule2": {"rule2_ratio"},
    "aqe_on_rules12": {"accurate_skew_factor", "rule2_ratio"},
    "aqe_on_rules12_iter": {"accurate_skew_factor", "rule2_ratio"},
}

# Rows per file split mechanism: with both values far above any facts file,
# FilePartition packs exactly one file per scan task, so a facts table
# written as N files is read by N map tasks (see docs/source_notes.md).
ONE_FILE_PER_TASK_BYTES = str(1024 * MIB)


@dataclass
class Scenario:
    experiment: str
    scenario_id: str
    params: Dict[str, object]
    tables: Dict[str, object]           # table name -> FactsSpec / DimSpec
    conf: Dict[str, str]                # overrides on top of BASELINE_CONF
    query: Callable                     # (dict of DataFrames, mode) -> DataFrame


DEFAULTS = {
    "join": "left",
    "null_share": 0.0,                  # NULL share of the join key (pipeline: key null_key)
    "hot_share": 0.0,                   # null_skew only: share of rows on hot keys
    "hot_keys": 1,                      # null_skew only: hot_share spread over this many keys
    "rows": 55_000_000,
    "payload_bytes": 100,
    "payload_kind": "hex",
    "payload_random_fraction": 1.0,     # for payload_kind "mixed"
    "num_files": 0,                     # > 0: facts written as N files, read by N tasks
    "n_keys": 1_000_000,
    "seed": 42,
    "shuffle_partitions": 200,
    "codec": "lz4",                     # spark.io.compression.codec (Spark default)
    "accurate_skew_factor": -1.0,       # rule 1: spark.shuffle.accurateBlockSkewedFactor
    "rule2_ratio": 0.0,                 # rule 2: measured compression ratio r (0 = off)
    "rule2_factor": 1.0,                # label only: rule2_ratio = factor x measured r
    "facts_sample": 0.0,                # null_skew only: > 0 keeps this share of facts rows
    # pipeline experiments only
    "n_dims": 0,
    "dim_payload_bytes": 20,
    "match_rate": 1.0,
    "filter_buckets_kept": 10,          # of 1000 -> ~1 % of a filtered dim
    "null_key": 1,                      # index i of the key k<i> that gets null_share
    "dataset": "",                      # real_query: taxi | hits | pageviews
    "query": "",                        # real_query: natural | lowcard
    "key": "",                          # real_keytable: join key column
    "variant": "",                      # hot_keytable: A (key table from Parquet) | B (inline)
    "operator": "",                     # operator_query: window | agg
    "executor_memory": "",              # spark.executor.memory (core conf), "" = cluster default
    "min_highly_compress": 0,           # > 0: spark.shuffle.minNumPartitionsToHighlyCompress
    "filter_aligned": 0,                # 1: facts keys of filtered dims reference only
                                        #    dim rows that pass the filter
}


def _common_conf(p, broadcast_disabled):
    conf = {
        "spark.sql.shuffle.partitions": str(p["shuffle_partitions"]),
        # Core confs: must be set before the SparkContext starts (the runner
        # creates the session with them).
        "spark.io.compression.codec": p["codec"],
        "spark.shuffle.accurateBlockSkewedFactor": str(p["accurate_skew_factor"]),
    }
    if broadcast_disabled:
        conf["spark.sql.autoBroadcastJoinThreshold"] = "-1"
        conf["spark.sql.adaptive.autoBroadcastJoinThreshold"] = "-1"
    if p["rule2_ratio"] > 0:
        r = p["rule2_ratio"]
        conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"] = str(int(64 * MIB / r))
        conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"] = str(int(256 * MIB / r))
    if p["executor_memory"]:
        conf["spark.executor.memory"] = p["executor_memory"]
    if p["min_highly_compress"] > 0:
        conf["spark.shuffle.minNumPartitionsToHighlyCompress"] = str(p["min_highly_compress"])
    if p["num_files"] > 0:
        conf["spark.sql.files.maxPartitionBytes"] = ONE_FILE_PER_TASK_BYTES
        conf["spark.sql.files.openCostInBytes"] = ONE_FILE_PER_TASK_BYTES
    return conf


def _suffix(p):
    s = ""
    if p["payload_kind"] == "mixed":
        s += "_mixed{:g}".format(p["payload_random_fraction"])
    elif p["payload_kind"] != "hex":
        s += "_" + p["payload_kind"]
    if p["num_files"] > 0:
        s += "_files{}".format(p["num_files"])
    if p["shuffle_partitions"] != 200:
        s += "_sp{}".format(p["shuffle_partitions"])
    if p["codec"] != "lz4":
        s += "_" + p["codec"]
    if p["accurate_skew_factor"] > 0:
        s += "_accskew{:g}".format(p["accurate_skew_factor"])
    if p["rule2_ratio"] > 0:
        s += "_rule2r{:.3g}".format(p["rule2_ratio"])
    if p["rule2_factor"] != 1.0:
        s += "_f{:g}".format(p["rule2_factor"])
    if p["facts_sample"] > 0:
        s += "_sample{:g}".format(p["facts_sample"])
    if p["executor_memory"]:
        s += "_heap" + p["executor_memory"]
    if p["min_highly_compress"] > 0:
        s += "_minhc{}".format(p["min_highly_compress"])
    return s


def null_skew(experiment, p):
    if p["join"] not in ("left", "inner"):
        raise ValueError("join must be left or inner")
    payload = dict(payload_bytes=p["payload_bytes"], seed=p["seed"],
                   payload_kind=p["payload_kind"],
                   payload_random_fraction=p["payload_random_fraction"])
    facts = FactsSpec(
        rows=p["rows"],
        keys=(KeySpec("k", p["n_keys"], hot_share=p["hot_share"], null_share=p["null_share"],
                      hot_keys=p["hot_keys"]),),
        num_files=p["num_files"], **payload)
    dim = DimSpec("k", p["n_keys"], match_rate=1.0, **payload)

    def query(t, mode):
        facts = t["facts"]
        if p["facts_sample"] > 0:
            # deterministic sample of facts rows (by id), independent of partitioning
            bucket = F.pmod(F.xxhash64(F.lit(p["seed"]), F.lit("sample"), F.col("id")), F.lit(10000))
            facts = facts.where(bucket < int(round(p["facts_sample"] * 10000)))
        return facts.join(t["dim_k"], on="k", how=p["join"])

    scenario_id = "{}_null{:g}_rows{}_payload{}".format(
        p["join"], p["null_share"], p["rows"], p["payload_bytes"])
    if p["hot_share"] > 0:
        scenario_id += "_hot{:g}".format(p["hot_share"])
        if p["hot_keys"] > 1:
            scenario_id += "x{}".format(p["hot_keys"])
    scenario_id += _suffix(p)
    return Scenario(experiment, scenario_id, p, {"facts": facts, "dim_k": dim},
                    _common_conf(p, broadcast_disabled=True), query)


def is_filtered_dim(i):
    """Dims 1, 3, 5, ... are filtered to ~1 %; dims 2, 4, ... are not."""
    return i % 2 == 1


def pipeline(experiment, p):
    n = p["n_dims"]
    if n < 1 or p["join"] not in ("left", "inner"):
        raise ValueError("pipeline needs n_dims >= 1 and join left/inner")
    aligned = p["filter_aligned"] == 1
    # Aligned layout: a filtered dim keeps exactly the key indices
    # [0, n_filtered) (filter layout "prefix"), and the facts key of that dim
    # is drawn from this range only; match_rate still applies on top.
    n_filtered = p["n_keys"] * p["filter_buckets_kept"] // FILTER_BUCKETS

    def key_range(i):
        return n_filtered if aligned and is_filtered_dim(i) else p["n_keys"]

    keys = tuple(KeySpec("k{}".format(i), key_range(i),
                         null_share=p["null_share"] if i == p["null_key"] else 0.0)
                 for i in range(1, n + 1))
    facts = FactsSpec(rows=p["rows"], keys=keys, payload_bytes=p["payload_bytes"],
                      seed=p["seed"], payload_kind=p["payload_kind"],
                      payload_random_fraction=p["payload_random_fraction"],
                      num_files=p["num_files"])
    dims = {"dim_k{}".format(i): DimSpec(
                "k{}".format(i), p["n_keys"], match_rate=p["match_rate"],
                payload_bytes=p["dim_payload_bytes"], seed=p["seed"],
                filter_layout="prefix" if aligned and is_filtered_dim(i) else "hash")
            for i in range(1, n + 1)}

    def query(t, mode):
        df = t["facts"]
        for i in range(1, n + 1):
            k = "k{}".format(i)
            d = t["dim_" + k]
            if is_filtered_dim(i):
                d = d.where(F.col("filter_bucket") < p["filter_buckets_kept"])
            d = d.select(F.col(k), F.col("payload").alias("dim{}_payload".format(i)))
            if mode == "aqe_on_hints" and is_filtered_dim(i):
                d = F.broadcast(d)
            df = df.join(d, on=k, how=p["join"])
        return df

    scenario_id = "{}_n{}_null{:g}_rows{}_payload{}".format(
        p["join"], n, p["null_share"], p["rows"], p["payload_bytes"])
    if p["null_key"] != 1:
        scenario_id += "_nullk{}".format(p["null_key"])
    if aligned:
        scenario_id += "_aligned_match{:g}".format(p["match_rate"])
    scenario_id += _suffix(p)
    tables = {"facts": facts}
    tables.update(dims)
    return Scenario(experiment, scenario_id, p, tables,
                    _common_conf(p, broadcast_disabled=False), query)


# real_query: natural (high-cardinality) and low-cardinality keys per dataset;
# for hits the low-cardinality key is CounterID or RegionID, whichever has the
# larger share of its most frequent value in the manifest (prepare_real.py).
REAL_KEYS = {
    "taxi": (["PULocationID", "pickup_hour"], ["PULocationID"]),
    "hits": (["UserID"], None),
    "pageviews": (["project", "page_title"], ["project"]),
}
REAL_AGG = {   # (column, function) of the per-key table besides the row count
    "taxi": ("total_amount", F.sum),
    "hits": ("EventTime", F.max),
    "pageviews": ("views", F.sum),
}


def real_lowcard_key(name, manifest):
    if name != "hits":
        return REAL_KEYS[name][1]
    keys = manifest["keys"]
    return ["CounterID"] if keys["CounterID"]["top1_share"] >= keys["RegionID"]["top1_share"] \
        else ["RegionID"]


def real_query(experiment, p):
    """Fact table LEFT JOIN a per-key table derived from the same data.

    natural: high-cardinality key, default broadcast thresholds.
    lowcard: low-cardinality, naturally skewed key, broadcast disabled to
    emulate a large dimension.
    """
    name, kind = p["dataset"], p["query"]
    if name not in REAL_KEYS or kind not in ("natural", "lowcard"):
        raise ValueError("dataset must be taxi|hits|pageviews and query natural|lowcard")
    spec = RealSpec(name)
    with open(manifest_path(spec)) as f:
        manifest = json.load(f)
    key = REAL_KEYS[name][0] if kind == "natural" else real_lowcard_key(name, manifest)
    agg_col, agg_fn = REAL_AGG[name]

    def query(t, mode):
        facts = t["facts"]
        if name == "taxi":
            facts = facts.withColumn("pickup_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
        per_key = facts.groupBy(*key).agg(F.count(F.lit(1)).alias("key_rows"),
                                          agg_fn(agg_col).alias("key_" + agg_col))
        return facts.join(per_key, on=key, how="left")

    params = dict(p, join_key="+".join(key))
    scenario_id = "{}_{}_{}".format(name, kind, "+".join(key)) + _suffix(p)
    return Scenario(experiment, scenario_id, params, {"facts": spec},
                    _common_conf(p, broadcast_disabled=(kind == "lowcard")), query)


def key_table_id(facts_id, key):
    return "{}_keys_{}".format(facts_id, key)


def real_keytable(experiment, p):
    """Batch 5c step 2: fact LEFT JOIN a per-key table materialized as Parquet
    (jobs/prepare_keytables.py), so that both join inputs are plain scans;
    broadcast disabled."""
    name, key = p["dataset"], p["key"]
    facts = RealSpec(name)
    keys = KeyTableSpec(key_table_id(facts.dataset_id(), key))

    def query(t, mode):
        return t["facts"].join(t["keys"], on=key, how="left")

    scenario_id = "{}_keytable_{}".format(name, key) + _suffix(p)
    return Scenario(experiment, scenario_id, dict(p, join_key=key), {"facts": facts, "keys": keys},
                    _common_conf(p, broadcast_disabled=True), query)


def hot_keytable(experiment, p):
    """Batch 5c step 3: synthetic facts (hot key) LEFT JOIN a per-key table,
    variant A: read from Parquet; variant B: the same GROUP BY computed inline."""
    if p["variant"] not in ("A", "B"):
        raise ValueError("variant must be A or B")
    payload = dict(payload_bytes=p["payload_bytes"], seed=p["seed"], payload_kind=p["payload_kind"],
                   payload_random_fraction=p["payload_random_fraction"])
    facts = FactsSpec(rows=p["rows"],
                      keys=(KeySpec("k", p["n_keys"], hot_share=p["hot_share"],
                                    null_share=p["null_share"], hot_keys=p["hot_keys"]),),
                      num_files=p["num_files"], **payload)
    tables = {"facts": facts}
    if p["variant"] == "A":
        tables["keys"] = KeyTableSpec(key_table_id(facts.dataset_id(), "k"))

    def query(t, mode):
        if p["variant"] == "A":
            keys = t["keys"]
        else:
            keys = t["facts"].groupBy("k").agg(*key_table_aggs("synthetic"))
        return t["facts"].join(keys, on="k", how="left")

    scenario_id = "variant{}_left_hot{:g}_rows{}".format(p["variant"], p["hot_share"], p["rows"]) + _suffix(p)
    return Scenario(experiment, scenario_id, p, tables, _common_conf(p, broadcast_disabled=True), query)


def operator_query(experiment, p):
    """Batch 6 step 1: a single shuffle-based operator on the facts table.

    window: row_number() OVER (PARTITION BY k ORDER BY id), all columns kept.
    agg:    GROUP BY id (unique per row), so partial aggregation before the
            shuffle cannot reduce the row count; the payload is carried
            through max(payload).
    """
    if p["operator"] not in ("window", "agg"):
        raise ValueError("operator must be window or agg")
    payload = dict(payload_bytes=p["payload_bytes"], seed=p["seed"], payload_kind=p["payload_kind"],
                   payload_random_fraction=p["payload_random_fraction"])
    facts = FactsSpec(rows=p["rows"],
                      keys=(KeySpec("k", p["n_keys"], hot_share=p["hot_share"],
                                    null_share=p["null_share"], hot_keys=p["hot_keys"]),),
                      num_files=p["num_files"], **payload)

    def query(t, mode):
        f = t["facts"]
        if p["operator"] == "window":
            return f.withColumn("rn", F.row_number().over(Window.partitionBy("k").orderBy("id")))
        return f.groupBy("id").agg(F.max("k").alias("k"), F.max("payload").alias("payload"),
                                   F.count(F.lit(1)).alias("n"))

    scenario_id = "{}_null{:g}_rows{}_payload{}".format(
        p["operator"], p["null_share"], p["rows"], p["payload_bytes"])
    if p["hot_share"] > 0:
        scenario_id += "_hot{:g}".format(p["hot_share"])
    scenario_id += _suffix(p)
    return Scenario(experiment, scenario_id, p, {"facts": facts},
                    _common_conf(p, broadcast_disabled=True), query)


# Aggregates of the per-key tables (batch 5c); also used by prepare_keytables.py.
KEY_TABLE_AGG_COLUMNS = {"taxi": ("total_amount", "sum"), "hits": ("EventTime", "max"),
                         "pageviews": ("views", "sum"), "synthetic": ("id", "max")}


def key_table_aggs(kind):
    col, fn = KEY_TABLE_AGG_COLUMNS[kind]
    return [F.count(F.lit(1)).alias("key_rows"), getattr(F, fn)(col).alias("key_" + col)]


# Experiment name -> (builder, defaults that differ from DEFAULTS).
EXPERIMENTS = {
    "pilot_null": (null_skew, {}),
    "inner_check": (null_skew, {"join": "inner"}),
    "exp_a_small": (null_skew, {"rows": 13_700_000}),
    "exp_b_partitions": (null_skew, {"null_share": 0.2}),
    "exp_c_compression": (null_skew, {"null_share": 0.08}),
    "exp_c2_dose": (null_skew, {"null_share": 0.08, "payload_kind": "mixed", "num_files": 45}),
    "exp_rules": (null_skew, {"null_share": 0.2, "payload_kind": "mixed", "num_files": 45,
                              "shuffle_partitions": 2001}),
    "exp_d_pipeline": (pipeline, {"match_rate": 0.7}),
    # Batch 4
    "exp_d2_inner": (pipeline, {"join": "inner", "filter_aligned": 1, "match_rate": 0.966}),
    "exp_d2_stress": (pipeline, {"join": "left", "n_dims": 5, "match_rate": 0.7,
                                 "null_share": 0.2, "null_key": 2, "payload_kind": "mixed",
                                 "shuffle_partitions": 2001}),
    "exp_e_volume": (null_skew, {"null_share": 0.2, "payload_kind": "mixed"}),
    "exp_e_coalesce": (null_skew, {"null_share": 0.0, "payload_kind": "mixed"}),
    "exp_f_hotkey": (null_skew, {"null_share": 0.0, "hot_share": 0.2, "payload_kind": "mixed"}),
    # Batch 5
    "exp_g_alt": (null_skew, {"payload_kind": "mixed", "rows": 55_000_000, "num_files": 44}),
    "exp_h_stress": (pipeline, {"join": "left", "n_dims": 5, "match_rate": 0.7,
                                "null_share": 0.2, "null_key": 2, "payload_kind": "mixed",
                                "payload_bytes": 400, "payload_random_fraction": 0.04,
                                "shuffle_partitions": 2001}),
    "exp_h_sens": (null_skew, {"payload_kind": "mixed", "rows": 55_000_000, "num_files": 44,
                               "null_share": 0.2, "payload_random_fraction": 0.04,
                               "shuffle_partitions": 2001}),
    "exp_h_sample": (null_skew, {"null_share": 0.08, "payload_kind": "mixed", "num_files": 45}),
    "exp_real": (real_query, {}),
    "exp_real_kt": (real_keytable, {}),
    "exp_op": (operator_query, {"rows": 55_000_000, "num_files": 44, "payload_kind": "mixed",
                                "null_share": 0.0}),
    "exp_hot_kt": (hot_keytable, {"rows": 55_000_000, "num_files": 44, "payload_kind": "mixed",
                                  "payload_random_fraction": 1.0, "hot_share": 0.2,
                                  "null_share": 0.0}),
}


def parse_params(pairs):
    """Turn ['key=value', ...] into typed params using the defaults' types."""
    params = {}
    for pair in pairs or []:
        key, value = pair.split("=", 1)
        if key not in DEFAULTS:
            raise ValueError("unknown parameter " + key)
        params[key] = type(DEFAULTS[key])(value)
    return params


def build_scenario(experiment, pairs, mode=None):
    builder, overrides = EXPERIMENTS[experiment]
    p = dict(DEFAULTS)
    p.update(overrides)
    p.update(parse_params(pairs))
    for key in MODE_REQUIRES.get(mode, ()):
        if p[key] <= 0:
            raise ValueError("mode {} requires parameter {}".format(mode, key))
    return builder(experiment, p)
