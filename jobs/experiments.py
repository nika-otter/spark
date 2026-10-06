"""Experiment definitions: datasets, query and conf overrides per scenario.

A scenario is built from an experiment name plus string parameters (from the
command line), so the runner and the batch scripts share one definition.

null_skew experiments: facts JOIN dim ON k with broadcast joins disabled
(static and adaptive threshold), so the join is always sort-merge.
pipeline experiments: facts joined sequentially to n dims on k1..kn with the
broadcast thresholds at their defaults.
"""

from dataclasses import dataclass
from typing import Callable, Dict

from pyspark.sql import functions as F

from generate import DimSpec, FactsSpec, KeySpec

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
}

# Parameters a mode requires (checked in build_scenario).
MODE_REQUIRES = {
    "aqe_on_rule1": {"accurate_skew_factor"},
    "aqe_on_rule2": {"rule2_ratio"},
    "aqe_on_rules12": {"accurate_skew_factor", "rule2_ratio"},
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
    "null_share": 0.0,                  # NULL share of the (first) join key
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
    # pipeline experiments only
    "n_dims": 0,
    "dim_payload_bytes": 20,
    "match_rate": 1.0,
    "filter_buckets_kept": 10,          # of 1000 -> ~1 % of a filtered dim
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
    return s


def null_skew(experiment, p):
    if p["join"] not in ("left", "inner"):
        raise ValueError("join must be left or inner")
    payload = dict(payload_bytes=p["payload_bytes"], seed=p["seed"],
                   payload_kind=p["payload_kind"],
                   payload_random_fraction=p["payload_random_fraction"])
    facts = FactsSpec(
        rows=p["rows"],
        keys=(KeySpec("k", p["n_keys"], hot_share=0.0, null_share=p["null_share"]),),
        num_files=p["num_files"], **payload)
    dim = DimSpec("k", p["n_keys"], match_rate=1.0, **payload)

    def query(t, mode):
        return t["facts"].join(t["dim_k"], on="k", how=p["join"])

    scenario_id = "{}_null{:g}_rows{}_payload{}".format(
        p["join"], p["null_share"], p["rows"], p["payload_bytes"]) + _suffix(p)
    return Scenario(experiment, scenario_id, p, {"facts": facts, "dim_k": dim},
                    _common_conf(p, broadcast_disabled=True), query)


def is_filtered_dim(i):
    """Dims 1, 3, 5, ... are filtered to ~1 %; dims 2, 4, ... are not."""
    return i % 2 == 1


def pipeline(experiment, p):
    n = p["n_dims"]
    if n < 1 or p["join"] not in ("left", "inner"):
        raise ValueError("pipeline needs n_dims >= 1 and join left/inner")
    keys = tuple(KeySpec("k{}".format(i), p["n_keys"],
                         null_share=p["null_share"] if i == 1 else 0.0)
                 for i in range(1, n + 1))
    facts = FactsSpec(rows=p["rows"], keys=keys, payload_bytes=p["payload_bytes"],
                      seed=p["seed"], payload_kind=p["payload_kind"],
                      payload_random_fraction=p["payload_random_fraction"],
                      num_files=p["num_files"])
    dims = {"dim_k{}".format(i): DimSpec("k{}".format(i), p["n_keys"], match_rate=p["match_rate"],
                                         payload_bytes=p["dim_payload_bytes"], seed=p["seed"])
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
        p["join"], n, p["null_share"], p["rows"], p["payload_bytes"]) + _suffix(p)
    tables = {"facts": facts}
    tables.update(dims)
    return Scenario(experiment, scenario_id, p, tables,
                    _common_conf(p, broadcast_disabled=False), query)


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
