"""Experiment definitions: datasets, query and conf overrides per scenario.

A scenario is built from an experiment name plus string parameters (from the
command line), so the runner and the batch scripts share one definition.

All experiments here are variants of one query, facts JOIN dim ON k with
broadcast joins disabled (static and adaptive threshold), so the join is
always sort-merge. They differ in their default parameters only.
"""

from dataclasses import dataclass
from typing import Callable, Dict

from generate import DimSpec, FactsSpec, KeySpec

MODES = {
    "aqe_off": {"spark.sql.adaptive.enabled": "false"},
    "aqe_on": {"spark.sql.adaptive.enabled": "true"},
    "aqe_on_nocoalesce": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
    },
    "aqe_on_skew64m": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(64 * 1024 * 1024),
    },
}


@dataclass
class Scenario:
    experiment: str
    scenario_id: str
    params: Dict[str, object]
    tables: Dict[str, object]           # table name -> FactsSpec / DimSpec
    conf: Dict[str, str]                # overrides on top of BASELINE_CONF
    query: Callable                     # (dict of DataFrames) -> DataFrame


NULL_SKEW_DEFAULTS = {
    "join": "left",
    "null_share": 0.0,
    "rows": 55_000_000,
    "payload_bytes": 100,
    "payload_kind": "hex",
    "n_keys": 1_000_000,
    "seed": 42,
    "shuffle_partitions": 200,
    "codec": "lz4",                      # spark.io.compression.codec (Spark default)
    "accurate_skew_factor": -1.0,        # spark.shuffle.accurateBlockSkewedFactor (Spark default)
}


def null_skew(experiment, params):
    p = dict(NULL_SKEW_DEFAULTS)
    p.update(params)
    if p["join"] not in ("left", "inner"):
        raise ValueError("join must be left or inner")

    facts = FactsSpec(
        rows=p["rows"],
        keys=(KeySpec("k", p["n_keys"], hot_share=0.0, null_share=p["null_share"]),),
        payload_bytes=p["payload_bytes"],
        seed=p["seed"],
        payload_kind=p["payload_kind"],
    )
    dim = DimSpec("k", p["n_keys"], match_rate=1.0, payload_bytes=p["payload_bytes"],
                  seed=p["seed"], payload_kind=p["payload_kind"])

    def query(t):
        return t["facts"].join(t["dim_k"], on="k", how=p["join"])

    scenario_id = "{}_null{:g}_rows{}_payload{}".format(
        p["join"], p["null_share"], p["rows"], p["payload_bytes"])
    if p["payload_kind"] != "hex":
        scenario_id += "_" + p["payload_kind"]
    if p["shuffle_partitions"] != 200:
        scenario_id += "_sp{}".format(p["shuffle_partitions"])
    if p["codec"] != "lz4":
        scenario_id += "_" + p["codec"]
    if p["accurate_skew_factor"] > 0:
        scenario_id += "_accskew{:g}".format(p["accurate_skew_factor"])

    conf = {
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": str(p["shuffle_partitions"]),
        # Core confs: must be set before the SparkContext starts (the runner
        # creates the session with them).
        "spark.io.compression.codec": p["codec"],
        "spark.shuffle.accurateBlockSkewedFactor": str(p["accurate_skew_factor"]),
    }
    return Scenario(experiment, scenario_id, p, {"facts": facts, "dim_k": dim}, conf, query)


# Experiment name -> defaults that differ from NULL_SKEW_DEFAULTS.
EXPERIMENTS = {
    "pilot_null": {},
    "inner_check": {"join": "inner"},
    "exp_a_small": {"rows": 13_700_000},
    "exp_b_partitions": {"null_share": 0.2},
    "exp_c_compression": {"null_share": 0.08},
}


def parse_params(pairs):
    """Turn ['key=value', ...] into typed params using the defaults' types."""
    params = {}
    for pair in pairs or []:
        key, value = pair.split("=", 1)
        if key not in NULL_SKEW_DEFAULTS:
            raise ValueError("unknown parameter " + key)
        params[key] = type(NULL_SKEW_DEFAULTS[key])(value)
    return params


def build_scenario(experiment, pairs):
    params = dict(EXPERIMENTS[experiment])
    params.update(parse_params(pairs))
    return null_skew(experiment, params)
