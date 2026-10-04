"""Experiment definitions: datasets, query and conf overrides per scenario.

A scenario is built from an experiment name plus string parameters (from the
command line), so the runner and the grid scripts share one definition.
"""

from dataclasses import dataclass
from typing import Callable, Dict

from generate import DimSpec, FactsSpec, KeySpec

MODES = {
    "aqe_off": {"spark.sql.adaptive.enabled": "false"},
    "aqe_on": {"spark.sql.adaptive.enabled": "true"},
}


@dataclass
class Scenario:
    experiment: str
    scenario_id: str
    params: Dict[str, object]
    tables: Dict[str, object]           # table name -> FactsSpec / DimSpec
    conf: Dict[str, str]                # overrides on top of BASELINE_CONF
    query: Callable                     # (dict of DataFrames) -> DataFrame


# ---------------------------------------------------------------------------
# Pilot: share of NULLs in the join key of facts, LEFT vs INNER join.
# Broadcast joins are disabled (static and adaptive) so the join is always
# sort-merge; every other AQE parameter stays at its default.

PILOT_NULL_DEFAULTS = {
    "join": "left",
    "null_share": 0.0,
    "rows": 30_000_000,
    "payload_bytes": 100,
    "n_keys": 1_000_000,
    "seed": 42,
}


def pilot_null(params):
    p = dict(PILOT_NULL_DEFAULTS)
    p.update(params)
    if p["join"] not in ("left", "inner"):
        raise ValueError("join must be left or inner")

    facts = FactsSpec(
        rows=p["rows"],
        keys=(KeySpec("k", p["n_keys"], hot_share=0.0, null_share=p["null_share"]),),
        payload_bytes=p["payload_bytes"],
        seed=p["seed"],
    )
    dim = DimSpec("k", p["n_keys"], match_rate=1.0, payload_bytes=p["payload_bytes"],
                  seed=p["seed"])

    def query(t):
        return t["facts"].join(t["dim_k"], on="k", how=p["join"])

    scenario_id = "{}_null{:g}_rows{}_payload{}".format(
        p["join"], p["null_share"], p["rows"], p["payload_bytes"])
    return Scenario(
        experiment="pilot_null",
        scenario_id=scenario_id,
        params=p,
        tables={"facts": facts, "dim_k": dim},
        conf={
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
        },
        query=query,
    )


EXPERIMENTS = {
    "pilot_null": (pilot_null, PILOT_NULL_DEFAULTS),
}


def parse_params(experiment, pairs):
    """Turn ['key=value', ...] into typed params using the defaults' types."""
    defaults = EXPERIMENTS[experiment][1]
    params = {}
    for pair in pairs or []:
        key, value = pair.split("=", 1)
        if key not in defaults:
            raise ValueError("unknown parameter {} for {}".format(key, experiment))
        params[key] = type(defaults[key])(value)
    return params


def build_scenario(experiment, pairs):
    return EXPERIMENTS[experiment][0](parse_params(experiment, pairs))
