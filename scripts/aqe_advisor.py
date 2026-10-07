"""Recommend AQE settings from a previous run (batch 5, step 3d).

Input: either the application id of a run known to the History Server
(REST API, default http://localhost:18080) or the path of its event log
(an eventlog_v2_<app> directory or a single event log file).

From the SQL executions of the run it sums, over all shuffle exchanges, the
"data size" (UnsafeRow bytes written to the shuffle) and "shuffle bytes
written" (bytes after the codec) and computes r = data size / bytes written.
It prints:
  rule 1  spark.shuffle.accurateBlockSkewedFactor=5.0 when the run used more
          than 2000 shuffle partitions (HighlyCompressedMapStatus)
  rule 2  advisoryPartitionSizeInBytes = 64 MiB / r and
          skewedPartitionThresholdInBytes = 256 MiB / r
each with a one-line justification. r is defined on a run with AQE on and
default settings; a warning is printed when the input run had AQE off.

Usage:
  .venv/bin/python scripts/aqe_advisor.py --app-id app-20261006071154-0272
  python3 scripts/aqe_advisor.py --eventlog /opt/spark-events/eventlog_v2_app-...
  options: --history-url URL, --execution TEXT (only SQL executions whose
           description contains TEXT)
"""

import argparse
import glob
import json
import os
import re
import sys
import urllib.request

MIB = 1024 * 1024
HIGHLY_COMPRESSED_ABOVE = 2000   # spark.shuffle.minNumPartitionsToHighlyCompress default
UNITS = {"B": 1, "KiB": 1024, "MiB": MIB, "GiB": 1024 ** 3, "TiB": 1024 ** 4}


def parse_size(value):
    """Total of a SQL size metric string as shown by the UI / REST API."""
    if not value:
        return 0.0
    line = value.strip().split("\n")[-1]
    m = re.match(r"([\d.,]+)\s*(B|KiB|MiB|GiB|TiB)", line)
    return float(m.group(1).replace(",", "")) * UNITS[m.group(2)] if m else 0.0


def parse_count(value):
    v = (value or "").strip().replace(",", "")
    return int(v) if re.fullmatch(r"\d+", v) else None


# --------------------------------------------------------------------------- inputs

def from_rest(base, app_id, execution_filter):
    def get(path):
        with urllib.request.urlopen("{}/api/v1/applications/{}{}".format(base, app_id, path)) as r:
            return json.loads(r.read())
    env = get("/environment")
    props = dict(env.get("sparkProperties", []))
    version = env.get("runtime", {}).get("sparkVersion") or get("")["attempts"][0].get("appSparkVersion")
    shuffles = []
    for ex in get("/sql?details=true&planDescription=false&length=100000"):
        if execution_filter and execution_filter not in (ex.get("description") or ""):
            continue
        for node in ex.get("nodes", []):
            if node["nodeName"] != "Exchange":
                continue
            m = {x["name"]: x["value"] for x in node.get("metrics", [])}
            shuffles.append({"execution": ex["id"], "data": parse_size(m.get("data size")),
                             "written": parse_size(m.get("shuffle bytes written")),
                             "partitions": parse_count(m.get("number of partitions"))})
    return props, version, shuffles


def walk(info, out):
    if info["nodeName"] == "Exchange":
        for metric in info.get("metrics", []):
            out[metric["accumulatorId"]] = metric["name"]
    for child in info.get("children", []):
        walk(child, out)


def from_eventlog(path, execution_filter):
    files = sorted(glob.glob(os.path.join(path, "events_*"))) if os.path.isdir(path) else [path]
    props, version = {}, None
    acc_exec, acc_name, totals, executions = {}, {}, {}, {}
    exchange_ids_by_exec = {}
    stage_exec = {}
    for f in files:
        with open(f) as fh:
            for line in fh:
                ev = json.loads(line)
                e = ev["Event"]
                if e == "SparkListenerLogStart":
                    version = ev.get("Spark Version")
                elif e == "SparkListenerEnvironmentUpdate":
                    props = ev.get("Spark Properties", {})
                elif e.endswith("SparkListenerSQLExecutionStart") or \
                        e.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
                    exec_id = ev["executionId"]
                    if "description" in ev:
                        executions[exec_id] = ev["description"]
                    ids = {}
                    walk(ev["sparkPlanInfo"], ids)
                    for acc_id, name in ids.items():
                        acc_exec[acc_id] = exec_id
                        acc_name[acc_id] = name
                    exchange_ids_by_exec.setdefault(exec_id, set()).update(ids)
                elif e == "SparkListenerTaskEnd":
                    for a in ev.get("Task Info", {}).get("Accumulables", []):
                        if a["ID"] in acc_name and a.get("Update") is not None:
                            totals[a["ID"]] = totals.get(a["ID"], 0) + float(a["Update"])
                elif e.endswith("SparkListenerDriverAccumUpdates"):
                    for acc_id, value in ev.get("accumUpdates", []):
                        if acc_id in acc_name:
                            totals[acc_id] = totals.get(acc_id, 0) + float(value)
    # Group accumulators per exchange: an exchange node has one id per metric;
    # pair "data size" and "shuffle bytes written" ids of the same node by order
    # of appearance in the plan (ids of one node are consecutive).
    shuffles = []
    for exec_id, ids in exchange_ids_by_exec.items():
        if execution_filter and execution_filter not in (executions.get(exec_id) or ""):
            continue
        by_name = {}
        for acc_id in sorted(ids):
            by_name.setdefault(acc_name[acc_id], []).append(acc_id)
        for d_id, w_id in zip(by_name.get("data size", []), by_name.get("shuffle bytes written", [])):
            data, written = totals.get(d_id, 0.0), totals.get(w_id, 0.0)
            if data or written:
                shuffles.append({"execution": exec_id, "data": data, "written": written,
                                 "partitions": None})
    return props, version, shuffles


# --------------------------------------------------------------------------- advice

def advise(props, version, shuffles, source):
    used = [s for s in shuffles if s["data"] > 0 and s["written"] > 0]
    print("source: {}".format(source))
    print("Spark {}, AQE enabled in the run: {}, codec: {}".format(
        version, props.get("spark.sql.adaptive.enabled", "true (default)"),
        props.get("spark.io.compression.codec", "lz4 (default)")))
    if not used:
        print("no shuffle with data found: no recommendation")
        return
    data = sum(s["data"] for s in used)
    written = sum(s["written"] for s in used)
    r = data / written
    ratios = sorted(s["data"] / s["written"] for s in used)
    print("shuffles: {} in {} SQL executions; data size {:.1f} MiB, bytes written {:.1f} MiB; "
          "r = {:.3f} (per shuffle {:.2f} .. {:.2f})".format(
              len(used), len({s["execution"] for s in used}), data / MIB, written / MIB, r,
              ratios[0], ratios[-1]))
    if props.get("spark.sql.adaptive.enabled", "true").lower() == "false":
        print("warning: the run had AQE off; r is defined on a run with AQE on and default settings")

    partitions = max([s["partitions"] for s in used if s["partitions"]] or
                     [int(props.get("spark.sql.shuffle.partitions", 200))])
    print("recommended settings:")
    if partitions > HIGHLY_COMPRESSED_ABOVE:
        print("  spark.shuffle.accurateBlockSkewedFactor=5.0"
              "  # rule 1: {} shuffle partitions > 2000, map outputs use HighlyCompressedMapStatus, "
              "which reports non-huge blocks as the average size".format(partitions))
    else:
        print("  (rule 1 not needed)  # {} shuffle partitions <= 2000: CompressedMapStatus keeps "
              "every block size".format(partitions))
    print("  spark.sql.adaptive.advisoryPartitionSizeInBytes={}  # rule 2: 64 MiB / r, r = {:.3f}; "
          "AQE compares this with compressed block sizes".format(int(64 * MIB / r), r))
    print("  spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes={}  # rule 2: 256 MiB / r, "
          "the default threshold expressed in compressed bytes".format(int(256 * MIB / r)))


def main():
    parser = argparse.ArgumentParser()
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--app-id")
    src.add_argument("--eventlog")
    parser.add_argument("--history-url", default="http://localhost:18080")
    parser.add_argument("--execution", default=None)
    args = parser.parse_args()
    if args.app_id:
        props, version, shuffles = from_rest(args.history_url, args.app_id, args.execution)
        advise(props, version, shuffles, "History Server {} app {}".format(args.history_url, args.app_id))
    else:
        props, version, shuffles = from_eventlog(args.eventlog, args.execution)
        advise(props, version, shuffles, "event log " + args.eventlog)


if __name__ == "__main__":
    sys.exit(main())
