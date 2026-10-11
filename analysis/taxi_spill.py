"""Batch 6, step 4b: where the taxi spill at 2001 partitions comes from.

From the batch-5c step-2 runs of taxi PULocationID at 2001 partitions (all
modes), without new runs:
  - spill per stage (run JSON: metrics.stages): the fact scan / shuffle-write
    stage, the key-table stage and the join stage;
  - in the join stage, the SQL "spill size" of the Sort over the fact shuffle
    and of the Sort over the key-table shuffle (History Server REST, plan
    graph; the sides are told apart by the "records read" of the exchange
    below each Sort). Caveat (SortExec.scala:121-125, 171-176): this metric is
    the increase of the task's memoryBytesSpilled while that Sort runs, so it
    also counts spills of other memory consumers of the same task; it does not
    attribute spill to a join side reliably;
  - the join-stage tasks that spill: count and their shuffle read.
Writes results/step4b_taxi_spill.csv (one row per measured run) and prints
medians per mode.

Usage: .venv/bin/python analysis/taxi_spill.py   (History Server on :18080)
"""

import json
import re
import urllib.request
from pathlib import Path

import pandas as pd

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch5c"
HS = "http://localhost:18080/api/v1/applications/"
MIB = 1024 * 1024
UNITS = {"B": 1, "KiB": 1024, "MiB": MIB, "GiB": 1024 ** 3, "TiB": 1024 ** 4}


def get(path):
    with urllib.request.urlopen(HS + path) as r:
        return json.loads(r.read())


def size(value):
    m = re.match(r"([\d.,]+)\s*(B|KiB|MiB|GiB|TiB)", (value or "").strip().split("\n")[-1])
    return float(m.group(1).replace(",", "")) * UNITS[m.group(2)] if m else 0.0


def main():
    done = {p.name for p in (BATCH / "done").iterdir()}
    rows = []
    for csv_path in sorted((BATCH / "jobs").glob("s2_*taxi_PULocationID_sp2001_*.csv")):
        if csv_path.stem not in done:
            continue
        for _, r in pd.read_csv(csv_path).iterrows():
            if r["is_warmup"]:
                continue
            rec = json.loads((BATCH / "jobs" / r["json_path"]).resolve().read_text())
            m = rec["metrics"]
            join_id = m["join_stage"]["stage_id"]
            scans = [s for s in m["stages"] if s["status"] == "COMPLETE" and s["stage_id"] != join_id]
            fact_scan = max(scans, key=lambda s: s["shuffle_write_bytes"])
            other = [s for s in scans if s is not fact_scan]
            join = [s for s in m["stages"] if s["stage_id"] == join_id][0]
            ex = get("{}/sql/{}?details=true&planDescription=false".format(r["app_id"], m["sql_execution_id"]))
            nodes = {n["nodeId"]: n for n in ex["nodes"]}
            child = {}
            for e in ex["edges"]:
                child.setdefault(e["toId"], []).append(e["fromId"])
            sort_spill = {}
            for n in ex["nodes"]:
                if n["nodeName"] != "Sort":
                    continue
                below, stack = None, list(child.get(n["nodeId"], []))
                while stack and below is None:
                    c = stack.pop()
                    if nodes[c]["nodeName"] == "Exchange":
                        below = c
                    stack.extend(child.get(c, []))
                recs = {x["name"]: x["value"] for x in nodes[below]["metrics"]}.get("records read", "0")
                side = "fact" if int(recs.replace(",", "")) > 1_000_000 else "keys"
                sort_spill[side] = size({x["name"]: x["value"] for x in n["metrics"]}.get("spill size"))
            tasks = get("{}/stages/{}/{}/taskList?length=100000".format(r["app_id"], join_id, join["attempt_id"]))
            sp = [t for t in tasks if t["status"] == "SUCCESS" and t["taskMetrics"]["diskBytesSpilled"] > 0]
            read = lambda t: (t["taskMetrics"]["shuffleReadMetrics"]["localBytesRead"]
                              + t["taskMetrics"]["shuffleReadMetrics"]["remoteBytesRead"])
            rows.append({
                "job_id": r["job_id"], "mode": r["mode"],
                "spill_disk_mib_total": r["spill_disk_bytes"] / MIB,
                "spill_disk_mib_fact_scan_stage": fact_scan["spill_disk_bytes"] / MIB,
                "fact_scan_stage_tasks": fact_scan["num_tasks"],
                "spill_disk_mib_other_scan_stages": sum(s["spill_disk_bytes"] for s in other) / MIB,
                "spill_disk_mib_join_stage": join["spill_disk_bytes"] / MIB,
                "sort_metric_spill_mib_fact_side": sort_spill.get("fact", 0) / MIB,
                "sort_metric_spill_mib_keys_side": sort_spill.get("keys", 0) / MIB,
                "join_tasks": join["num_tasks"],
                "join_tasks_spilling": len(sp),
                "spilling_task_read_mib_min": min(map(read, sp)) / MIB if sp else None,
                "spilling_task_read_mib_max": max(map(read, sp)) / MIB if sp else None,
            })
    df = pd.DataFrame(rows)
    df.to_csv(RESULTS / "step4b_taxi_spill.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_columns", None):
        print(df.groupby("mode").median(numeric_only=True).round(1).T.to_string())


if __name__ == "__main__":
    main()
