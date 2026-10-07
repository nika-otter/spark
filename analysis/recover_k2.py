"""Recover the k2-join metrics of the batch-5a stress runs (step 3a) from the
History Server.

Before the fix in jobs/metrics.py (batch 5), a skew-optimized join node, named
"SortMergeJoin(skew=true)" in the plan graph, was not recognized, so the run
records of the rules variants have no entry for the k2 join. The History
Server keeps the SQL plan graph and the stages: in the stress plans there are
the joins are nested k5(k4(k3(k2(k1)))); node ids are assigned in pre-order,
so with AQE on (sort-merge joins on k2 and k4 only) k2 is the SortMergeJoin
with the larger id, and with AQE off (five sort-merge joins) it is the fourth
by id (checked against the recorded k2 entries of the AQE-off and AQE-on
runs, see --check). Its stage
is read from the "(stage S.A: task T)" annotations of the Sort nodes below it.

Writes results/exp_h_stress_k2.csv (one row per measured run).
Usage: .venv/bin/python analysis/recover_k2.py [--check]
"""

import json
import re
import sys
import urllib.request
from pathlib import Path

import pandas as pd

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch5a"
HS = "http://localhost:18080/api/v1/applications/"
MIB = 1024 * 1024
STAGE_RE = re.compile(r"\(stage (\d+)\.(\d+): task \d+\)")


def get(path):
    with urllib.request.urlopen(HS + path) as r:
        return json.loads(r.read())


def k2_from_history(app, execution_id):
    ex = get("{}/sql/{}?details=true&planDescription=false".format(app, execution_id))
    nodes = {n["nodeId"]: n for n in ex["nodes"]}
    children = {}
    for e in ex["edges"]:
        children.setdefault(e["toId"], []).append(e["fromId"])
    smj = sorted(n for n, v in nodes.items() if v["nodeName"].startswith("SortMergeJoin"))
    if len(smj) == 2:
        k2 = smj[1]
    elif len(smj) == 5:
        k2 = smj[3]
    else:
        return None
    stages, readers, stack = set(), [], list(children.get(k2, []))
    while stack:
        n = stack.pop()
        name = nodes[n]["nodeName"]
        if name.startswith("SortMergeJoin") or name.startswith("BroadcastHashJoin") or name == "Exchange":
            continue
        metrics = {m["name"]: m["value"] for m in nodes[n].get("metrics", [])}
        if name.startswith("AQEShuffleRead"):
            readers.append(metrics)
        else:
            for v in metrics.values():
                stages.update((int(a), int(b)) for a, b in STAGE_RE.findall(v or ""))
        stack.extend(children.get(n, []))
    out = {"k2_node_name": nodes[k2]["nodeName"], "k2_stages": sorted(stages),
           "k2_skewed_partitions": sum(int(r.get("number of skewed partitions", "0").replace(",", "") or 0)
                                       for r in readers),
           "k2_splits": sum(int(r.get("number of skewed partition splits", "0").replace(",", "") or 0)
                            for r in readers)}
    if len(stages) == 1:
        sid, att = stages.pop()
        st = get("{}/stages/{}/{}?details=false".format(app, sid, att))
        tasks = [t for t in get("{}/stages/{}/{}/taskList?length=100000".format(app, sid, att))
                 if t["status"] == "SUCCESS"]
        out.update({"k2_stage_id": sid, "k2_stage_tasks": len(tasks),
                    "k2_longest_task_s": max(t["duration"] for t in tasks) / 1000,
                    "k2_stage_s": (pd.Timestamp(st["completionTime"].replace("GMT", ""))
                                   - pd.Timestamp(st["submissionTime"].replace("GMT", ""))).total_seconds(),
                    "k2_spill_disk_mib": st["diskBytesSpilled"] / MIB,
                    "k2_spill_memory_mib": st["memoryBytesSpilled"] / MIB})
    return out


def main():
    check = "--check" in sys.argv
    done = {p.name for p in (BATCH / "done").iterdir()}
    rows = []
    for csv_path in sorted((BATCH / "jobs").glob("3a_*.csv")):
        if csv_path.stem not in done:
            continue
        for _, r in pd.read_csv(csv_path).iterrows():
            if r["is_warmup"]:
                continue
            rec = json.loads((BATCH / "jobs" / r["json_path"]).resolve().read_text())
            got = k2_from_history(r["app_id"], rec["metrics"]["sql_execution_id"])
            recorded = [j for j in rec["metrics"]["joins"] if j["left_keys"] == "k2"]
            row = {"job_id": r["job_id"], "run_index": r["run_index"], "mode": r["mode"],
                   "variant": re.search(r"_stress_(.*)$", r["job_id"]).group(1),
                   "iteration": json.loads(r["params"]).get("iteration")}
            row.update(got or {})
            if recorded and recorded[0]["stage"]:
                row["recorded_k2_stage_id"] = recorded[0]["stage"]["stage_id"]
                row["recorded_k2_longest_task_s"] = recorded[0]["stage"]["task_duration_ms"]["max"] / 1000
            rows.append(row)
    df = pd.DataFrame(rows)
    df.to_csv(RESULTS / "exp_h_stress_k2.csv", index=False)
    if check:
        c = df.dropna(subset=["recorded_k2_stage_id"])
        print("runs with a recorded k2 entry:", len(c), " stage ids equal:",
              int((c["recorded_k2_stage_id"] == c["k2_stage_id"]).sum()),
              " longest task equal:",
              int(((c["recorded_k2_longest_task_s"] - c["k2_longest_task_s"]).abs() < 0.002).sum()))
    print(df.groupby(["variant"])[["k2_skewed_partitions", "k2_splits", "k2_stage_tasks",
                                   "k2_longest_task_s", "k2_stage_s", "k2_spill_disk_mib"]]
          .median().round(2).to_string())


if __name__ == "__main__":
    main()
