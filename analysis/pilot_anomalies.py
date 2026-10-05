"""Checks of three pilot_null anomalies from event logs and executor logs.

  a) spill: tasks that spilled per run, and spill events from executor logs
     ("UnsafeExternalSorter ... spilling sort data of X to disk")
  b) skew splits vs number of map tasks of the facts scan stage
  c) join stage duration next to the longest join task

Writes results/pilot_anomalies_spill.csv, results/pilot_anomalies_splits.csv,
results/pilot_null_stage_durations.csv.
Run on the host with the cluster up:  .venv/bin/python analysis/pilot_anomalies.py
"""

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
WORKERS = ["spark-worker-1", "spark-worker-2", "spark-worker-3"]
SPILL_RE = re.compile(
    r"^(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) INFO UnsafeExternalSorter: Thread (\d+) spilling sort "
    r"data of ([\d.]+ [KMGT]?i?B) to disk \((\d+) times? so far\)")


def docker(*args):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout


def load_runs():
    df = pd.read_csv(RESULTS / "runs.csv")
    df = df[(df["experiment"] == "pilot_null") & (~df["is_warmup"])].copy()
    params = df["params"].apply(json.loads)
    df["join_type"] = params.apply(lambda p: p["join"])
    df["null_share"] = params.apply(lambda p: p["null_share"])
    return df


def event_logs(app_ids):
    out = docker("exec", "spark-master", "python3", "/opt/project/analysis/eventlog_extract.py",
                 *app_ids)
    return json.loads(out)


def spill_events(app_ids):
    rows = []
    for worker in WORKERS:
        for app in app_ids:
            try:
                text = docker("exec", worker, "bash", "-c",
                              "cat /opt/spark-local/work/{}/*/stderr 2>/dev/null || true".format(app))
            except subprocess.CalledProcessError:
                continue
            for line in text.splitlines():
                m = SPILL_RE.match(line)
                if m:
                    ts = datetime.strptime(m.group(1), "%y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    rows.append({"app_id": app, "worker": worker, "ts_ms": ts.timestamp() * 1000,
                                 "thread": m.group(2), "size": m.group(3),
                                 "previous_spills": int(m.group(4))})
    return pd.DataFrame(rows)


def run_json(row):
    with open(RESULTS / row["json_path"]) as f:
        return json.load(f)


def main():
    runs = load_runs()
    apps = sorted(runs["app_id"].unique())
    logs = event_logs(apps)
    spills = spill_events(apps)

    durations, spill_rows, split_rows = [], [], []
    for _, row in runs.iterrows():
        ev = logs[row["app_id"]]
        stage = [s for s in ev["stages"] if s["stage_id"] == row["join_stage_id"]][0]
        tasks = [t for t in ev["tasks"] if t["stage_id"] == row["join_stage_id"]]
        start, end = stage["submission_ms"], stage["completion_ms"]
        durations.append({
            "join_type": row["join_type"], "null_share": row["null_share"], "mode": row["mode"],
            "run_index": row["run_index"], "wall_time_s": row["elapsed_seconds"],
            "join_stage_duration_s": (end - start) / 1000,
            "longest_join_task_s": max(t["finish_ms"] - t["launch_ms"] for t in tasks) / 1000,
        })

        spilled = [t for t in tasks if t["memory_spilled"] or t["disk_spilled"]]
        events = spills[(spills["app_id"] == row["app_id"]) & (spills["ts_ms"] >= start - 1000)
                        & (spills["ts_ms"] <= end + 1000)] if len(spills) else spills
        spill_rows.append({
            "join_type": row["join_type"], "null_share": row["null_share"], "mode": row["mode"],
            "run_index": row["run_index"],
            "tasks_with_spill": len(spilled),
            "task_memory_spilled": ";".join(str(t["memory_spilled"]) for t in spilled),
            "task_disk_spilled": ";".join(str(t["disk_spilled"]) for t in spilled),
            "task_peak_execution_memory": ";".join(str(t["peak_execution_memory"]) for t in spilled),
            "task_shuffle_read_bytes": ";".join(str(t["shuffle_read_bytes"]) for t in spilled),
            "spill_log_events": len(events),
            "spill_log_sizes": ";".join(events["size"]) if len(events) else "",
            "spill_log_previous_spills": ";".join(map(str, events["previous_spills"])) if len(events) else "",
        })

        record = run_json(row)
        scans = [s for s in record["metrics"]["stages"]
                 if s["status"] == "COMPLETE" and s["input_bytes"] > 0]
        facts = max(scans, key=lambda s: s["input_bytes"])
        split_rows.append({
            "join_type": row["join_type"], "null_share": row["null_share"], "mode": row["mode"],
            "run_index": row["run_index"], "facts_scan_map_tasks": facts["num_tasks"],
            "skewed_partitions": row["skewed_partitions"], "skew_splits": row["skew_splits"],
        })

    keys = ["join_type", "null_share", "mode"]
    d = pd.DataFrame(durations)
    d_sum = d.groupby(keys).agg(
        runs=("run_index", "size"),
        wall_time_s_median=("wall_time_s", "median"),
        join_stage_duration_s_median=("join_stage_duration_s", "median"),
        longest_join_task_s_median=("longest_join_task_s", "median"),
    ).reset_index()
    d_sum.to_csv(RESULTS / "pilot_null_stage_durations.csv", index=False)

    s = pd.DataFrame(spill_rows)
    s.to_csv(RESULTS / "pilot_anomalies_spill.csv", index=False)

    sp = pd.DataFrame(split_rows)
    sp_sum = sp.groupby(keys).agg(
        facts_scan_map_tasks=("facts_scan_map_tasks", lambda x: "|".join(map(str, sorted(set(x))))),
        skew_splits=("skew_splits", lambda x: "|".join(map(str, sorted(set(x))))),
    ).reset_index()
    sp_sum.to_csv(RESULTS / "pilot_anomalies_splits.csv", index=False)

    with pd.option_context("display.width", 250, "display.max_columns", None,
                           "display.max_colwidth", 80):
        print(d_sum.to_string(index=False))
        print()
        print(s[(s["mode"] == "aqe_off") & (s["join_type"] == "left")].to_string(index=False))
        print()
        print(sp_sum.to_string(index=False))


if __name__ == "__main__":
    main()
