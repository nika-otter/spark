"""Batch 4, step 4: the task holding the NULL partition in the batch-3 combined
check (exp_rules), AQE off vs AQE on, from event logs and executor logs only.

Usage (host, project venv):
  .venv/bin/python analysis/step4_null_task.py EVENTLOG_DIR [STDERR_DIR]

EVENTLOG_DIR holds copies of /opt/spark-events/eventlog_v2_<app> of the eight
applications (4 x aqe_off, 4 x aqe_on); STDERR_DIR holds copies of
/opt/spark-local/work/<app> from the three workers, named w<N>-<app>.
For every measured run: the join stage (largest shuffle read), its longest
task with all task metrics, the SQL metrics that task reported for the
facts-side shuffle read and Sort (accumulator ids mapped to plan nodes via the
SQL plan events), the tasks running concurrently on the same executor and,
if STDERR_DIR is given, the UnsafeExternalSorter spill log lines on that
executor during the task.
Writes results/step4_null_task.csv and prints a comparison.
"""

import glob
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

RESULTS = Path(__file__).resolve().parent.parent / "results"
MIB = 1024 * 1024


def events(app_dir):
    for path in sorted(glob.glob(os.path.join(app_dir, "events_*"))):
        with open(path) as f:
            for line in f:
                yield json.loads(line)


SPILL_RE = re.compile(r"^(\d\d/\d\d/\d\d \d\d:\d\d:\d\d) INFO UnsafeExternalSorter: Thread (\d+) "
                      r"spilling sort data of ([\d.]+) MiB")


def plan_metrics(info, out, path=()):
    """accumulator id -> (node name, node path, metric name) over a SparkPlanInfo tree."""
    for m in info.get("metrics", []):
        out[m["accumulatorId"]] = (info["nodeName"], path, m["name"])
    for i, child in enumerate(info.get("children", [])):
        plan_metrics(child, out, path + (i,))


def facts_side_sql_metrics(task, accmap):
    """SQL metrics of the task for the Exchange/Sort with the most records/memory."""
    by_node = {}
    for a in task["Task Info"]["Accumulables"]:
        if a["ID"] in accmap:
            name, path, metric = accmap[a["ID"]]
            # SQL metric updates are logged as strings or numbers
            by_node.setdefault((name, path), {})[metric] = float(a.get("Update") or 0)
    exchanges = [v for (n, _), v in by_node.items() if n == "Exchange"]
    sorts = [v for (n, _), v in by_node.items() if n == "Sort"]
    ex = max(exchanges, key=lambda v: v.get("records read", 0), default={})
    so = max(sorts, key=lambda v: v.get("peak memory", 0), default={})
    return {
        "sql_records_read": ex.get("records read"),
        "sql_blocks_read": (ex.get("remote blocks read", 0) or 0) + (ex.get("local blocks read", 0) or 0),
        "sql_remote_reqs_duration_ms": ex.get("remote reqs duration"),
        "sql_fetch_wait_ms": ex.get("fetch wait time"),
        "sql_sort_time_ms": so.get("sort time"),
        "sql_sort_peak_memory_mib": (so.get("peak memory") or 0) / MIB,
        "sql_sort_spill_size_mib": (so.get("spill size") or 0) / MIB,
    }


def spill_lines(stderr_dir, app, host, executor, launch_ms, finish_ms):
    """(seconds after launch, thread, MiB) of spill log lines during the task."""
    worker = host.rsplit("-", 1)[1]
    paths = glob.glob(os.path.join(stderr_dir, "w{}-{}".format(worker, app), str(executor), "stderr"))
    out = []
    for path in paths:
        with open(path) as f:
            for line in f:
                m = SPILL_RE.match(line)
                if not m:
                    continue
                ts = pd.Timestamp(datetime.strptime(m.group(1), "%y/%m/%d %H:%M:%S"),
                                  tz="UTC").timestamp()  # container clocks are UTC
                if launch_ms / 1000 - 1 <= ts <= finish_ms / 1000 + 1:
                    out.append((int(ts - launch_ms / 1000), int(m.group(2)), float(m.group(3))))
    return out


def load(app_dir):
    job_desc, stage_job, stages, tasks, plans = {}, {}, {}, [], []
    for ev in events(app_dir):
        e = ev["Event"]
        if e == "SparkListenerJobStart":
            desc = (ev.get("Properties") or {}).get("spark.job.description")
            job_desc[ev["Job ID"]] = desc
            for s in ev["Stage IDs"]:
                stage_job[s] = ev["Job ID"]
        elif e == "SparkListenerStageCompleted":
            info = ev["Stage Info"]
            stages[info["Stage ID"]] = info
        elif e == "SparkListenerTaskEnd" and ev["Task End Reason"]["Reason"] == "Success":
            tasks.append(ev)
        elif e.endswith("SparkListenerSQLAdaptiveExecutionUpdate") or \
                e.endswith("SparkListenerSQLExecutionStart"):
            plans.append(ev)
    return job_desc, stage_job, stages, tasks, plans


def task_row(ev):
    ti, tm = ev["Task Info"], ev["Task Metrics"]
    sr = tm["Shuffle Read Metrics"]
    sw = tm.get("Shuffle Write Metrics", {})
    return {
        "stage_id": ev["Stage ID"], "task_id": ti["Task ID"], "index": ti["Index"],
        "executor": ti["Executor ID"], "host": ti["Host"],
        "launch_ms": ti["Launch Time"], "finish_ms": ti["Finish Time"],
        "duration_ms": ti["Finish Time"] - ti["Launch Time"],
        "executor_run_time_ms": tm["Executor Run Time"],
        "executor_cpu_time_ms": tm["Executor CPU Time"] / 1e6,
        "deserialize_ms": tm["Executor Deserialize Time"],
        "gc_ms": tm["JVM GC Time"],
        "result_serialize_ms": tm["Result Serialization Time"],
        "fetch_wait_ms": sr["Fetch Wait Time"],
        "remote_mib": sr["Remote Bytes Read"] / MIB,
        "local_mib": sr["Local Bytes Read"] / MIB,
        "remote_to_disk_mib": sr.get("Remote Bytes Read To Disk", 0) / MIB,
        "records_read": sr["Total Records Read"],
        "remote_blocks": sr["Remote Blocks Fetched"],
        "local_blocks": sr["Local Blocks Fetched"],
        "memory_spilled_mib": tm["Memory Bytes Spilled"] / MIB,
        "disk_spilled_mib": tm["Disk Bytes Spilled"] / MIB,
        "peak_exec_mem_mib": tm["Peak Execution Memory"] / MIB,
        "shuffle_write_mib": sw.get("Shuffle Bytes Written", 0) / MIB,
    }


def main(eventlog_dir, stderr_dir=None):
    rows = []
    for app_dir in sorted(glob.glob(os.path.join(eventlog_dir, "eventlog_v2_app-*"))):
        app = app_dir.rsplit("eventlog_v2_", 1)[1]
        job_desc, stage_job, stages, tasks, plans = load(app_dir)
        accmap = {}
        for ev in plans:
            plan_metrics(ev["sparkPlanInfo"], accmap)
        by_task_id = {t["Task Info"]["Task ID"]: t for t in tasks}
        df = pd.DataFrame([task_row(t) for t in tasks])
        df["desc"] = df["stage_id"].map(lambda s: job_desc.get(stage_job.get(s)))
        for desc, g in df.groupby("desc"):
            if not desc or "run-0" in desc:
                continue  # setup queries and the warm-up run
            read = g.assign(read=g.remote_mib + g.local_mib).groupby("stage_id")["read"].sum()
            join_stage = read.idxmax()
            st = g[g.stage_id == join_stage]
            t = st.loc[st.duration_ms.idxmax()].to_dict()
            # concurrency on the same executor while the longest task ran
            same = df[(df.executor == t["executor"]) & (df.task_id != t["task_id"])
                      & (df.launch_ms < t["finish_ms"]) & (df.finish_ms > t["launch_ms"])]
            overlap = ((same[["finish_ms"]].clip(upper=t["finish_ms"])["finish_ms"]
                        - same[["launch_ms"]].clip(lower=t["launch_ms"])["launch_ms"]).sum()
                       / t["duration_ms"])
            info = stages[join_stage]
            t.update(facts_side_sql_metrics(by_task_id[t["task_id"]], accmap))
            if stderr_dir:
                spills = spill_lines(stderr_dir, app, t["host"], t["executor"],
                                     t["launch_ms"], t["finish_ms"])
                t["spill_log_lines"] = len(spills)
                t["spill_log_detail"] = ";".join("{}s:thread{}:{:g}MiB".format(*x) for x in spills)
            t.update({
                "app": app, "mode": "aqe_on" if st.shape[0] < 100 else "aqe_off",
                "run": desc.rsplit("|", 1)[1],
                "join_stage_tasks": len(st),
                "join_stage_ms": info["Completion Time"] - info["Submission Time"],
                "concurrent_tasks_same_executor": len(same),
                "mean_other_tasks_running_same_executor": overlap,
                "other_tasks_same_executor_read_mib": (same.remote_mib + same.local_mib).sum(),
                "second_longest_task_ms": st.duration_ms.nlargest(2).iloc[-1],
                "stage_task_read_median_mib": (st.remote_mib + st.local_mib).median(),
            })
            rows.append(t)
    out = pd.DataFrame(rows).sort_values(["mode", "app", "run"])
    out.to_csv(RESULTS / "step4_null_task.csv", index=False)
    with pd.option_context("display.width", 250, "display.max_columns", None):
        print(out.T.to_string())
        num = out.select_dtypes("number").columns
        print(out.groupby("mode")[num].median().T.round(2).to_string())


if __name__ == "__main__":
    main(*sys.argv[1:])
