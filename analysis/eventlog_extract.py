"""Extract stage and task records from Spark event logs (runs in the master container).

Usage: python3 eventlog_extract.py APP_ID [APP_ID ...] > out.json

Output: {app_id: {"stages": [...], "tasks": [...]}} where times are epoch ms.
Only successful task attempts are kept.
"""

import glob
import json
import sys

EVENT_DIR = "/opt/spark-events"


def event_files(app_id):
    files = glob.glob("{}/eventlog_v2_{}/events_*".format(EVENT_DIR, app_id))
    files += glob.glob("{}/{}".format(EVENT_DIR, app_id))  # non-rolling layout
    return sorted(files, key=lambda f: int(f.rsplit("/", 1)[1].split("_")[1])
                  if "/events_" in f else 0)


def extract(app_id):
    stages, tasks = [], []
    for path in event_files(app_id):
        with open(path) as f:
            for line in f:
                if '"SparkListenerStageCompleted"' in line:
                    info = json.loads(line)["Stage Info"]
                    stages.append({
                        "stage_id": info["Stage ID"],
                        "attempt": info["Stage Attempt ID"],
                        "name": info["Stage Name"],
                        "num_tasks": info["Number of Tasks"],
                        "submission_ms": info.get("Submission Time"),
                        "completion_ms": info.get("Completion Time"),
                        "failure": info.get("Failure Reason"),
                    })
                elif '"SparkListenerTaskEnd"' in line:
                    ev = json.loads(line)
                    if ev["Task End Reason"]["Reason"] != "Success":
                        continue
                    ti, tm = ev["Task Info"], ev.get("Task Metrics") or {}
                    sr = tm.get("Shuffle Read Metrics", {})
                    tasks.append({
                        "stage_id": ev["Stage ID"],
                        "task_id": ti["Task ID"],
                        "host": ti["Host"],
                        "launch_ms": ti["Launch Time"],
                        "finish_ms": ti["Finish Time"],
                        "executor_run_time_ms": tm.get("Executor Run Time"),
                        "memory_spilled": tm.get("Memory Bytes Spilled"),
                        "disk_spilled": tm.get("Disk Bytes Spilled"),
                        "peak_execution_memory": tm.get("Peak Execution Memory"),
                        "shuffle_read_bytes": (sr.get("Remote Bytes Read", 0)
                                               + sr.get("Local Bytes Read", 0)),
                    })
    return {"stages": stages, "tasks": tasks}


if __name__ == "__main__":
    json.dump({app: extract(app) for app in sys.argv[1:]}, sys.stdout)
