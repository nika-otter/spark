"""Batch 7 (Spark 4.1.3): three gap-filling measurements.

  step 1  experiment B (exp_b_partitions, LEFT, 55M rows, 20 % NULL): AQE off at
          1000 and 2000 shuffle partitions (new) + anchors AQE off at 200 and 2001
  step 2  pipelines: INNER n=5, n=20 (exp_d2_inner, batch 4) and LEFT n=5, n=20
          (exp_d_pipeline, batch 3), 3M facts rows: mode aqe_off_hints (new: AQE off
          with the broadcast hints of aqe_on_hints) + anchor aqe_off
  step 3  TPC-DS SF10, all 103 queries, one profiling run each with AQE on and
          defaults, codec zstd (the lz4 profile is batch 5a)

  python scripts/batch7.py validate   step 0: dataset check, one trial per new point
                                      (B at 1000 / 2000 with AQE off; the four
                                      pipelines with aqe_off_hints; TPC-DS q3 with
                                      zstd); writes results/batch7/validation.json
  python scripts/batch7.py run        steps 1 and 2: 4 rounds, each round runs every
                                      (point, mode) once in a randomized order as one
                                      application with 1 warm-up + 2 measured runs;
                                      then step 3

Every job: scripts/cluster_check.py before and after (AC power, containers, 3
alive workers; the batch stops otherwise) and, after the job, the History Server
must list exactly 3 executors on 3 hosts with no remove reason for every
application of the job, and pmset must show no host sleep during the job. A job
that fails these checks is moved to
results/batch7_invalid/ and rerun. Resumable (done markers in results/batch7/done).
Start or resume with scripts/batch7.sh MODE (runs this under caffeinate).
"""

import csv
import json
import os
import random
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import cluster_check

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch7"
INVALID = ROOT / "results" / "batch7_invalid"
HS = "http://localhost:18080/api/v1/applications/"
ROUNDS = 4
TPCDS = sorted(p.stem for p in (ROOT / "bench" / "tpcds").glob("q*.sql"))

B_POINTS = [(sp, "aqe_off") for sp in (1000, 2000, 200, 2001)]
PIPELINES = [("exp_d2_inner", {"n_dims": 5, "rows": 3_000_000}, "inner_n5"),
             ("exp_d2_inner", {"n_dims": 20, "rows": 3_000_000}, "inner_n20"),
             ("exp_d_pipeline", {"n_dims": 5, "join": "left", "rows": 3_000_000}, "left_n5"),
             ("exp_d_pipeline", {"n_dims": 20, "join": "left", "rows": 3_000_000}, "left_n20")]


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def submit(script, args, log_path):
    cmd = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
           "/opt/project/jobs/" + script] + args
    with open(log_path, "w") as f:
        return subprocess.run(cmd, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT).returncode


def param_args(params):
    args = []
    for key, value in sorted(params.items()):
        args += ["--param", "{}={}".format(key, value)]
    return args


def items():
    out = [("B_sp{}_{}".format(sp, mode), "run_experiment.py",
            ["--experiment", "exp_b_partitions", "--mode", mode] + param_args({"shuffle_partitions": sp}))
           for sp, mode in B_POINTS]
    for experiment, params, label in PIPELINES:
        for mode in ("aqe_off_hints", "aqe_off"):
            out.append(("{}_{}".format(label, mode), "run_experiment.py",
                        ["--experiment", experiment, "--mode", mode] + param_args(params)))
    return out


def executors_ok(app_id, wait_s=60):
    """3 executors on 3 hosts, none removed with a reason (History Server)."""
    deadline = time.time() + wait_s
    while True:
        try:
            with urllib.request.urlopen(HS + app_id + "/allexecutors", timeout=10) as r:
                ex = [e for e in json.loads(r.read()) if e["id"] != "driver"]
            hosts = {e["hostPort"].split(":")[0] for e in ex}
            removed = [e["id"] for e in ex if e.get("removeReason")]
            return len(ex) == 3 and len(hosts) == 3 and not removed, \
                "{} executors on {} hosts, removed {}".format(len(ex), len(hosts), removed)
        except Exception as exc:  # app not yet loaded by the History Server
            if time.time() > deadline:
                return False, "history server: {}".format(exc)
            time.sleep(5)


def slept_between(start, end):
    """Sleep events of the host in [start, end] (epoch seconds), from pmset -g log."""
    out = subprocess.run(["pmset", "-g", "log"], capture_output=True, text=True).stdout
    events = []
    for line in out.splitlines():
        if "Entering Sleep" not in line:
            continue
        try:
            t = time.mktime(time.strptime(line[:19], "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            continue
        if start <= t <= end:
            events.append(line[:19])
    return events


def quarantine(job_id, csv_path):
    for sub in ("jobs", "runs", "logs"):
        (INVALID / sub).mkdir(parents=True, exist_ok=True)
    if csv_path.exists():
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                src = os.path.normpath(os.path.join(OUT / "jobs", row["json_path"]))
                if os.path.exists(src):
                    shutil.move(src, INVALID / "runs" / os.path.basename(src))
        shutil.move(str(csv_path), INVALID / "jobs" / (job_id + "_" + time.strftime("%H%M%S") + ".csv"))
    log_path = OUT / "logs" / (job_id + ".log")
    if log_path.exists():
        shutil.move(str(log_path), INVALID / "logs" / (job_id + "_" + time.strftime("%H%M%S") + ".log"))


def run_job(job_id, script, args, attempts=3):
    csv_path = OUT / "jobs" / (job_id + ".csv")
    if (OUT / "done" / job_id).exists():
        return csv_path
    for attempt in range(1, attempts + 1):
        if csv_path.exists():
            csv_path.unlink()
        problems = cluster_check.check()
        if problems:
            log("cluster check failed before {}: {}".format(job_id, "; ".join(problems)))
            sys.exit("unhealthy cluster")
        start = time.time()
        rc = submit(script, args + ["--job-id", job_id, "--csv",
                                    "/opt/project/results/batch7/jobs/" + csv_path.name],
                    OUT / "logs" / (job_id + ".log"))
        log("{} rc={} {:.0f}s".format(job_id, rc, time.time() - start))
        problems = cluster_check.check()
        if problems:
            log("cluster check failed after {}: {}".format(job_id, "; ".join(problems)))
            quarantine(job_id, csv_path)
            sys.exit("unhealthy cluster")
        if rc != 0 or not csv_path.exists():
            return None
        sleeps = slept_between(start, time.time())
        if sleeps:
            log("{} invalid (attempt {}): host sleep at {}; moved to {}, rerunning".format(
                job_id, attempt, sleeps, INVALID))
            quarantine(job_id, csv_path)
            continue
        with open(csv_path) as f:
            apps = sorted({row["app_id"] for row in csv.DictReader(f)})
        checks = [executors_ok(a) for a in apps]
        if all(ok for ok, _ in checks):
            (OUT / "done" / job_id).touch()
            return csv_path
        log("{} invalid (attempt {}): {}; moved to {}, rerunning".format(
            job_id, attempt, [m for _, m in checks], INVALID))
        quarantine(job_id, csv_path)
    sys.exit("job kept failing the executor check: " + job_id)


def validate():
    result = {}
    # 1. datasets (generate-only skips existing datasets and says so in its log)
    for experiment, params, label in [("exp_b_partitions", {"shuffle_partitions": 200}, "B")] + PIPELINES:
        log_path = OUT / "logs" / "check_datasets_{}.log".format(label)
        rc = submit("run_experiment.py", ["--experiment", experiment, "--generate-only"] + param_args(params),
                    log_path)
        text = log_path.read_text()
        result["datasets_" + label] = {"rc": rc, "existing": text.count("exists, skipping"),
                                       "generated": text.count('"write_seconds"')}
    r = subprocess.run(["docker", "compose", "exec", "-T", "spark-master", "bash", "-c",
                        "ls /opt/spark-data/tpcds_sf10 && cat /opt/spark-data/tpcds_sf10/manifest.json"],
                       cwd=ROOT, capture_output=True, text=True)
    result["tpcds_sf10"] = {"listing": r.stdout.split("{")[0].split(), "manifest_ok": '"tables"' in r.stdout}
    # 2-4. trials
    trials = [("trial_B_sp{}".format(sp), "run_experiment.py",
               ["--experiment", "exp_b_partitions", "--mode", "aqe_off", "--warmup", "0", "--runs", "1"]
               + param_args({"shuffle_partitions": sp})) for sp in (1000, 2000)]
    trials += [("trial_{}_aqe_off_hints".format(label), "run_experiment.py",
                ["--experiment", experiment, "--mode", "aqe_off_hints", "--warmup", "0", "--runs", "1"]
                + param_args(params)) for experiment, params, label in PIPELINES]
    trials.append(("trial_tpcds_q3_zstd", "run_bench.py",
                   ["--bench", "tpcds", "--query", "q3", "--profile", "--codec", "zstd"]))
    for job_id, script, args in trials:
        path = run_job(job_id, script, args)
        if path is None:
            sys.exit("trial failed: " + job_id)
        with open(path) as f:
            row = list(csv.DictReader(f))[-1]
        rec = json.loads((OUT / "jobs" / row["json_path"]).resolve().read_text())
        entry = {"app_id": row["app_id"], "seconds": float(row["elapsed_seconds"]),
                 "join_strategies": json.loads(row["join_strategies"]),
                 "join_stage_tasks": row["join_stage_tasks"],
                 "codec_effective_conf": rec["conf"].get("spark.io.compression.codec"),
                 "adaptive_enabled_conf": rec["conf"].get("spark.sql.adaptive.enabled"),
                 "final_plan": rec["metrics"]["final_physical_plan"]}
        with urllib.request.urlopen(HS + row["app_id"] + "/environment", timeout=30) as r:
            env = json.loads(r.read())
        entry["codec_event_log"] = dict(env.get("sparkProperties", [])).get("spark.io.compression.codec")
        result[job_id] = entry
    (OUT / "validation.json").write_text(json.dumps(result, indent=1))
    log("validation written")


def run():
    for rnd in range(1, ROUNDS + 1):
        if not cluster_check.on_ac_power():
            sys.exit("refusing to start round {}: not on AC power".format(rnd))
        todo = items()
        random.Random("7-{}".format(rnd)).shuffle(todo)
        for order, (label, script, args) in enumerate(todo):
            run_job("r{}_{:02d}_{}".format(rnd, order, label), script,
                    args + ["--warmup", "1", "--runs", "2"])
    if not cluster_check.on_ac_power():
        sys.exit("refusing to start step 3: not on AC power")
    for q in TPCDS:
        run_job("3_profile_tpcds_{}_zstd".format(q), "run_bench.py",
                ["--bench", "tpcds", "--query", q, "--profile", "--codec", "zstd"])
    log("finished")


def main():
    for sub in ("done", "logs", "jobs"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)
    if not cluster_check.on_ac_power():
        sys.exit("refusing to start: not on AC power")
    {"validate": validate, "run": run}[sys.argv[1]]()


if __name__ == "__main__":
    main()
