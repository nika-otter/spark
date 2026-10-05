"""Batch 2: INNER JOIN check, experiments A, B and C, in randomized rounds.

Protocol: each round runs every (scenario, mode) once, in a randomized order
(seeded by the round number, so a resumed batch keeps the same order), as one
Spark application with 1 warm-up and 2 measured runs. 4 rounds.

Resumable: a finished job leaves results/batch2/done/<job_id>; on restart
finished jobs are skipped and unfinished ones are rerun from scratch (their
per-job CSV is replaced). Spark output goes to results/batch2/logs/<job_id>.log.

Start or resume with scripts/batch2.sh (runs this under caffeinate).
"""

import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch2"
ROUNDS = 4
WARMUP, RUNS = 1, 2

# (experiment, params, modes)
POINTS = []
for ns in (0, 0.12, 0.5):
    POINTS.append(("inner_check", {"null_share": ns}, ["aqe_off", "aqe_on", "aqe_on_nocoalesce"]))
for ns in (0, 0.05, 0.10, 0.15, 0.20, 0.30):
    POINTS.append(("exp_a_small", {"null_share": ns}, ["aqe_off", "aqe_on", "aqe_on_skew64m"]))
POINTS.append(("exp_a_small", {"rows": 55_000_000, "null_share": 0.03}, ["aqe_off", "aqe_on"]))
for sp in (200, 1000, 2000, 2001, 4000):
    modes = ["aqe_on"] + (["aqe_off"] if sp in (200, 2001) else [])
    POINTS.append(("exp_b_partitions", {"shuffle_partitions": sp}, modes))
for sp in (2001, 4000):
    POINTS.append(("exp_b_partitions", {"shuffle_partitions": sp, "accurate_skew_factor": 5.0},
                   ["aqe_on"]))
for kind in ("random", "repetitive"):
    for codec in ("lz4", "zstd"):
        POINTS.append(("exp_c_compression", {"payload_kind": kind, "codec": codec},
                       ["aqe_off", "aqe_on"]))


def param_args(params):
    args = []
    for key, value in sorted(params.items()):
        args += ["--param", "{}={}".format(key, value)]
    return args


def point_key(experiment, params):
    return experiment + "".join("_{}-{}".format(k, v) for k, v in sorted(params.items()))


def build_plan():
    jobs = []
    for rnd in range(1, ROUNDS + 1):
        items = [(e, p, m) for e, p, modes in POINTS for m in modes]
        random.Random(rnd).shuffle(items)
        for order, (experiment, params, mode) in enumerate(items):
            jobs.append({
                "job_id": "r{}_{:03d}_{}_{}".format(rnd, order, point_key(experiment, params), mode),
                "round": rnd,
                "experiment": experiment,
                "params": params,
                "mode": mode,
            })
    return jobs


def submit(args, log_path):
    cmd = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
           "/opt/project/jobs/run_experiment.py"] + args
    with open(log_path, "w") as log:
        return subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT).returncode


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def main():
    for sub in ("done", "logs", "jobs"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)

    plan_path = OUT / "plan.json"
    if plan_path.exists():
        jobs = json.loads(plan_path.read_text())
    else:
        jobs = build_plan()
        plan_path.write_text(json.dumps(jobs, indent=1))

    # Datasets first (skipped when they exist).
    seen = set()
    for experiment, params, _ in POINTS:
        key = point_key(experiment, params)
        if key in seen:
            continue
        seen.add(key)
        rc = submit(["--experiment", experiment, "--generate-only"] + param_args(params),
                    OUT / "logs" / "generate_{}.log".format(key))
        log("generate {} rc={}".format(key, rc))
        if rc != 0:
            sys.exit("dataset generation failed: " + key)

    todo = [j for j in jobs if not (OUT / "done" / j["job_id"]).exists()]
    log("{} of {} jobs to run".format(len(todo), len(jobs)))
    failed = []
    for i, job in enumerate(todo, 1):
        csv_path = OUT / "jobs" / (job["job_id"] + ".csv")
        if csv_path.exists():
            csv_path.unlink()
        start = time.time()
        args = ["--experiment", job["experiment"], "--mode", job["mode"],
                "--warmup", str(WARMUP), "--runs", str(RUNS), "--job-id", job["job_id"],
                "--csv", "/opt/project/results/batch2/jobs/{}.csv".format(job["job_id"])]
        rc = submit(args + param_args(job["params"]), OUT / "logs" / (job["job_id"] + ".log"))
        log("[{}/{}] {} rc={} {:.0f}s".format(i, len(todo), job["job_id"], rc, time.time() - start))
        if rc == 0 and csv_path.exists():
            (OUT / "done" / job["job_id"]).touch()
        else:
            failed.append(job["job_id"])
    log("finished, failed jobs: {}".format(failed or "none"))


if __name__ == "__main__":
    main()
