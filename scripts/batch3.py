"""Batch 3: experiment C2 (compression dose-response), rule check, experiment D.

Phases (all resumable):
  1. generate datasets (skipped when they exist)
  2. ratio calibration: one AQE-off application (1 run, no warm-up) per
     dataset/codec/partitions that a rule-2 mode needs; the measured overall
     shuffle compression ratio r is stored in results/batch3/ratios.json
  3. rounds: each round runs every (point, mode) once in a randomized order
     (seeded by the round number) as one application with 1 warm-up and 2
     measured runs. Experiment D points run in the first D_ROUNDS rounds only.

A finished job leaves results/batch3/done/<job_id>; on restart finished jobs
are skipped. Spark output goes to results/batch3/logs/. Start or resume with
scripts/batch3.sh (runs this under caffeinate).
"""

import json
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch3"
ROUNDS = 4
D_ROUNDS = 4
WARMUP, RUNS = 1, 2

# payload_random_fraction per target lz4 compression ratio (calibrated, see report)
LEVELS = {"1x": 1.0, "2x": 0.53, "4x": 0.19, "8x": 0.04, "16x": 0.0}
D_ROWS = 3_000_000

RULE2_MODES = {"aqe_on_rule2", "aqe_on_rules12"}
RULE1_MODES = {"aqe_on_rule1", "aqe_on_rules12"}

# (experiment, params, modes)
POINTS = []
C2_MODES = ["aqe_off", "aqe_on", "aqe_on_nocoalesce", "aqe_on_rule2"]
for level, fraction in LEVELS.items():
    for codec in ("lz4", "zstd"):
        if codec == "zstd" and level not in ("1x", "4x", "16x"):
            continue
        POINTS.append(("exp_c2_dose", {"payload_random_fraction": fraction, "codec": codec},
                       C2_MODES))
POINTS.append(("exp_rules", {"payload_random_fraction": LEVELS["8x"]},
               ["aqe_off", "aqe_on", "aqe_on_rule1", "aqe_on_rule2", "aqe_on_rules12"]))
for n in (5, 20):
    for join in ("inner", "left"):
        POINTS.append(("exp_d_pipeline", {"n_dims": n, "join": join, "rows": D_ROWS},
                       ["aqe_off", "aqe_on", "aqe_on_hints"]))
POINTS.append(("exp_d_pipeline", {"n_dims": 5, "join": "left", "rows": D_ROWS,
                                  "null_share": 0.2, "payload_kind": "mixed",
                                  "payload_bytes": 100,
                                  "payload_random_fraction": LEVELS["8x"],
                                  "shuffle_partitions": 2001},
               ["aqe_off", "aqe_on", "aqe_on_rules12"]))


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
        items = [(e, p, m) for e, p, modes in POINTS for m in modes
                 if e != "exp_d_pipeline" or rnd <= D_ROUNDS]
        random.Random(1000 + rnd).shuffle(items)
        for order, (experiment, params, mode) in enumerate(items):
            jobs.append({
                "job_id": "r{}_{:03d}_{}_{}".format(rnd, order, point_key(experiment, params), mode),
                "round": rnd, "experiment": experiment, "params": params, "mode": mode,
            })
    return jobs


def submit(args, log_path):
    cmd = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
           "/opt/project/jobs/run_experiment.py"] + args
    with open(log_path, "w") as log:
        return subprocess.run(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT).returncode


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def read_ratio(csv_path):
    import csv
    with open(csv_path) as f:
        rows = list(csv.DictReader(f))
    row = rows[-1]
    return float(row["compression_ratio_overall"]), float(row["compression_ratio_facts"])


def calibrate_ratios():
    path = OUT / "ratios.json"
    ratios = json.loads(path.read_text()) if path.exists() else {}
    for experiment, params, modes in POINTS:
        if not RULE2_MODES & set(modes):
            continue
        key = point_key(experiment, params)
        if key in ratios:
            continue
        csv_path = OUT / "calibration" / (key + ".csv")
        if csv_path.exists():
            csv_path.unlink()
        rc = submit(["--experiment", experiment, "--mode", "aqe_off", "--warmup", "0",
                     "--runs", "1", "--job-id", "calib_" + key,
                     "--csv", "/opt/project/results/batch3/calibration/{}.csv".format(key)]
                    + param_args(params), OUT / "logs" / "calib_{}.log".format(key))
        if rc != 0:
            sys.exit("ratio calibration failed: " + key)
        overall, facts = read_ratio(csv_path)
        ratios[key] = {"r_overall": overall, "r_facts": facts}
        path.write_text(json.dumps(ratios, indent=1, sort_keys=True))
        log("ratio {} r_overall={:.3f} r_facts={:.3f}".format(key, overall, facts))
    return ratios


def mode_params(job, ratios):
    params = dict(job["params"])
    if job["mode"] in RULE1_MODES:
        params["accurate_skew_factor"] = 5.0
    if job["mode"] in RULE2_MODES:
        params["rule2_ratio"] = round(ratios[point_key(job["experiment"], job["params"])]["r_overall"], 4)
    return params


def main():
    for sub in ("done", "logs", "jobs", "calibration"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)

    plan_path = OUT / "plan.json"
    if plan_path.exists():
        jobs = json.loads(plan_path.read_text())
    else:
        jobs = build_plan()
        plan_path.write_text(json.dumps(jobs, indent=1))

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

    ratios = calibrate_ratios()

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
                "--csv", "/opt/project/results/batch3/jobs/{}.csv".format(job["job_id"])]
        rc = submit(args + param_args(mode_params(job, ratios)),
                    OUT / "logs" / (job["job_id"] + ".log"))
        log("[{}/{}] {} rc={} {:.0f}s".format(i, len(todo), job["job_id"], rc, time.time() - start))
        if rc == 0 and csv_path.exists():
            (OUT / "done" / job["job_id"]).touch()
        else:
            failed.append(job["job_id"])
    log("finished, failed jobs: {}".format(failed or "none"))


if __name__ == "__main__":
    main()
