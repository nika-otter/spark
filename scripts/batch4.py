"""Batch 4: corrected experiment D, data volume and skew source, Spark 3.5 replication.

Steps run strictly in order (step 3 last, on the Spark 3.5 cluster):
  step 1  exp_d2_inner (INNER pipelines, aligned filters), exp_d2_stress
  step 2  exp_e_volume, exp_e_coalesce, exp_f_hotkey (4.1.3)
  step 3  exp_b_partitions, exp_c2_dose, exp_rules on Spark 3.5.9
Datasets of all steps are generated first, on the 4.1.3 cluster (existing
ones are skipped; results/batch4/generated_s<step> marks a finished step).
Each step then: (1) ratio calibration for rule-2 modes: one AQE-off application
(1 run, no warm-up) per point on the step's cluster, r = overall shuffle
compression ratio, stored in results/batch4/ratios.json, (2) rounds: each
round runs every (point, mode) of the step once in a randomized order (seeded
by step and round) as one application with 1 warm-up and 2 measured runs.

A finished job leaves results/batch4/done/<job_id>; on restart finished jobs
are skipped. Spark output goes to results/batch4/logs/. The script switches
between the clusters itself (only one runs at a time). Start or resume with
scripts/batch4.sh (runs this under caffeinate).
"""

import json
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch4"
WARMUP, RUNS = 1, 2
ROUNDS = {1: 4, 2: 4, 3: 4}

CLUSTERS = {
    "4.1.3": {"compose": [], "master": "spark-master"},
    "3.5.9": {"compose": ["-f", "docker-compose.spark35.yml"], "master": "spark35-master"},
}
STEP_CLUSTER = {1: "4.1.3", 2: "4.1.3", 3: "3.5.9"}

# payload_random_fraction per lz4 compression level (calibrated in batch 3)
LEVELS = {"1x": 1.0, "4x": 0.19, "8x": 0.04, "16x": 0.0}
D_ROWS = 3_000_000
# facts rows -> Parquet files (proportional to rows)
VOLUMES = {27_500_000: 22, 55_000_000: 44, 110_000_000: 88}

RULE2_MODES = {"aqe_on_rule2", "aqe_on_rules12"}
RULE1_MODES = {"aqe_on_rule1", "aqe_on_rules12"}
ALL_FIVE = ["aqe_off", "aqe_on", "aqe_on_rule1", "aqe_on_rule2", "aqe_on_rules12"]

# step -> [(experiment, params, modes)]
POINTS = {1: [], 2: [], 3: []}
for n in (5, 20):
    POINTS[1].append(("exp_d2_inner", {"n_dims": n, "rows": D_ROWS},
                      ["aqe_off", "aqe_on", "aqe_on_hints"]))
POINTS[1].append(("exp_d2_stress", {"rows": D_ROWS, "payload_bytes": 400,
                                    "payload_random_fraction": LEVELS["8x"]}, ALL_FIVE))

for rows, files in VOLUMES.items():
    vol = {"rows": rows, "num_files": files}
    POINTS[2].append(("exp_e_volume", dict(vol, payload_random_fraction=LEVELS["1x"],
                                           shuffle_partitions=200), ["aqe_off", "aqe_on"]))
    POINTS[2].append(("exp_e_volume", dict(vol, payload_random_fraction=LEVELS["8x"],
                                           shuffle_partitions=2001),
                      ["aqe_off", "aqe_on", "aqe_on_rules12"]))
    POINTS[2].append(("exp_e_coalesce", dict(vol, payload_random_fraction=LEVELS["8x"],
                                             shuffle_partitions=2001),
                      ["aqe_off", "aqe_on", "aqe_on_rule2"]))
HOT = {"rows": 55_000_000, "num_files": VOLUMES[55_000_000]}
for join in ("inner", "left"):
    POINTS[2].append(("exp_f_hotkey", dict(HOT, join=join, payload_random_fraction=LEVELS["1x"],
                                           shuffle_partitions=200), ["aqe_off", "aqe_on"]))
    POINTS[2].append(("exp_f_hotkey", dict(HOT, join=join, payload_random_fraction=LEVELS["8x"],
                                           shuffle_partitions=2001),
                      ["aqe_off", "aqe_on", "aqe_on_rules12"]))
POINTS[2].append(("exp_f_hotkey", dict(HOT, join="left", hot_keys=5,
                                       payload_random_fraction=LEVELS["8x"],
                                       shuffle_partitions=2001),
                  ["aqe_off", "aqe_on", "aqe_on_rules12"]))

for sp, modes in ((200, ["aqe_on", "aqe_off"]), (2000, ["aqe_on"]), (2001, ["aqe_on", "aqe_off"])):
    POINTS[3].append(("exp_b_partitions", {"shuffle_partitions": sp}, modes))
POINTS[3].append(("exp_b_partitions", {"shuffle_partitions": 2001, "accurate_skew_factor": 5.0},
                  ["aqe_on"]))
for level in ("1x", "4x", "16x"):
    POINTS[3].append(("exp_c2_dose", {"payload_random_fraction": LEVELS[level], "codec": "lz4"},
                      ["aqe_off", "aqe_on", "aqe_on_rule2"]))
POINTS[3].append(("exp_rules", {"payload_random_fraction": LEVELS["8x"]}, ALL_FIVE))


def param_args(params):
    args = []
    for key, value in sorted(params.items()):
        args += ["--param", "{}={}".format(key, value)]
    return args


def point_key(experiment, params):
    return experiment + "".join("_{}-{}".format(k, v) for k, v in sorted(params.items()))


def ratio_key(step, experiment, params):
    return "s{}_{}".format(step, point_key(experiment, params))


def build_plan():
    jobs = []
    for step in (1, 2, 3):
        for rnd in range(1, ROUNDS[step] + 1):
            items = [(e, p, m) for e, p, modes in POINTS[step] for m in modes]
            random.Random(4000 + 100 * step + rnd).shuffle(items)
            for order, (experiment, params, mode) in enumerate(items):
                jobs.append({
                    "job_id": "s{}_r{}_{:03d}_{}_{}".format(
                        step, rnd, order, point_key(experiment, params), mode),
                    "step": step, "round": rnd, "experiment": experiment,
                    "params": params, "mode": mode, "cluster": STEP_CLUSTER[step],
                })
    return jobs


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def compose(cluster, *args, **kwargs):
    cmd = ["docker", "compose"] + CLUSTERS[cluster]["compose"] + list(args)
    return subprocess.run(cmd, cwd=ROOT, **kwargs)


def is_running(container):
    r = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", container],
                       capture_output=True, text=True)
    return r.returncode == 0 and r.stdout.strip() == "true"


def ensure_cluster(cluster):
    other = [c for c in CLUSTERS if c != cluster][0]
    if is_running(CLUSTERS[cluster]["master"]) and not is_running(CLUSTERS[other]["master"]):
        return
    with open(OUT / "logs" / "cluster_switch.log", "a") as f:
        compose(other, "down", stdout=f, stderr=subprocess.STDOUT)
        rc = compose(cluster, "up", "-d", "--wait", stdout=f, stderr=subprocess.STDOUT).returncode
    if rc != 0:
        sys.exit("cluster {} failed to start".format(cluster))
    time.sleep(10)  # let the workers register with the master
    log("cluster {} up".format(cluster))


def submit(cluster, args, log_path):
    cmd = ["exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
           "/opt/project/jobs/run_experiment.py"] + args
    with open(log_path, "w") as f:
        return compose(cluster, *cmd, stdout=f, stderr=subprocess.STDOUT).returncode


def generate(step):
    marker = OUT / "generated_s{}".format(step)
    if marker.exists():
        return
    ensure_cluster("4.1.3")
    seen = set()
    for experiment, params, _ in POINTS[step]:
        key = point_key(experiment, params)
        if key in seen:
            continue
        seen.add(key)
        rc = submit("4.1.3", ["--experiment", experiment, "--generate-only"] + param_args(params),
                    OUT / "logs" / "generate_{}.log".format(key))
        log("generate {} rc={}".format(key, rc))
        if rc != 0:
            sys.exit("dataset generation failed: " + key)
    marker.touch()


def read_ratio(csv_path):
    import csv
    with open(csv_path) as f:
        row = list(csv.DictReader(f))[-1]
    return float(row["compression_ratio_overall"]), float(row["compression_ratio_facts"])


def calibrate_ratios(step):
    path = OUT / "ratios.json"
    ratios = json.loads(path.read_text()) if path.exists() else {}
    for experiment, params, modes in POINTS[step]:
        if not RULE2_MODES & set(modes):
            continue
        key = ratio_key(step, experiment, params)
        if key in ratios:
            continue
        ensure_cluster(STEP_CLUSTER[step])
        csv_path = OUT / "calibration" / (key + ".csv")
        if csv_path.exists():
            csv_path.unlink()
        rc = submit(STEP_CLUSTER[step],
                    ["--experiment", experiment, "--mode", "aqe_off", "--warmup", "0",
                     "--runs", "1", "--job-id", "calib_" + key,
                     "--csv", "/opt/project/results/batch4/calibration/{}.csv".format(key)]
                    + param_args(params), OUT / "logs" / "calib_{}.log".format(key))
        if rc != 0:
            sys.exit("ratio calibration failed: " + key)
        overall, facts = read_ratio(csv_path)
        ratios[key] = {"r_overall": overall, "r_facts": facts, "cluster": STEP_CLUSTER[step]}
        path.write_text(json.dumps(ratios, indent=1, sort_keys=True))
        log("ratio {} r_overall={:.3f} r_facts={:.3f}".format(key, overall, facts))
    return ratios


def mode_params(job, ratios):
    params = dict(job["params"])
    if job["mode"] in RULE1_MODES:
        params["accurate_skew_factor"] = 5.0
    if job["mode"] in RULE2_MODES:
        key = ratio_key(job["step"], job["experiment"], job["params"])
        params["rule2_ratio"] = round(ratios[key]["r_overall"], 4)
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

    for step in (1, 2, 3):
        generate(step)
    failed = []
    for step in (1, 2, 3):
        todo = [j for j in jobs if j["step"] == step and not (OUT / "done" / j["job_id"]).exists()]
        if not todo:
            continue
        log("step {}: {} of {} jobs to run".format(
            step, len(todo), sum(1 for j in jobs if j["step"] == step)))
        ratios = calibrate_ratios(step)
        ensure_cluster(STEP_CLUSTER[step])
        for i, job in enumerate(todo, 1):
            csv_path = OUT / "jobs" / (job["job_id"] + ".csv")
            if csv_path.exists():
                csv_path.unlink()
            start = time.time()
            args = ["--experiment", job["experiment"], "--mode", job["mode"],
                    "--warmup", str(WARMUP), "--runs", str(RUNS), "--job-id", job["job_id"],
                    "--csv", "/opt/project/results/batch4/jobs/{}.csv".format(job["job_id"])]
            rc = submit(job["cluster"], args + param_args(mode_params(job, ratios)),
                        OUT / "logs" / (job["job_id"] + ".log"))
            log("[s{} {}/{}] {} rc={} {:.0f}s".format(
                step, i, len(todo), job["job_id"], rc, time.time() - start))
            if rc == 0 and csv_path.exists():
                (OUT / "done" / job["job_id"]).touch()
            else:
                failed.append(job["job_id"])
    log("finished, failed jobs: {}".format(failed or "none"))


if __name__ == "__main__":
    main()
