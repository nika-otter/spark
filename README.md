# Spark AQE experiments — local cluster

Spark standalone cluster in Docker for experiments with Adaptive Query Execution.

- Image: `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` (native `linux/arm64`)
- 1 master, 3 workers (3 cores / 6 GB container limit / 4 GB executor heap each), History Server
- Driver runs in client mode inside the master container (`spark.driver.memory=1g`)

## Requirements

Docker Desktop with **10 CPUs** and **22 GB** memory (Settings → Resources).

## Commands

| Command | What it does |
|---|---|
| `make up` | Start the cluster |
| `make down` | Stop the cluster (volumes with data and event logs are kept) |
| `make status` | Containers and their resource usage |
| `make logs` | Follow cluster logs |
| `make submit JOB=jobs/x.py ARGS="..."` | Run a PySpark job with spark-submit in the master container |
| `make smoke-generate` | Write smoke-test tables (10M and 1M rows) as Parquet |
| `make smoke` | Run the smoke join with AQE off, then on |
| `make history-check` | List applications known to the History Server |
| `make venv` | Create the project-local `.venv` used by `analysis/` |
| `make pilot-null` | Generate datasets and run the NULL-share pilot grid |
| `make pilot-null-summary` | Summary CSV and plot for the pilot |
| `scripts/batch2.sh` | Start or resume batch 2 (INNER check, experiments A/B/C) in the background under caffeinate |
| `.venv/bin/python analysis/batch2.py` | Batch 2 summaries (`results/*_summary.csv`) and plots |
| `.venv/bin/python analysis/pilot_anomalies.py` | Pilot anomaly checks from event logs and executor logs |
| `scripts/batch3.sh` | Start or resume batch 3 (experiment C2, rule check, experiment D) under caffeinate |
| `.venv/bin/python analysis/batch3.py` | Batch 3 summaries (`results/exp_c2_summary.csv`, `exp_rules_summary.csv`, `exp_d_summary.csv`, `exp_d_plans/`) |
| `scripts/batch4.sh` | Start or resume batch 4 (corrected D, data volume and skew source, Spark 3.5.9 replication) under caffeinate; switches between the two clusters itself |
| `.venv/bin/python analysis/batch4.py` | Batch 4 summaries (`results/exp_d2_summary.csv`, `exp_e_summary.csv`, `exp_f_summary.csv`, `spark35_summary.csv`, `exp_d2_plans/`) |
| `.venv/bin/python analysis/step4_null_task.py DIR [STDERR_DIR]` | Batch 4 step 4: NULL-partition task of the batch-3 combined check from copied event/executor logs |
| `.venv/bin/python scripts/gen_tpc.py tpch\|tpcds 10` | TPC-H / TPC-DS SF10 with DuckDB into `data/` (then `docker cp` into `/opt/spark-data/`) |
| `scripts/download_real.sh` | Download the three real datasets of batch 5 into `data/raw/` |
| `scripts/batch5a.sh` | Batch 5a: TPC-H/TPC-DS profiles and timing, rule 2 vs alternatives, rule-2 refinements |
| `scripts/batch5b.sh prepare\|rounds` | Batch 5b: load real datasets, profile, then 4 rounds |
| `.venv/bin/python analysis/batch5.py` | Batch 5a summaries; `analysis/recover_k2.py` k2-join metrics of step 3a from the History Server |
| `.venv/bin/python scripts/aqe_advisor.py --app-id ID \| --eventlog PATH` | Recommended AQE settings (rules 1 and 2) from a previous run |
| `scripts/batch5c.sh prepare\|rounds` | Batch 5c: key tables, validation trials, then rounds (steps 2 and 3); summaries with `analysis/batch5c.py` |
| `.venv/bin/python analysis/figures.py` | Thesis figures (Ukrainian) in `results/figures/` as PNG 300 dpi and PDF |

To delete all data and event logs: `docker compose down -v`.

## UIs

- Master: http://localhost:8080
- Workers: http://localhost:8081, :8082, :8083
- Running application: http://localhost:4040
- History Server: http://localhost:18080

## Layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Cluster definition, resource limits, volumes |
| `conf/spark-defaults.conf` | Infrastructure settings (master URL, executor sizing, event log) |
| `jobs/spark_conf.py` | Baseline experiment config (shuffle partitions, AQE, broadcast threshold, CBO off) and effective-config dump |
| `jobs/generate.py` | Deterministic data generator (facts, dim) with per-dataset manifest |
| `jobs/metrics.py` | Per-run metrics from the driver REST API -> JSON + `results/runs.csv` |
| `jobs/experiments.py` | Experiment definitions (datasets, query, conf overrides) |
| `jobs/run_experiment.py` | One app per (scenario, mode): 1 warm-up + 5 measured runs to noop |
| `jobs/smoke_*.py` | Smoke test |
| `scripts/run_pilot_null.sh` | Pilot grid driver |
| `analysis/pilot_null.py` | Pilot summary and plot (runs on the host in `.venv`) |
| `results/runs.csv` | One row per run; full record in `results/runs/<experiment>/<scenario>/<mode>/*.json` |
| `results/calibration/` | Calibration runs for the pilot |
| `scripts/batch2.py` | Batch 2 plan: 4 rounds, randomized order, 1 warm-up + 2 measured runs per app; resumable |
| `results/batch2/` | Batch 2 plan, per-job CSVs, done markers (logs are not committed) |
| `scripts/batch3.py` | Batch 3 plan: generation, rule-2 ratio calibration, 4 randomized rounds |
| `results/batch3/` | Batch 3 plan, ratios.json, calibration CSVs, per-job CSVs, done markers |
| `docker-compose.spark35.yml` | Spark 3.5.9 replica of the cluster (project `spark35`, shares the data volume); only one cluster runs at a time |
| `scripts/batch4.py` | Batch 4 plan: generation, rule-2 ratio calibration per step, 4 randomized rounds per step, cluster switching |
| `results/batch4/` | Batch 4 plan, ratios.json, calibration CSVs, per-job CSVs, done markers |
| `results/README.md` | Index of all experiments: datasets, modes, summaries, figures |
| `docs/source_notes_3.5.md` | Differences of Spark 3.5.9 from 4.1.3 for the facts in source_notes.md |
| `docs/source_notes.md` | Spark 4.1.3 source references: skew join, coalescing, MapStatus, shuffle sizes |

## Generator and runner

```
make submit JOB=jobs/generate.py ARGS="facts --rows 1000000 --n-keys 1000 --null-share 0.1"
make submit JOB=jobs/run_experiment.py ARGS="--experiment pilot_null --mode aqe_on --param join=left --param null_share=0.12"
```

Datasets are written to `/opt/spark-data/datasets/<id>/data` with
`manifest.json` next to it; `<id>` encodes all parameters and existing
datasets are not regenerated.

## Storage

Named Docker volumes (inside the Docker VM, not on the macOS filesystem):

| Volume | Mount path | Content |
|---|---|---|
| `spark-data` | `/opt/spark-data` (all containers) | Parquet data |
| `spark-events` | `/opt/spark-events` | Event logs |
| `spark-local-master`, `spark-local-1..3` | `/opt/spark-local` | Spark local/shuffle/spill dirs |

Only the project directory is bind-mounted (`/opt/project` in the master).
