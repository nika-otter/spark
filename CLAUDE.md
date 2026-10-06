# CLAUDE.md

Master's thesis experiments on Adaptive Query Execution (AQE) in Apache Spark:
how AQE (skew join, partition coalescing, runtime broadcast) behaves under NULL
skew, data size, shuffle partition count, compression and multi-join pipelines.

## Working rules (from the user)
- Reply in **Ukrainian**. Code, comments and file names in **English**.
- Do only what the step list asks: no extra experiments, no parameter tuning
  beyond what is listed, **no thesis text**.
- Report numbers as they are, including results that contradict expectations;
  do not interpret beyond the data. Say when a metric cannot be obtained reliably.
- Verify claims about Spark internals against the source (tag v4.1.3), not memory;
  see `docs/source_notes.md`.
- Before a long batch: estimate total runtime, compare with the threshold the
  user gives, stop and ask if exceeded.
- Commit at the end of each step list (attribution line per system reminder).
  Remote: `origin` = https://github.com/nika-otter/spark.git (push only when asked).

## Cluster
- Docker Desktop on macOS arm64 (12 cores, 32 GB); VM: 10 CPU, 24 GB.
- Image `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` (native arm64).
- `docker-compose.yml`: master (+ driver, 3.5 GB), 3 workers (3 cores, 6 GB
  limit, executor 4g heap), History Server (:18080). UIs: 8080, 8081-8083, 4040.
- Named volumes inside the VM: `spark-data` (/opt/spark-data, Parquet),
  `spark-events` (event logs), `spark-local-*` (shuffle/spill, executor logs in
  `/opt/spark-local/work/<app>/<exec>/stderr`). Only the project is bind-mounted
  (`/opt/project` in the master).
- `conf/spark-defaults.conf`: infrastructure (master URL, executor sizing, event
  log, driver 2g). `jobs/spark_conf.py`: `BASELINE_CONF` (SQL/AQE values, all equal
  to Spark defaults, set explicitly; CBO off, never ANALYZE TABLE) and
  `effective_conf()` recorded with every run.
- Commands: `make up | down | status | logs`, `make submit JOB=jobs/x.py ARGS="..."`,
  `make history-check`, `make venv` (project-local `.venv` with pandas/matplotlib;
  install nothing globally).

## Code
- `jobs/generate.py`: deterministic generator (xxhash64 of id + seed, never rand()).
  `FactsSpec` (keys `KeySpec` with null/hot share, payload kind hex|random|
  repetitive|mixed, `payload_random_fraction`, `num_files`), `DimSpec` (match_rate,
  `filter_bucket` 0..999). Dataset dir `/opt/spark-data/datasets/<id>/{data,manifest.json}`;
  `<id>` encodes all parameters (hashed when > 200 chars); existing datasets are skipped.
- `jobs/experiments.py`: `EXPERIMENTS` (name -> builder + defaults), `DEFAULTS`
  (all `--param` keys), `MODES` (aqe_off, aqe_on, aqe_on_nocoalesce, aqe_on_skew64m,
  aqe_on_rule1/rule2/rules12, aqe_on_hints). Rule 1 = `spark.shuffle.accurateBlockSkewedFactor=5`;
  rule 2 = advisory 64MB/r and skew threshold 256MB/r, r = measured shuffle ratio.
- `jobs/run_experiment.py`: one app per (scenario, mode); warm-up + measured runs to
  `noop`, no caching, GC between runs so old shuffle files are deleted.
- `jobs/metrics.py`: per-run metrics from the driver REST API -> JSON + one CSV row.
  Known limits are documented in its docstring (partition sizes via join-task
  reads; Parquet `input_bytes` unreliable; size metrics have 4 significant digits).

## Measurement protocol (batches 2 and 3)
- Rounds: each round runs every (point, mode) once in randomized order (seeded by
  round), one application = 1 warm-up + 2 measured runs; 4 rounds = 8 measured runs.
- Summaries: timings as median, IQR (q25/q75) and minimum; other metrics as median;
  skew as "runs with skew / runs". NULL partition size = largest join-task read
  with AQE off.

## Discipline for long runs
- Spark output always to log files, never to the terminal.
- One resumable background script per batch, started with a single command
  (`scripts/batchN.sh`): pid lock, `nohup caffeinate -ims`, done markers in
  `results/batchN/done/`, per-job CSVs in `results/batchN/jobs/`, `plan.json`
  fixed on first start. Rerunning the same command resumes.
- The Mac must be on AC power; host sleep once invalidated a whole grid
  (`results/pilot_null_attempt1_host_sleep/`). Check `pmset -g log` after a batch.
- Wait for a background batch with a background `until ...; do sleep 60; done` task.

## Where things are
- Run records: `results/runs/<experiment>/<scenario>/<mode>/<app>_run-<i>.json`;
  pilot rows in `results/runs.csv`; batch rows in `results/batch{2,3}/jobs/*.csv`
  (`json_path` there is relative to that jobs dir).
- Summaries: `results/*_summary.csv` (pilot_null, inner_check, exp_a, exp_b, exp_c,
  exp_c2, exp_rules, exp_d); D final plans in `results/exp_d_plans/`.
- Analysis (host, `.venv/bin/python`): `analysis/pilot_null.py`, `pilot_anomalies.py`,
  `batch2.py`, `batch3.py`, `eventlog_extract.py` (runs in the master container).
- Thesis figures: `analysis/figures.py` -> `results/figures/*.{png,pdf}` (300 dpi,
  Ukrainian labels, decimal comma, legend or direct labels never both, no lines
  through unmeasured points, IQR bars).
- Source notes: `docs/source_notes.md` (skew condition, split limit = map tasks,
  MapStatus > 2000 partitions, compressed vs uncompressed sizes, memory 1/N rule).
- Calibration: `results/calibration/`, `results/batch3/{ratios.json,calibration_*.csv}`.

## Gotchas
- Non-login shells lack Docker: `export PATH=$PATH:~/.docker/bin`.
- The tool shell is zsh: `$VAR` with spaces is not word-split; use arrays.
- `timeout` is not available on macOS.
- Container clocks are UTC; the host is UTC+3.
