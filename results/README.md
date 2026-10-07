# Results index

One entry per experiment of all batches. Common to all, unless stated:
Spark 4.1.3 cluster of `docker-compose.yml` (3 executors x 3 cores, 4 GB heap);
facts `k` keys drawn uniformly from 1 000 000 keys; dim with one row per key
(`match_rate` 1.0 unless stated); payload 100 bytes; seed 42; query writes to
`noop`; `BASELINE_CONF` of `jobs/spark_conf.py` (all AQE values at defaults);
lz4 shuffle codec; CBO off. Experiment names are the keys of `EXPERIMENTS` in
`jobs/experiments.py`; per-run records are in
`runs/<experiment>/<scenario>/<mode>/*.json`.

Payload "levels" (batches 3 and 4) are `payload_kind=mixed` with
`payload_random_fraction` 1.0 / 0.53 / 0.19 / 0.04 / 0.0 = levels 1x / 2x / 4x /
8x / 16x (measured lz4 ratios 1.17 / 2.0 / 3.84 / 6.80 / 12.9 at 8 % NULL).

Protocols:
- Pilot: one application per point, 1 warm-up + 5 measured runs.
- Batches 2-4: rounds; each round runs every (point, mode) once in a randomized
  order as one application with 1 warm-up + 2 measured runs; 4 rounds = 8
  measured runs per (point, mode). Summaries: timings as median, IQR, minimum;
  other metrics as median; skew as "runs with skew / runs".

Modes: `aqe_off`, `aqe_on` (defaults), `aqe_on_nocoalesce`
(coalescePartitions off), `aqe_on_skew64m` (skew threshold 64 MB),
`aqe_on_rule1` (`spark.shuffle.accurateBlockSkewedFactor=5`), `aqe_on_rule2`
(advisory 64MB/r, skew threshold 256MB/r, r = overall shuffle compression ratio
of the AQE-off calibration run of the same point), `aqe_on_rules12` (both),
`aqe_on_hints` (broadcast hints on filtered dims). In batch 2, the rule-1
control of experiment B is `aqe_on` with parameter `accurate_skew_factor=5.0`.

| # | Experiment (batch) | Dataset and query | Points | Modes | Summary | Figures |
|---|---|---|---|---|---|---|
| 1 | `pilot_null` (pilot) | 55M rows, hex payload, 200 partitions, broadcast off; LEFT and INNER JOIN on `k` | NULL share LEFT 0, 1, 2, 5, 8, 12, 20, 30, 50 %; INNER 0, 12, 50 % | aqe_off, aqe_on | `pilot_null_summary.csv`, `runs.csv`, `pilot_null_stage_durations.csv`, `pilot_anomalies_*.csv` | `figures/pilot_null`, `pilot_null_time.png` |
| 2 | `inner_check` (2) | as pilot, INNER JOIN | NULL 0, 12, 50 % | aqe_off, aqe_on, aqe_on_nocoalesce | `inner_check_summary.csv` | – |
| 3 | `exp_a_small` (2) | 13.7M rows, hex, 200 partitions, LEFT; control 55M at 3 % NULL | NULL 0, 5, 10, 15, 20, 30 % | aqe_off, aqe_on, aqe_on_skew64m (control: off, on) | `exp_a_summary.csv` | `figures/exp_a`, `exp_a_time.png` |
| 4 | `exp_b_partitions` (2) | 55M rows, hex, 20 % NULL, LEFT | shuffle partitions 200, 1000, 2000, 2001, 4000 | aqe_on at all; aqe_off at 200, 2001; rule-1 control at 2001, 4000 | `exp_b_summary.csv` | `figures/exp_b`, `exp_b_time.png` |
| 5 | `exp_c_compression` (2) | 55M rows, 8 % NULL, LEFT, 200 partitions | payload random / repetitive x codec lz4 / zstd | aqe_off, aqe_on | `exp_c_summary.csv` | – |
| 6 | `exp_c2_dose` (3) | 55M rows, 8 % NULL, LEFT, 200 partitions, mixed payload, 45 files | levels 1x-16x with lz4; 1x, 4x, 16x with zstd | aqe_off, aqe_on, aqe_on_nocoalesce, aqe_on_rule2 | `exp_c2_summary.csv` | `figures/exp_c2` |
| 7 | `exp_rules` (3), "combined check" | 55M rows, 20 % NULL, LEFT, level 8x, 45 files, 2001 partitions | one point | all five: off, on, rule1, rule2, rules12 | `exp_rules_summary.csv` | – |
| 8 | `exp_d_pipeline` (3) | 3M facts rows, n dims (1M rows, 20-byte payload, match 0.7), odd dims filtered to ~1 % (hash filter), default broadcast thresholds; stress: n=5 LEFT, 20 % NULL on k1, level 8x, 2001 partitions | n = 5, 20 x INNER, LEFT; stress | aqe_off, aqe_on, aqe_on_hints (stress: off, on, rules12) | `exp_d_summary.csv`, plans in `exp_d_plans/` | `figures/exp_d` |
| 9 | `exp_d2_inner` (4, step 1a) | as 8 but filtered dims use the "prefix" filter layout and the facts key of a filtered dim is drawn only from the 10 000 keys that pass the filter; match_rate 0.966 on every dim (0.966^20 = 0.50) | n = 5, 20, INNER | aqe_off, aqe_on, aqe_on_hints | `exp_d2_summary.csv` (rows `exp_d2_inner`; output rows, broadcast build sides), plans in `exp_d2_plans/` | `figures/exp_d2` |
| 10 | `exp_d2_stress` (4, step 1b) | 3M facts rows, n = 5 LEFT, match 0.7, hash filter; 20 % NULL on k2 (first unfiltered dim), payload 400 bytes level 8x (`payload_random_fraction` 0.04), 2001 partitions | one point | all five | `exp_d2_summary.csv` (rows `exp_d2_stress`, columns `k2_*`) | `figures/exp_d2_stress` |
| 11 | `exp_e_volume` (4, step 2a/2b) | LEFT, broadcast off, 20 % NULL; rows 27.5M / 55M / 110M with 22 / 44 / 88 Parquet files | 2a: level 1x, 200 partitions; 2b: level 8x, 2001 partitions | 2a: off, on; 2b: off, on, rules12 | `exp_e_summary.csv` | `figures/exp_e` |
| 12 | `exp_e_coalesce` (4, step 2c) | as 11, 0 % NULL, level 8x, 2001 partitions | rows 27.5M / 55M / 110M | off, on, rule2 | `exp_e_summary.csv` | `figures/exp_e` |
| 13 | `exp_f_hotkey` (4, step 2d) | 55M rows, 44 files, broadcast off, NULL 0, 20 % of rows on hot key 0 (present in dim); multi: 5 hot keys 0-4 of 4 % each | baseline level 1x / 200 partitions, INNER and LEFT; combined level 8x / 2001, INNER and LEFT; multi: combined, LEFT | baseline: off, on; combined and multi: off, on, rules12 | `exp_f_summary.csv` (also holds the 55M NULL rows of 11, column `skew_source`) | `figures/exp_f` |
| 14 | Spark 3.5.9 replication (4, step 3) | cluster `docker-compose.spark35.yml` (same resources, same data volume); datasets of 4, 6 and 7 | B: 200, 2000, 2001 (on), 2001 rule 1, 200 and 2001 (off); C2: levels 1x, 4x, 16x lz4; combined check | B as listed; C2: off, on, rule2; combined: all five | `spark35_summary.csv` (3.5.9 columns `_359`, 4.1.3 columns `_413` from summaries 4, 6, 7) | `figures/spark35` |
| 15 | Step 4 of batch 4 (no new runs) | event and executor logs of the batch-3 combined check (7), AQE off vs on | longest join task of each measured run | aqe_off, aqe_on | `step4_null_task.csv` | – |

Other files: `calibration/` (pilot calibration), `batch3/ratios.json` and
`batch4/ratios.json` (rule-2 ratios r), `batch3/calibration_*.csv`,
`batch{2,3,4}/plan.json` (job order), `batch{2,3,4}/jobs/*.csv` (one row per run;
`json_path` relative to that directory), `pilot_null_attempt1_host_sleep/`
(invalid first pilot attempt, host sleep).

Source references: `../docs/source_notes.md` (4.1.3) and
`../docs/source_notes_3.5.md` (differences in 3.5.9).
