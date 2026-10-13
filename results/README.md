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
| 16 | `tpch_profile`, `tpch_timing` (5a, step 1a) | TPC-H SF10 (DuckDB 1.4.5 dbgen, Parquet in `/opt/spark-data/tpch_sf10`; lineitem 59 986 052 rows), Spark query texts `bench/tpch/q1..q22.sql` (Spark v4.1.3 `sql/core/src/test/resources/tpch`), default broadcast thresholds, 200 partitions | profile: every query, one AQE-on run with lz4 and one with zstd; timing: one application per query, 1 warm-up + 3 measured rounds, modes interleaved in randomized order; r per query = overall ratio of the lz4 profile | profile: aqe_on; timing: aqe_off, aqe_on, aqe_on_rule2 | `tpc_shuffle_ratios.csv`, `tpc_ratio_summary.csv`, `tpc_timing_summary.csv` | `figures/tpc_ratios`, `figures/tpc_timing` |
| 17 | `tpcds_profile`, `tpcds_timing` (5a, step 1b) | TPC-DS SF10 (DuckDB dsdgen; store_sales 28 800 991 rows), `bench/tpcds/*.sql` (103 queries, Spark v4.1.3 `tpcds`); column `customer.c_last_review_date_sk` renamed to `c_last_review_date` at view registration (query texts use the TPC-DS v1.4 name, q30) | profile: all 103 queries, lz4; timing: the 30 queries with the largest total shuffle bytes written in the profile (list in `batch5a/ratios.json`, key `tpcds_top30`), 1 warm-up + 3 measured rounds | aqe_on, aqe_on_rule2 | as 16 | as 16 |
| 18 | `exp_g_alt` (5a, step 2) | 55M rows, 44 files, LEFT, broadcast off, lz4. S1: 1x, 20 % NULL, 200; S2: 8x, 20 % NULL, 200; S3: 8x, 20 % NULL, 2001; S4: 8x, 0 % NULL, 2001; S5: 1x, 0 % NULL, 2001. In S3-S5 rule 1 is added to every AQE-on mode; r from an AQE-on profiling run | S1-S5 | aqe_off, aqe_on, aqe_on_fixed8 (advisory 8MB, skew 32MB), aqe_on_fixed16 (16MB, 64MB), aqe_on_nocoalesce, aqe_on_rule2 | `exp_g_summary.csv` | `figures/exp_g` |
| 19 | `exp_h_stress` (5a, step 3a) | as 10, 7M facts rows (NULL partition of the k2 join 635.7 MiB uncompressed shuffle data, estimated as NULL rows x data size per record of the k2 exchange), payload 400 B level 8x | one point; r from AQE-off (4.925) and AQE-on (7.332) profiles; maximum per-shuffle ratio 8.185 | aqe_off, aqe_on, rules 1+2 with r_off / r_on / r_max, iterated (warm-up with r_on, then 3 runs each using r of the previous run) | `exp_h_stress_summary.csv`, `exp_h_stress_k2.csv` (k2 join, recovered from the History Server, `analysis/recover_k2.py`), `exp_h_iter.csv` | `figures/exp_h` |
| 20 | `exp_h_sens` (5a, step 3b) | dataset and point of S3 | r (S3 profile, 7.687) x 0.5, 1, 2 | aqe_on_rules12 | `exp_h_sens_summary.csv` | `figures/exp_h` |
| 21 | `exp_h_sample` (5a, step 3c) | datasets of 6 (55M, 8 % NULL, 45 files), levels 1x, 4x, 16x; facts filtered to a deterministic 1 % / 10 % (xxhash64 of id) | one AQE-on run each | aqe_on | `exp_h_sample.csv` | `figures/exp_h` |
| 22 | `scripts/aqe_advisor.py` (5a, step 3d) | three existing runs | – | – | `advisor_output.txt` | – |
| 23 | `exp_real` (5b, step 1c) | real datasets below; Q-natural: fact LEFT JOIN per-key table (row count and one aggregate) derived from the same data on a high-cardinality key (taxi: PULocationID + pickup hour; hits: UserID; pageviews: project + page_title), default broadcast thresholds; Q-lowcard: same on a low-cardinality key (taxi: PULocationID; hits: RegionID; pageviews: project), broadcast disabled to emulate a large dimension. Note: the per-key side is HashAggregate over a shuffle, so `OptimizeSkewedJoin` (pattern Sort over a shuffle stage on both sides, `OptimizeSkewedJoin.scala:201-203`) cannot apply to these plans | dataset x query x 200 / 2001 partitions; r per point from an AQE-on profile | 200: aqe_off, aqe_on, aqe_on_rule2; 2001: all five | `exp_real_summary.csv`, `exp_real_keys.csv` (key distributions), manifests in `batch5b/manifests/` | `figures/exp_real` |
| 24 | Batch 5c, step 0 (validation) | one AQE-on trial per scenario of steps 2 and 3: plan shape (both SortMergeJoin children Sort directly over a shuffle stage), largest fact-side partition (uncompressed: exact UnsafeRow bytes per hash partition from `jobs/prepare_keytables.py`; compressed: divided by the trial's fact-exchange ratio) against each mode's threshold | 10 trials | aqe_on | `batch5c_validation.csv`, `batch5c/trials.json`, key-table manifests in `batch5c/manifests/` | – |
| 25 | Batch 5c, step 1 | batch-5b results only (no reruns): wall-time median and IQR of every point | – | – | `exp_real_wall_iqr.csv` | – |
| 26 | `exp_real_kt` (5c, step 2) | real datasets of 23; per-key table (key, row count, one aggregate) written to Parquet by `jobs/prepare_keytables.py` in a separate job; query fact LEFT JOIN key table ON key; broadcast disabled. Keys: taxi PULocationID, hits RegionID, hits CounterID, pageviews project. 3 rounds (6 measured runs per point and mode). Four round-2 jobs ran while the host slept on battery; they were rerun after the batch, the invalid records are in `batch5c_invalid_host_sleep/` | 4 keys x 200 / 2001 partitions; r from the AQE-on trial | 200: aqe_off, aqe_on, aqe_on_fixed16, aqe_on_rule2; 2001: aqe_off, aqe_on, aqe_on_rule1, aqe_on_fixed16 + rule 1, aqe_on_rule2, aqe_on_rules12 | `exp_real_kt_summary.csv` | `figures/exp_real_kt` |
| 27 | `exp_hot_kt` (5c, step 3) | synthetic facts of 13 (55M, 44 files, level 1x, 20 % on hot key 0), 200 partitions, broadcast disabled. A: facts LEFT JOIN per-key table read from Parquet; B: same per-key table computed with GROUP BY k in the query. 4 rounds | variants A, B | aqe_off, aqe_on, aqe_on_force (`spark.sql.adaptive.forceOptimizeSkewedJoin=true`) | `exp_hot_kt_summary.csv`, final plans in `exp_hot_kt_plans/`; source code in `../docs/source_notes.md` ("Which joins can be optimized at all") | `figures/exp_hot_kt` |
| 28 | Batch 6, step 0 (validation) | one AQE-on trial per scenario: plan shape, shuffle records vs input rows, ratio, largest / median task read, MapStatus class | 19 trials | aqe_on | `batch6_validation.csv`, `batch6/trials.json` | – |
| 29 | `exp_op` (6, step 1) | 55M rows, 44 files, 0 % NULL, broadcast disabled; window: row_number() OVER (PARTITION BY k ORDER BY id), all columns; agg: GROUP BY id (unique; shuffle records = input rows, partial aggregation does not reduce them), max(payload), count; hot window: 20 % on one key, 1x, 200 | levels 1x / 8x x 200 / 2001; hot window | aqe_off, aqe_on, aqe_on_fixed16, aqe_on_rule2 (rule 1 added to the last two at 2001); hot: aqe_off, aqe_on | `exp_op_summary.csv` | `figures/exp_op` |
| 30 | `exp_real_kt` with zstd (6, step 2) | as 26 with `spark.io.compression.codec=zstd`, keys taxi PULocationID, hits RegionID, pageviews project; 2 rounds (reduced from 3: projection 14.1 h > 12 h; user accepted ~13 h) | 3 keys x 200 / 2001 | 200: aqe_off, aqe_on, aqe_on_fixed16, aqe_on_rule2; 2001: aqe_off, aqe_on, aqe_on_fixed16 + rule 1, aqe_on_rules12 | `exp_zstd_kt_summary.csv` | `figures/exp_zstd` |
| 31 | executor memory (6, step 3) | `spark.executor.memory` 2g, 3g, 4g (third level 3g by user decision; measured anonymous memory of a worker container: 2.70 / 3.71 / 4.73 GiB at 2g / 3g / 4g in this step, up to 5.35 GiB at 4g in the step-0 trials; one OOM kill at 4g in batch 5b). Scenarios: `exp_g_alt` 8x, 0 % NULL, 2001; `exp_c2_dose` 4x, 8 % NULL, 200 | 2 scenarios x 3 heaps | aqe_off, aqe_on, rule 2 (+ rule 1 at 2001) | `exp_mem_summary.csv`, `batch6/memlog/` | `figures/exp_mem` |
| 32 | Step 4a (6) | `jobs/shuffle_blocks.py`: hits, 2001 partitions, AQE off; per map task of the fact shuffle: rows of the hottest key, exact block length from the shuffle index file, size reported by MapStatus | CounterID = 3922, RegionID = 229 | aqe_off | `step4a_<key>_blocks.csv`, `step4a_<key>_summary.json` | `figures/step4a_blocks` |
| 33 | Step 4b (6, no new runs) | batch-5c taxi PULocationID at 2001 partitions: spill per stage, Sort "spill size" per join side (History Server), spilling join tasks | all modes of 26 | – | `step4b_taxi_spill.csv` | – |
| 34 | Step 4c (6) | `spark.shuffle.minNumPartitionsToHighlyCompress=2002` vs rule 1; hits RegionID 2001 (key table): rule 1 vs the alternative; synthetic 55M, 8x, 20 % NULL, 2001: rules 1+2 vs alternative + rule 2 (rule 2 added: without it the NULL partition, 172 MiB compressed, is below the 256 MB threshold; user decision). Map output status sizes on the driver (`run_experiment --map-status-size`; SizeEstimator and Java-serialized length) | 2 points | see left | `exp_minhc_summary.csv` | – |
| 35 | Batch 7, step 0 (validation) | datasets present (none regenerated); B at 1000 / 2000 with AQE off: SortMergeJoin, both sides Sort > Exchange, 1000 / 2000 join tasks; pipelines with aqe_off_hints: 3 BHJ + 2 SMJ (n=5), 10 + 10 (n=20), equal to aqe_on_hints; TPC-DS zstd: codec zstd in the effective conf and in the event log | 7 trials | – | `batch7/validation.json` | – |
| 36 | `exp_b_partitions` gap (7, step 1) | as 4 (hex payload, as defined in batch 2; measured r = 1.25), AQE off only | 1000, 2000 (new) + anchors 200, 2001 | aqe_off | `exp_b_gap_summary.csv` (anchors next to `exp_b_summary.csv`) | – |
| 37 | pipeline gap (7, step 2) | INNER n=5, 20 as 9 (`exp_d2_inner`), LEFT n=5, 20 as 8 (`exp_d_pipeline`), 3M rows; new mode `aqe_off_hints` (AQE off, the broadcast hints of `aqe_on_hints`) | 4 pipelines | aqe_off_hints (new), aqe_off (anchor) | `exp_d_gap_summary.csv` (anchors next to `exp_d2_summary.csv` / `exp_d_summary.csv`) | – |
| 38 | TPC-DS zstd profile (7, step 3) | as 17, one AQE-on profiling run per query (all 103) with codec zstd | 103 queries | aqe_on | `tpc_shuffle_ratios_tpcds_zstd.csv`; `tpc_ratio_summary.csv` rebuilt (TPC-DS zstd row added, other rows unchanged) | – |

Batch 7 checks per job: AC power, containers, 3 alive workers, exactly 3 executors
on 3 hosts in the History Server, and no host sleep (pmset) during the job; none
failed. The anchors of steps 1 and 2 are faster than the earlier series and lie
outside their IQR (deterministic metrics such as spill are equal), so the new points
are compared with the new anchors.

## Real datasets (batch 5, step 1c)

Downloaded with `scripts/download_real.sh` into `data/raw/` (not committed; per
file URL, UTC time and size in `data/raw/downloads.tsv`), loaded by
`jobs/prepare_real.py` into `/opt/spark-data/datasets/real_<name>/` (manifest
with row count, schema and key distributions).

| Dataset | Source URL | Downloaded (UTC) | Rows after loading | License |
|---|---|---|---|---|
| NYC TLC yellow taxi trips, 2018 (12 monthly files) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-MM.parquet` (page https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | 2026-10-07 07:42-07:45 | 102 871 387 (all rows; column types differ between months and are cast to one schema) | no license stated on the TLC page; it states "The trip data was not created by the TLC, and TLC makes no representations as to the accuracy of these data" |
| ClickBench hits | `https://datasets.clickhouse.com/hits_compatible/hits.parquet` (https://github.com/ClickHouse/ClickBench) | 2026-10-07 08:31 (resumed after an interrupted download; 14 779 976 446 bytes) | 99 997 497 (the whole dataset; below the 100M target, as published) | no license for the dataset stated in the ClickBench README |
| Wikimedia pageviews, hourly, 2024-01-01 00:00-23:00 (24 files) | `https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-HH0000.gz` | 2026-10-07 07:45-07:54 | 143 351 375 (0 lines dropped) | CC0 ("All Analytics datasets are available under the Creative Commons CC0 dedication", https://dumps.wikimedia.org/other/pageviews/readme.html) |

hits columns (15 of 105, fixed before any measurement, not chosen by
compressibility; `HITS_COLUMNS` in `jobs/prepare_real.py`):

| Column | Reason |
|---|---|
| WatchID | row identifier |
| UserID | join key of Q-natural (high cardinality) |
| CounterID | candidate low-cardinality key |
| RegionID | candidate low-cardinality key (chosen: larger top-1 share, 18.3 % vs 8.5 %) |
| EventTime | time column (event timestamp) |
| EventDate | time column (date) |
| ClientIP | numeric (32-bit) |
| OS | numeric (small code) |
| ResolutionWidth | numeric |
| IsRefresh | numeric (flag) |
| TraficSourceID | numeric (small code) |
| SearchEngineID | numeric (small code) |
| AdvEngineID | numeric (small code) |
| URL | long text |
| Title | long text |

Other files: `calibration/` (pilot calibration), `batch3/ratios.json`,
`batch4/ratios.json`, `batch5a/ratios.json`, `batch5b/ratios.json` (rule-2 ratios r
and profiles), `batch3/calibration_*.csv`,
`batch{2,3,4}/plan.json` (job order), `batch{2,3,4}/jobs/*.csv` (one row per run;
`json_path` relative to that directory), `pilot_null_attempt1_host_sleep/`
(invalid first pilot attempt, host sleep).

Source references: `../docs/source_notes.md` (4.1.3) and
`../docs/source_notes_3.5.md` (differences in 3.5.9).
