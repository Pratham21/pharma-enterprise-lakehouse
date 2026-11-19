# Hands-on PySpark, Delta CDC, Type-2, and skew lab

**Status:** Code supplied, but a Spark cluster and Delta JVM artifacts must be provided before the cloud/local Spark demonstrations can be called executed. The standard-library reference is separately tested. Do not present the examples as a 5 TB benchmark.

## A. Run local PySpark + Delta Lake

Use Python 3.11/3.12 and Java 17, network access to download `pyspark`/`delta-spark` dependencies and Delta JVM jars, or use an existing compatible Databricks cluster.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[spark]'
python -m unittest discover -s tests -p test_spark_pipeline.py -v
python -m pharma_demo.spark_demo --rows 200 --output spark_outputs
```

The demo applies initial and change batches, verifies **replay invariance**, and writes `spark_outputs/spark_delta_report.json`. It creates real Delta tables under `spark_outputs/delta/` (not Git-tracked). The PySpark/Delta installation also requires Maven JVM artifact download on first launch.

Inspect:

- `spark_outputs/delta/current`: latest row per `rx_id`, including delete tombstones.
- `spark_outputs/delta/history`: one observed source event per `event_id`.
- `spark_outputs/delta/scd2`: one canonical business-state version per `(rx_id, event_seq)`; `effective_from`, `effective_to`, `is_current`, `is_deleted`.
- `spark_outputs/delta/quarantine`: invalid event with stable reject key.

**Important edge case:** Same `(rx_id, event_seq)` with conflicting data violates this project's event-sequence assumption. All events remain in history, while SCD2 picks the earliest timestamp for that sequence; production code should flag conflicts, reconcile to a source LSN, and prevent ambiguous business-state updates.

## B. Run on Databricks with Unity Catalog

1. Import the entire repository into a Databricks Git folder; do not copy only the notebooks because they import `src/pharma_demo/`.
2. Open `notebooks/00_setup.py`. Confirm the existing Unity Catalog name and privileges.
3. Run `01_generate_source.py` with `batch_number=1`.
4. Run `02_bronze_autoloader.py` (checkpointed ingestion).
5. Run `03_silver_cdc.py` (**incremental** `readStream.table(bronze)`, `foreachBatch`, Delta `MERGE`).
6. Run `04_gold_marts.py`, `05_validate.py`.
7. Change `batch_number=2`; rerun notebooks 01 → 05. Validate `rx_scd2_history`: version 1 is closed when version 2 arrives, and the current version is a delete for RX000001.
8. Rerun the exact same batch: counts and business state must remain unchanged.
9. Run `06_performance_lab.py` independently (it is **not** part of the main scheduled data pipeline).

Check the Spark UI (SQL and Stages tabs), Delta history and Unity Catalog table lineage. Document run timestamp, cluster type, runtime and table version. Workspace execution and security controls must be validated on your own workspace; this code is an educational prototype.

## C. Skew and join performance lab

```bash
python -m pharma_demo.performance --rows 200000 --keys 1000 --hot-pct 65 --salts 8 --output reports/performance_benchmark.json
```

The program creates synthetic fact and dimension DataFrames, then executes three join strategies:

| Scenario | Join strategy | Discussion point |
|---|---|---|
| Baseline | Disables AQE and automatic broadcast to expose a shuffle join | Inspect uneven task times, shuffle, and potential spills |
| Small dimension | Explicit broadcast of tiny dimension | Removes the join shuffle, but **never** broadcast a 100 GB dimension without memory evidence |
| Large / skewed dimension | Deterministically salts the hot key and selectively replicates only the hot dimension row | Distributes one dominant join key over multiple tasks at the cost of replication |

All three must produce the same row count and weighted units. Runtime is measured, but speed-ups are **not guaranteed** at sample sizes because scheduling and JVM warm-up may dominate. Try multiple cluster sizes and numbers of shuffle partitions; take representative median times only after repeated runs.

### Evidence checklist for screenshots

- Spark UI SQL DAG or physical plan: `SortMergeJoin` vs `BroadcastHashJoin`.
- Stage detail: task duration distribution, max versus median task time.
- Shuffle read/write bytes and disk/memory spill.
- Data skew: percent of facts with `join_key = 0` and largest shuffle partition.
- Data correctness: three matching aggregation results.
- Cloud job history and Delta table history (only after real runs).

**5 TB x 100 GB interview interpretation:** Start with filtering and projection, collecting table statistics and examining the plan. A 100 GB dimension is ordinarily not safe to broadcast. Prefer appropriately sized shuffle joins with AQE enabled; use salting only if severe skew persists and test replication cost. Validate with Spark UI, p95 task duration, shuffles, spills, SLAs and spend.

## D. Fault recovery and production boundaries

- Auto Loader's checkpoint tracks **file ingestion**, not the commit status of the downstream Silver MERGEs.
- Silver has a **separate** `readStream` checkpoint. `foreachBatch` may replay a micro-batch after a failure; idempotent keyed MERGEs are essential.
- Each destination table's Delta transaction is atomic, but the four destination tables together **do not form an atomic transaction**. Retried whole batches converge; serving-layer readers still need a release gate / versioned publish process.
- The history table is an operational source-event history, **not** a legally validated pharma audit trail. Implement validated retention, change control, access review, audit trails, and any applicable GxP requirements separately.
- Gold marts are deliberately rebuilt per demo run; production-scale Gold would use incremental reads / Change Data Feed and careful publication, not full-table overwrite per micro-batch.
