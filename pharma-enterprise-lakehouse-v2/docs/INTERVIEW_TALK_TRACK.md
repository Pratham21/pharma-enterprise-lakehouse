# 60-second GitHub interview walkthrough

> "I built an independent synthetic pharma commercial lakehouse proof of concept to demonstrate architecture choices. It uses Databricks Auto Loader for Bronze ingestion, a real PySpark `foreachBatch` incremental Silver pipeline, keyed Delta MERGE for current state and event history, and an SCD Type 2 projection. It includes quarantine for bad input and a replay test, because separate Delta table commits require idempotent recovery. Gold tables are reporting-friendly star-schema marts, with basic quality gates. I also built a Spark performance experiment to compare a shuffle join, a small-dimension broadcast join and selective key salting; the experiment checks result correctness and records observed timing. The standard-library reference tests pass; the Spark/Delta integration path needs an available Spark runtime, and I have not claimed 5 TB production deployment."

## Likely challenging questions

**Why Delta MERGE and why not overwrite?** A keyed MERGE applies new CDC changes transactionally to a single target table and preserves untouched rows. Full overwrite scales poorly and risks undesired replacements. We still need stable event IDs and source order.

**Why SCD2 as well as a current-state table?** `rx_current` supports efficient operational queries; Type 2 stores attribute states over effective time intervals and logical deletes. Event history preserves observed source events. They have different grains.

**Exactly-once guarantee?** Stream checkpoints alone do not grant end-to-end exactly-once with `foreachBatch`; the code uses idempotent MERGE, event IDs and sequence guards. Silver writes to several Delta targets are not cross-table atomic.

**What about 100 GB dimensions?** Do not blindly broadcast them. Filter/aggregate the dimension and inspect actual sizes and Spark UI; use AQE and shuffle joins, test salting if skew remains, and validate output correctness.

**Is this a production pharma platform?** No. No patient/PHI data, real vendors, formal validation, audit controls, 5 TB tests, or enterprise MDM. It is a working architectural proof of concept once run in a configured Spark + Delta environment.
