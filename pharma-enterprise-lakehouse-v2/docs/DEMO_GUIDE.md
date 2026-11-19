# 5-minute GitHub and Databricks demo talk track

## From GitHub (works without Databricks)

1. Open the repo `README.md` architecture Mermaid chart.
2. Open `src/pharma_demo/synthetic.py`: demonstrate synthetic source generator and two CDC batches.
3. Open `tests/test_reference.py`: six tests for duplicates, invalid rows, deletes, out-of-order events and replay safety.
4. Show `sample_outputs/quality_report.json`: 1,000 initial prescriptions -> 980 current active, 20 logically deleted, two duplicate events removed, one quarantined.
5. Explain that `notebooks/` is a Databricks implementation designed to be run in a UC-enabled workspace, **not a cloud test that has already been completed**.

## In a Databricks workspace (after you actually run it)

1. Show Catalog Explorer: the `pharma_bronze`, `pharma_silver`, `pharma_gold` schemas and their tables.
2. Show `02_bronze_autoloader.py` (source, checkpoint and typed event schema).
3. Show `03_silver_cdc.py` (data quality, deduplication, event ordering and Delta MERGE).
4. Show `04_gold_marts.py` and run `sql/business_queries.sql` against your demo tables.
5. Run the second incremental batch to demonstrate updates, deletes, replay and metrics checks.
6. Open Job runs to explain dependencies and failed quality-gate behavior.

## Five good explanations

- **Why Delta Lake?** Transactions, MERGE for CDC, schema enforcement and reliable datasets.
- **Why not just repartition?** `repartition(400, key)` can worsen hot-key skew because identical keys still hash to one partition; inspect AQE and consider selective salting.
- **Why a tombstone?** Prevent an old, out-of-order update from resurrecting a deleted entity.
- **Where does the semantic layer fit?** Over Gold, to publish reusable KPI definitions independent of individual BI tools.
- **How would you productionize?** Incremental Silver/Gold, CI/CD, environment-specific catalogs, workload performance tests, secrets/network control, alerts and audit requirements.

## Boundaries

Don't say "I processed 5 TB a day", "I deployed this at Eisai", "this is HIPAA/GxP compliant", or "I've tested these Databricks jobs" unless you subsequently run/validate them. This is a public synthetic portfolio demo.


## Version 2 upgrade: real Spark/Delta walkthrough

See `docs/SPARK_LAB.md` for the prerequisite Java/PySpark/Delta runtime, incremental MERGE + SCD2 runbook, a replay test, and physical Spark UI evidence. The main original standard-library reference CLI is still available and does **not** run Spark. The v2 source package and performance notebook require real Spark.
