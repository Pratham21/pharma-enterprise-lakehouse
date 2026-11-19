# Lead Data Engineer discussion prompts

**Q: Why these AWS/Databricks components?**
A: Use incremental file ingestion, transactionally reliable Delta tables, isolated compute, governed curated data marts and built-in orchestration; validate based on latency, cost, compliance and scale.

**Q: What does the Silver layer model?**
A: A deduplicated CDC current-state table with tombstones, a source-event history, a data-quality quarantine. A full Data Vault 2.0 would be an optional extension for multi-source historical integration.

**Q: How do you avoid duplicate output when a job crashes?**
A: Preserve Auto Loader checkpoints for file progress, deduplicate source event IDs, order events using reliable source sequence data, and use conditional idempotent Delta MERGE. Validate reconciliation counts before release.

**Q: Why not broadcast a 100 GB dimension table?**
A: It could exceed executor broadcast memory and overwhelm resources. Start with Spark UI, plan and statistics; reduce early, consider partition layout, AQE skew optimization and selective salting if key skew persists.

**Q: What is the semantic layer?**
A: A governed shared definition of business measures and dimensions built over curated data. Here it is a SQL view over Gold metrics and product dimensions; production platforms may use metric views or BI semantic models.

**Q: How would you handle 20 TB/day?**
A: Incremental source-specific pipelines, autoscaling compute after benchmarking, optimize file layouts and joins, separate workload compute, test fault recovery and monitor SLA and cost. This demo does not prove 20 TB/day capacity.

**Q: Where is Informatica?**
A: Not implemented; it could complement AWS/Databricks for enterprise ingestion, metadata and data quality. Explain similar engineering principles without implying IDMC hands-on deployment.

**Q: How do you measure platform success?**
A: Data quality pass rate, freshness, incident rate, recovery time, per-GB cost, data product adoption and business value. Report results transparently and reconcile source vs published data.

**Q: Do you have a real SCD Type 2 table?**
A: V2 includes an incrementally updated `rx_scd2_history` with `effective_from`, `effective_to`, `is_current`, and delete tombstones. Canonical versions are selected from event history by source sequence. This is not full Data Vault or regulated audit retention.

**Q: How do you demonstrate actual Spark performance optimization?**
A: Run `notebooks/06_performance_lab.py` or `python -m pharma_demo.performance` to compare shuffle, tiny-dimension broadcast, and hot-key salting. It checks that all three produce identical aggregates, prints physical plans, and measures wall time. Capture the Spark UI metrics; do not invent speedup percentages.

**Q: Has this code been tested in a real Spark or Databricks environment?**
A: The upgraded repository includes Spark/Delta integration tests, but they have not been run in the current offline build environment. The reference model and syntax checks were executed. I would run GitHub Actions and verify a Databricks job before reporting successful deployment or measured speedups.
