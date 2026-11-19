# Architecture and modeling decisions

## Scope

This project demonstrates a synthetic commercial pharma dataset, not actual prescription, patient, provider or trial records. Names are fake. The AWS lakehouse portion uses Databricks managed Delta tables and a Unity Catalog volume; physical data placement depends on your workspace and storage configuration.

## Layers

1. **Landing:** Separate input files from persisted checkpoint and schema state. UC Volume file paths have the shape `/Volumes/<catalog>/<schema>/<volume>/...`.
2. **Bronze:** Auto Loader incrementally ingests source events, source file path and ingestion timestamp. Persist raw-ish source data (typed JSON schema here) for reprocessing.
3. **Silver:** `readStream` reads new Bronze changes with a checkpoint; `foreachBatch` validates events and uses keyed Delta MERGE for source history, current state, and Type 2 history. Source sequence controls ordering; logical deletes remain tombstones. Bad rows are quarantined.
4. **Gold:** Publish active-prescription facts, HCP and product dimensions, daily aggregation and governed SQL view.
5. **Orchestration and observability:** Lakeflow Jobs task dependencies and assertions; in production add alerting, SLA dashboards, structured logging, tracing and reconciliation.

## Why a star schema in Gold?

Business users query prescription count and units by date, product and region. The fact has prescription grain; dimensions hold HCP and product descriptive attributes. Gold dimensional data marts minimize repeated joins and stabilize business calculations.

This demo deliberately uses business/natural keys for dimensional joins. A production star schema might instead use surrogate dimension keys and slowly changing dimensions when historical attributes must be preserved.

## Where Data Vault could fit

For a multi-source, multi-region pharma platform, a Data Vault 2.0 integration zone in Silver is a *possible* choice, not a default requirement:

- **Hub HCP:** stable HCP business key and record source.
- **Hub Product:** stable product business key.
- **Link HCP–Product:** a modeled relationship where business processes justify one.
- **Satellites:** attributes with load timestamp, effective timestamp and record source; changes tracked historically.

The current v2 example **implements Type 2 history** in `rx_scd2_history` by versioning on `(rx_id, event_seq)` with `effective_from`, `effective_to`, `is_current` and logical delete markers. It does **not** implement formal Data Vault. The event-history table is a convenient educational audit surface, not a validated regulated audit trail.

## Failure and replays

- A persistent Auto Loader checkpoint tracks discovered files and stream progress.
- Source systems may replay business events even in new files; event-ID deduplication and source sequence guards make silver state idempotent for the demonstrated scenario.
- Deleted source entities are tombstoned to prevent a late older event from reviving them.
- Failure in quality gate stops the job. Real systems need alerting, hold/release policy and explicit promotion boundaries; the demo checks after Gold materialization.
- V2 uses incremental Bronze → Silver streaming micro-batches and keyed Delta MERGE for history, current state, quarantine, and Type 2 projections. Gold marts remain full-refresh for the small demo. Production needs incremental Change Data Feed propagation into Gold and a release/publishing boundary. Four separate Delta commits are not cross-table atomic; full-batch retries converge state but may still expose intermediate versions to downstream readers.

## Scalability design for 5–20 TB/day

This repository does **not** benchmark or claim to process 5–20 TB/day. A production plan would profile per-source file rates, source freshness objectives and peak traffic, then use:

- isolated workloads and separate ingestion/ETL/BI compute;
- Auto Loader scalable discovery and source-specific checkpoints;
- incremental Delta processing rather than complete Bronze scans;
- optimized joins, stats, selective broadcasting, AQE, clustering and file compaction;
- data quality SLAs, replayability, data retention and cost monitoring;
- workload tests at realistic cardinality, skew and data formats.

## Federated governance / MDM

- Global standards: regulated data classification, retention, access principles, business glossary and critical metrics.
- Domain ownership: Commercial stewards maintain definitions and quality of commercial HCP/product data; other domains manage their own sources within enterprise policies.
- Unity Catalog: technical access enforcement, table ownership and lineage inside the Databricks environment.
- Informatica IDMC is an optional integration/metadata/data-quality component for organizations already using it; **it is not deployed in this demo**.
- MDM would be a *separate master-data management process* for matching and survivorship of entities across multiple sources. A Gold table is not an MDM golden record.

## Executable performance lab

`src/pharma_demo/performance.py` compares a shuffle baseline, broadcast when the dimension is genuinely small, and selective salting when a single hot key causes skew. It validates aggregation equality and prints Spark physical plans and observed elapsed seconds. Timings require actual execution and are not claimed in the archive. See `docs/SPARK_LAB.md`.
