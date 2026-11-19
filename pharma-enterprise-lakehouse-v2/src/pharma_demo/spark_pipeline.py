"""Real PySpark + Delta Lake incremental CDC transformation utilities.

This module requires PySpark and delta-spark. Designed for Spark 3.5+/DBR and
portable Delta table paths for local tests. Not a validated GxP implementation.

Event assumptions:
- event_id is immutable and identifies one source event;
- event_seq monotonically increases for each rx_id;
- timestamp is UTC, and sequence is authoritative over timestamp;
- conflicting events with equal (rx_id, event_seq) are preserved in raw history,
  but the earliest timestamp is selected for the Type-2 business-state projection.
  Real sources should quarantine conflicts and use authoritative source LSNs.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

EVENT_FIELDS = (
    "event_id", "rx_id", "hcp_id", "product_id", "region", "quantity",
    "event_ts", "event_seq", "operation", "source_system",
)


def quality_split(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Validate and deterministic-deduplicate a *bounded* CDC micro-batch.

    Duplicates across micro-batches are suppressed in the Delta history MERGE.
    Invalid source events go to the quarantine result with a stable reject key.
    """
    # try_to_timestamp avoids an exception on malformed timestamps in ANSI mode.
    enriched = raw.withColumn("_parsed_ts", F.expr("try_to_timestamp(event_ts)"))
    reason = (
        F.when(F.col("event_id").isNull() | (F.trim("event_id") == "") |
               F.col("rx_id").isNull() | (F.trim("rx_id") == ""), "missing_identifiers")
        .when(~F.col("operation").isin("I", "U", "D") | F.col("operation").isNull(),
              "unsupported_operation")
        .when(F.col("event_seq").isNull() | (F.col("event_seq") <= 0) |
              F.col("_parsed_ts").isNull(), "invalid_event_order")
        .when((F.col("operation") != "D") &
              (F.col("hcp_id").isNull() | (F.trim("hcp_id") == "") |
               F.col("product_id").isNull() | (F.trim("product_id") == "") |
               F.col("region").isNull()), "missing_dimension_key")
        .when((F.col("operation") != "D") &
              (F.col("quantity").isNull() | (F.col("quantity") <= 0)), "invalid_quantity")
    )
    checked = enriched.withColumn("reject_reason", reason)
    # Excludes volatile ingestion timestamps so retries get an identical key.
    digest = F.sha2(F.to_json(F.struct(*[F.col(c) for c in EVENT_FIELDS])), 256)
    quarantine = (checked.filter(F.col("reject_reason").isNotNull())
                  .withColumn("reject_key", digest)
                  .select("reject_key", "reject_reason", *EVENT_FIELDS)
                  .dropDuplicates(["reject_key"]))
    valid = checked.filter(F.col("reject_reason").isNull())
    # Normalized business column names/order; choose a deterministic survivor for
    # duplicate event IDs arriving in the same bounded micro-batch.
    window = Window.partitionBy("event_id").orderBy(
        F.col("_parsed_ts").asc(), F.col("rx_id").asc(), F.col("operation").asc())
    valid = (valid.withColumn("_dedupe_rank", F.row_number().over(window))
             .filter(F.col("_dedupe_rank") == 1)
             .drop("_dedupe_rank", "reject_reason", "_parsed_ts")
             .withColumn("event_ts", F.expr("try_to_timestamp(event_ts)"))
             .select(*EVENT_FIELDS))
    return valid, quarantine


def latest_by_key(valid: DataFrame) -> DataFrame:
    """One highest-ordered event per prescription in the incoming micro-batch."""
    ordering = Window.partitionBy("rx_id").orderBy(
        F.col("event_seq").desc(), F.col("event_ts").asc(), F.col("event_id").asc())
    return (valid.withColumn("_rn", F.row_number().over(ordering))
            .filter(F.col("_rn") == 1).drop("_rn")
            .withColumn("is_deleted", F.col("operation") == F.lit("D"))
            .withColumnRenamed("event_id", "last_event_id")
            .drop("operation")
            .select("rx_id", "hcp_id", "product_id", "region", "quantity",
                    "event_ts", "event_seq", "source_system", "is_deleted", "last_event_id"))


def scd2_projection(history: DataFrame) -> DataFrame:
    """Type-2 versions from all unique observed events of the selected keys.

    (rx_id, event_seq) is the version key; sequence is assumed authoritative.
    Deleted versions are kept as tombstone versions to support historical views.
    """
    seq_window = Window.partitionBy("rx_id", "event_seq").orderBy(
        F.col("event_ts").asc(), F.col("event_id").asc())
    canonical = (history.withColumn("_r", F.row_number().over(seq_window))
                 .filter(F.col("_r") == 1).drop("_r"))
    seq = Window.partitionBy("rx_id").orderBy("event_seq")
    return (canonical
            .withColumn("effective_from", F.col("event_ts"))
            .withColumn("effective_to", F.lead("event_ts").over(seq))
            .withColumn("is_current", F.col("effective_to").isNull())
            .withColumn("is_deleted", F.col("operation") == F.lit("D"))
            .select("rx_id", "event_seq", "event_id", "hcp_id", "product_id",
                    "region", "quantity", "source_system", "operation", "is_deleted",
                    "effective_from", "effective_to", "is_current"))


def _exists(spark: SparkSession, destination: str, by_path: bool) -> bool:
    return (Path(destination, "_delta_log").exists() if by_path
            else spark.catalog.tableExists(destination))


def _delta(spark: SparkSession, destination: str, by_path: bool) -> DeltaTable:
    return (DeltaTable.forPath(spark, destination) if by_path
            else DeltaTable.forName(spark, destination))


def _initialize(spark: SparkSession, df: DataFrame, destination: str, by_path: bool) -> None:
    if _exists(spark, destination, by_path):
        return
    writer = df.limit(0).write.format("delta").mode("errorifexists")
    if by_path:
        writer.save(destination)
    else:
        writer.saveAsTable(destination)


def merge_delta(spark: SparkSession, df: DataFrame, destination: str,
                keys: tuple[str, ...], *, by_path: bool = False,
                condition: str | None = None) -> None:
    """Idempotent insert/replace by keys, optionally guarded against stale events."""
    _initialize(spark, df, destination, by_path)
    clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    target = _delta(spark, destination, by_path).alias("t")
    builder = target.merge(df.alias("s"), clause)
    if condition:
        builder = builder.whenMatchedUpdate(condition=condition,
                                            set={c: f"s.`{c}`" for c in df.columns if c not in keys})
    else:
        builder = builder.whenMatchedUpdateAll()
    builder.whenNotMatchedInsertAll().execute()


def append_unique_events(spark: SparkSession, df: DataFrame, destination: str,
                         *, by_path: bool = False, key: str = "event_id") -> None:
    """Append validated CDC history exactly once by event ID under serial writes."""
    _initialize(spark, df, destination, by_path)
    (_delta(spark, destination, by_path).alias("t")
      .merge(df.alias("s"), f"t.`{key}` = s.`{key}`")
      .whenNotMatchedInsertAll()
      .execute())


def read_delta(spark: SparkSession, destination: str, by_path: bool) -> DataFrame:
    return spark.read.format("delta").load(destination) if by_path else spark.table(destination)


def apply_cdc_batch(spark: SparkSession, raw_batch: DataFrame,
                    destinations: Mapping[str, str], *, by_path: bool = False) -> dict[str, int]:
    """Apply a bounded Bronze CDC batch to durable Silver Delta tables.

    Cross-table operations are NOT atomic. Reprocessing the whole micro-batch after
    a failed foreachBatch is intentional, safe for serial execution and converges
    the history/current/SCD2 tables through keyed MERGEs.
    """
    needed = {"quarantine", "history", "current", "scd2"}
    if set(destinations) != needed:
        raise ValueError(f"destinations must have exactly: {sorted(needed)}")
    valid, rejected = quality_split(raw_batch)
    # Materialize per batch because the same data participates in several actions.
    valid = valid.persist()
    rejected = rejected.persist()
    try:
        count_valid = valid.count()
        count_reject = rejected.count()
        if count_reject:
            append_unique_events(spark, rejected, destinations["quarantine"],
                                 by_path=by_path, key="reject_key")
        if count_valid:
            append_unique_events(spark, valid, destinations["history"], by_path=by_path)
            # The current state is ordered by source event sequence, not ingest time.
            incoming_latest = latest_by_key(valid)
            guard = "s.event_seq > t.event_seq"
            merge_delta(spark, incoming_latest, destinations["current"],
                        ("rx_id",), by_path=by_path, condition=guard)
            # Recompute only business keys touched by this micro-batch, not full Bronze.
            changed = valid.select("rx_id").distinct()
            matching_history = read_delta(spark, destinations["history"], by_path).join(
                changed, "rx_id", "inner")
            updated_versions = scd2_projection(matching_history)
            merge_delta(spark, updated_versions, destinations["scd2"],
                        ("rx_id", "event_seq"), by_path=by_path)
        return {"valid_events": count_valid, "quarantined_events": count_reject}
    finally:
        valid.unpersist()
        rejected.unpersist()


def gold_metrics(current: DataFrame) -> DataFrame:
    """Gold metrics at prescription-date x region x product grain."""
    active = current.filter(~F.col("is_deleted")).withColumn("rx_date", F.to_date("event_ts"))
    return (active.groupBy("rx_date", "region", "product_id")
            .agg(F.countDistinct("rx_id").alias("rx_count"),
                 F.sum("quantity").alias("units")))
