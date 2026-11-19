# Databricks notebook source
"""Data quality gates for incremental Silver + illustrative Gold snapshots."""
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")
silver = f"{catalog}.pharma_silver"
gold = f"{catalog}.pharma_gold"
current = spark.table(f"{silver}.rx_current")
history = spark.table(f"{silver}.rx_events_history")
scd2 = spark.table(f"{silver}.rx_scd2_history")
quarantine = spark.table(f"{silver}.rx_quarantine")
fact = spark.table(f"{gold}.fact_prescriptions")
hcp = spark.table(f"{gold}.dim_hcp")
product = spark.table(f"{gold}.dim_product")
metrics = spark.table(f"{gold}.daily_prescription_metrics")

current_scd2 = scd2.filter(F.col("is_current"))
checks = {
    "no_duplicate_current_rx": current.count() == current.select("rx_id").distinct().count(),
    "no_duplicate_historical_event_ids": history.count() == history.select("event_id").distinct().count(),
    "one_current_scd2_version_per_rx": (
        current_scd2.count() == current_scd2.select("rx_id").distinct().count()),
    "current_state_matches_scd2_sequences": (
        current.join(current_scd2, ["rx_id", "event_seq"], "left_anti").count() == 0),
    "no_deleted_rows_in_gold": fact.join(current.filter("is_deleted"), "rx_id", "inner").count() == 0,
    "positive_fact_quantity": fact.filter(F.col("quantity") <= 0).count() == 0,
    "valid_hcp_references": fact.join(hcp, "hcp_id", "left_anti").count() == 0,
    "valid_product_references": fact.join(product, "product_id", "left_anti").count() == 0,
    "fact_and_mart_counts_match": fact.count() == metrics.agg(F.sum("rx_count")).first()[0],
    "fact_and_mart_units_match": fact.agg(F.sum("quantity")).first()[0] ==
                                metrics.agg(F.sum("units")).first()[0],
}
for label, passed in checks.items():
    print(("PASS" if passed else "FAIL"), label)
print("Quarantined input events:", quarantine.count())
if not all(checks.values()):
    raise RuntimeError("Data quality gate failed; do not promote to business consumers.")
print("All demo data quality checks passed")
