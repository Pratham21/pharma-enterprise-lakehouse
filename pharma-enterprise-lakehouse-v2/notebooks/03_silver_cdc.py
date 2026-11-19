# Databricks notebook source
"""Incremental Bronze-to-Silver CDC using real Spark foreachBatch + Delta MERGE.

Copy the full repository into a Databricks Git folder (or deploy its bundle).
The source package is resolved from ../src. Streaming checkpoint is on a UC
volume; re-running this notebook consumes only newer Bronze Delta commits.
"""
import json
import sys
from pathlib import Path

# DBR 14+ notebooks use their containing folder as current working directory.
sys.path.insert(0, str(Path.cwd().parent / "src"))
from pharma_demo.spark_pipeline import apply_cdc_batch

dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")
if not catalog.replace("_", "").isalnum():
    raise ValueError("Unsafe catalog identifier")

bronze = f"{catalog}.pharma_bronze.prescription_events"
root = f"/Volumes/{catalog}/pharma_admin/landing/stream_state/silver_cdc"
destinations = {
    "quarantine": f"{catalog}.pharma_silver.rx_quarantine",
    "history": f"{catalog}.pharma_silver.rx_events_history",
    "current": f"{catalog}.pharma_silver.rx_current",
    "scd2": f"{catalog}.pharma_silver.rx_scd2_history",
}


def handle_batch(df, batch_id: int):
    result = apply_cdc_batch(df.sparkSession, df, destinations)
    print(json.dumps({"microbatch": batch_id, **result}))

query = (spark.readStream.table(bronze)
         .writeStream.foreachBatch(handle_batch)
         .option("checkpointLocation", f"{root}/checkpoint")
         .trigger(availableNow=True)
         .start())
query.awaitTermination()
print("Silver current count:", spark.table(destinations["current"]).count())
print("SCD2 version count:", spark.table(destinations["scd2"]).count())
