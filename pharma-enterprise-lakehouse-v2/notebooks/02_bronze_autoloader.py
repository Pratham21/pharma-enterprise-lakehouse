# Databricks notebook source
"""Incrementally ingest new JSON source files to a managed Bronze Delta table."""
from pyspark.sql import functions as F, types as T

dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")
volume = f"/Volumes/{catalog}/pharma_admin/landing"
source = f"{volume}/source/prescription_events"
metadata = f"{volume}/stream_state/prescription_events"
target = f"{catalog}.pharma_bronze.prescription_events"

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("rx_id", T.StringType()),
    T.StructField("hcp_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("event_ts", T.StringType()),
    T.StructField("event_seq", T.LongType()),
    T.StructField("operation", T.StringType()),
    T.StructField("source_system", T.StringType()),
])

incoming = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{metadata}/schemas")
    .schema(schema)
    .load(source)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp()))

query = (incoming.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{metadata}/checkpoint")
    .trigger(availableNow=True)
    .toTable(target))
query.awaitTermination()
print("Bronze records:", spark.table(target).count())
