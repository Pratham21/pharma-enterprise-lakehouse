# Databricks notebook source
"""Build dimensional Gold marts and approved KPI views from Silver current state."""
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")
root = f"/Volumes/{catalog}/pharma_admin/landing/source"
gold = f"{catalog}.pharma_gold"

hcp = spark.read.json(f"{root}/hcps").select("hcp_id", "hcp_name", "region", "specialty")
products = spark.read.json(f"{root}/products").select("product_id", "product_name", "therapeutic_area")
for frame, name in ((hcp, "dim_hcp"), (products, "dim_product")):
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{gold}.{name}")

active = (spark.table(f"{catalog}.pharma_silver.rx_current")
    .filter(~F.col("is_deleted"))
    .withColumn("rx_date", F.to_date("event_ts"))
    .select("rx_id", "hcp_id", "product_id", "region", "rx_date",
            "quantity", "event_ts", "event_seq"))
active.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{gold}.fact_prescriptions")

metrics = (active.groupBy("rx_date", "region", "product_id")
    .agg(F.countDistinct("rx_id").alias("rx_count"),
         F.sum("quantity").alias("units")))
metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{gold}.daily_prescription_metrics")

# Governed business-facing view; an enterprise metric view can be layered on later.
spark.sql(f"""
CREATE OR REPLACE VIEW {gold}.vw_rx_metrics AS
SELECT m.rx_date, m.region, p.therapeutic_area, p.product_name,
       m.rx_count, m.units
FROM {gold}.daily_prescription_metrics m
JOIN {gold}.dim_product p ON m.product_id = p.product_id
""")
print("Gold fact records:", spark.table(f"{gold}.fact_prescriptions").count())
