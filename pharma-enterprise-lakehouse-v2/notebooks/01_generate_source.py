# Databricks notebook source
"""Generate synthetic prescription CDC and dimension JSON files in a UC volume.

No real patients, actual prescriptions, Eisai systems or protected health data.
Use batch_number=1 first, then batch_number=2 to simulate incremental changes.
"""
from datetime import datetime, timedelta, timezone
from random import Random
from pyspark.sql import types as T

dbutils.widgets.text("catalog", "main")
dbutils.widgets.dropdown("batch_number", "1", ["1", "2"])
catalog = dbutils.widgets.get("catalog")
batch_number = int(dbutils.widgets.get("batch_number"))
root = f"/Volumes/{catalog}/pharma_admin/landing/source"

rng = Random(21)
regions = ("US", "EU", "APAC")
products = [(f"PRD{i:03d}", f"Demo Product {i:03d}",
             ("Oncology", "Neurology", "Immunology")[i % 3]) for i in range(1, 13)]
hcps = [(f"HCP{i:04d}", f"Synthetic HCP {i:04d}",
         regions[(i - 1) % 3],
         ("General Medicine", "Oncology", "Neurology")[i % 3]) for i in range(1, 81)]
base = datetime(2026, 1, 1, tzinfo=timezone.utc)
initial = []
for i in range(1000):
    hcp = hcps[rng.randrange(len(hcps))]
    product = products[rng.randrange(len(products))]
    initial.append((
        f"EVT-I-{i+1:06d}", f"RX{i+1:06d}", hcp[0], product[0], hcp[2],
        rng.randint(1, 5), (base + timedelta(minutes=i)).isoformat(),
        1, "I", "synthetic_crm"
    ))

event_schema = T.StructType([
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

if batch_number == 1:
    records = initial
    spark.createDataFrame(hcps, "hcp_id STRING, hcp_name STRING, region STRING, specialty STRING") \
        .coalesce(1).write.mode("overwrite").json(f"{root}/hcps")
    spark.createDataFrame(products, "product_id STRING, product_name STRING, therapeutic_area STRING") \
        .coalesce(1).write.mode("overwrite").json(f"{root}/products")
else:
    updates = []
    for i, r in enumerate(initial[:120]):
        updates.append((f"EVT-U-{i+1:06d}", r[1], r[2], r[3], r[4], r[5] + 2,
                        (base + timedelta(days=1, minutes=i)).isoformat(), 2, "U", r[9]))
    deletes = []
    for i, r in enumerate(initial[:20]):
        deletes.append((f"EVT-D-{i+1:06d}", r[1], r[2], r[3], r[4], 0,
                        (base + timedelta(days=2, minutes=i)).isoformat(), 3, "D", r[9]))
    old = initial[0]
    late = ("EVT-LATE-000001", old[1], old[2], old[3], old[4], 999,
            (base + timedelta(minutes=1)).isoformat(), 1, "U", old[9])
    bad = ("EVT-BAD-000001", "RX-BAD", old[2], "", old[4], -5,
           old[6], 1, "I", old[9])
    records = updates + deletes + [initial[3], updates[0], late, bad]

# Append files instead of overwriting source directory: Auto Loader sees new files.
# Replaying a batch creates new files, but Silver deduplicates by event_id.
spark.createDataFrame(records, event_schema).coalesce(1) \
    .write.mode("append").json(f"{root}/prescription_events")
print(f"Generated synthetic batch {batch_number}: {len(records)} event rows")
