"""Execute a REAL local PySpark + Delta Lake CDC demo (not the stdlib reference).

Requires `pip install -e '.[spark]'`. First Delta run may download JVM jars
from Maven Central; a live Databricks workspace already includes Delta Lake.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pharma_demo.synthetic import generate
from pharma_demo.spark_pipeline import apply_cdc_batch, gold_metrics, read_delta


def make_spark() -> SparkSession:
    builder = (SparkSession.builder.master("local[2]").appName("pharma-delta-cdc-poc")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4"))
    return configure_spark_with_delta_pip(builder).getOrCreate()


def run_spark_demo(spark: SparkSession, n: int, output: Path) -> dict:
    source = generate(n=n)
    paths = {name: str((output / "delta" / name).resolve())
             for name in ("history", "current", "scd2", "quarantine")}
    output.mkdir(parents=True, exist_ok=True)
    for label in ("batch_1", "batch_2"):
        df = spark.createDataFrame(source[label])
        print(f"Processing {label} (PySpark + Delta MERGE)")
        apply_cdc_batch(spark, df, paths, by_path=True)

    current = read_delta(spark, paths["current"], by_path=True)
    history = read_delta(spark, paths["history"], by_path=True)
    scd2 = read_delta(spark, paths["scd2"], by_path=True)
    rejected = read_delta(spark, paths["quarantine"], by_path=True)
    gold = gold_metrics(current)
    before = {
        "current_records": current.count(),
        "active_records": current.filter(~F.col("is_deleted")).count(),
        "deleted_records": current.filter(F.col("is_deleted")).count(),
        "history_event_records": history.count(),
        "scd2_version_records": scd2.count(),
        "quarantined_records": rejected.count(),
        "gold_units": gold.agg(F.sum("units")).first()[0],
    }
    # Deliberately replay batch 2 to demonstrate idempotent Delta table state.
    apply_cdc_batch(spark, spark.createDataFrame(source["batch_2"]), paths, by_path=True)
    after = {
        "current_records": read_delta(spark, paths["current"], True).count(),
        "active_records": read_delta(spark, paths["current"], True)
            .filter(~F.col("is_deleted")).count(),
        "deleted_records": read_delta(spark, paths["current"], True)
            .filter(F.col("is_deleted")).count(),
        "history_event_records": read_delta(spark, paths["history"], True).count(),
        "scd2_version_records": read_delta(spark, paths["scd2"], True).count(),
        "quarantined_records": read_delta(spark, paths["quarantine"], True).count(),
        "gold_units": gold_metrics(read_delta(spark, paths["current"], True))
            .agg(F.sum("units")).first()[0],
    }
    if before != after:
        raise AssertionError(f"CDC replay changed output: before={before}, after={after}")
    return {"engine": "actual PySpark and Delta Lake", "input_rows": n,
            "summary": after, "replay_identical": True}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200)
    parser.add_argument("--output", type=Path, default=Path("spark_outputs"))
    args = parser.parse_args()
    spark = make_spark()
    try:
        result = run_spark_demo(spark, args.rows, args.output)
        (args.output / "spark_delta_report.json").write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
