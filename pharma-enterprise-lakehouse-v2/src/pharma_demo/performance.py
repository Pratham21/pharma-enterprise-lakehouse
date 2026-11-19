"""Reproducible Spark skew / shuffle / broadcast / salting demonstration.

Runs on real Spark. Uses scaled synthetic data: not a 5 TB benchmark. Shows
techniques, EXPLAIN plans, result correctness, and observed wall-clock timings.
No guarantee that an optimized variant is faster on every machine or dataset.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def synthetic_join_data(spark: SparkSession, rows: int, keys: int, hot_pct: int):
    if rows < 1000 or keys < 3 or not (0 <= hot_pct <= 95):
        raise ValueError("rows>=1000, keys>=3 and 0<=hot_pct<=95 required")
    facts = (spark.range(rows)
        .withColumn("join_key", F.when(F.col("id") % 100 < hot_pct, F.lit(0))
                    .otherwise(F.pmod(F.col("id"), F.lit(keys - 1)) + F.lit(1)))
        .withColumn("quantity", (F.col("id") % 7 + 1).cast("int")))
    dimension = (spark.range(keys).withColumnRenamed("id", "join_key")
                 .withColumn("multiplier", F.col("join_key") % 13 + 1))
    return facts, dimension


def salted_join(facts, dimension, salts: int = 8):
    """Split hot fact key across salts; replicate *only* hot dimension row.

    Use for a non-broadcastable dimension where heavy-key skew persists after AQE.
    This particular synthetic join is inner on a unique dimension business key.
    """
    if salts < 2:
        raise ValueError("salts must be >= 2")
    salted_facts = facts.withColumn(
        "salt", F.when(F.col("join_key") == 0,
                        F.pmod(F.xxhash64("id"), F.lit(salts))).otherwise(F.lit(0)))
    salted_dims = dimension.withColumn(
        "salt", F.explode(F.when(F.col("join_key") == 0,
                                 F.sequence(F.lit(0), F.lit(salts - 1)))
                          .otherwise(F.array(F.lit(0)))))
    return salted_facts.join(salted_dims, ["join_key", "salt"], "inner")


def measure(joined, label: str) -> dict:
    # Consume a column from both relations; count() alone can elide join payloads.
    result = joined.agg(F.count("id").alias("rows"),
                        F.sum(F.col("quantity") * F.col("multiplier")).alias("weighted_units"))
    print(f"\n==== {label}: physical plan (EXPLAIN FORMATTED) ====")
    result.explain(mode="formatted")
    start = time.perf_counter()
    row = result.collect()[0]
    return {"scenario": label, "rows": row["rows"],
            "weighted_units": row["weighted_units"],
            "wall_seconds": round(time.perf_counter() - start, 3)}


def run_benchmark(spark: SparkSession, rows=200000, keys=1000, hot_pct=65,
                  salts=8) -> dict:
    spark.conf.set("spark.sql.shuffle.partitions", "12")
    facts, dims = synthetic_join_data(spark, rows, keys, hot_pct)
    facts = facts.cache()
    dims = dims.cache()
    facts.count(); dims.count()  # exclude source generation from comparison
    try:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        baseline = measure(facts.join(dims, "join_key", "inner"), "forced_shuffle_baseline")

        # Small dimensions only: do NOT broadcast the real 100 GB interview dimension.
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        broadcast = measure(facts.join(F.broadcast(dims), "join_key", "inner"),
                            "broadcast_small_dimension")

        # Alternative when the dimension cannot fit memory / cannot broadcast.
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        salted = measure(salted_join(facts, dims, salts), "salt_hot_key")
        examples = [baseline, broadcast, salted]
        assert len({(x["rows"], x["weighted_units"]) for x in examples}) == 1, (
            "Join strategies produced different results")
        return {"input_rows": rows, "dimension_rows": keys, "hot_key_pct": hot_pct,
                "salts": salts, "scenarios": examples, "outputs_match": True,
                "note": "Scaled demo only; compare actual Spark UI task skew/spills before generalizing."}
    finally:
        facts.unpersist(); dims.unpersist()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--hot-pct", type=int, default=65)
    parser.add_argument("--salts", type=int, default=8)
    parser.add_argument("--output", type=Path, default=Path("reports/performance_benchmark.json"))
    args = parser.parse_args()
    spark = (SparkSession.builder.master("local[2]").appName("pharma-skew-performance-demo")
             .getOrCreate())
    try:
        report = run_benchmark(spark, args.rows, args.keys, args.hot_pct, args.salts)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print("\nCorrectness validated; report:", args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
