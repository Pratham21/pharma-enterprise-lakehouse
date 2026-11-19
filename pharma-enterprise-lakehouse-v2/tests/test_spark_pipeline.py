"""Integration tests requiring PySpark + Delta JVM artifacts.

The local offline environment may not have the packages/jars. Tests skip unless
REQUIRE_SPARK_TESTS=1, which makes CI fail clearly if Spark cannot initialize.
"""
import os
import tempfile
import unittest
from pathlib import Path

from pharma_demo.synthetic import generate

try:
    from pyspark.sql import functions as F
    from pharma_demo.spark_demo import make_spark
    from pharma_demo.spark_pipeline import (apply_cdc_batch, gold_metrics,
                                            quality_split, read_delta)
    from pharma_demo.performance import synthetic_join_data, salted_join
    PACKAGES = True
except ImportError:
    PACKAGES = False


@unittest.skipUnless(PACKAGES, "Requires pip install -e '.[spark]' and Delta JVM jars")
class SparkPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.spark = make_spark()
        except Exception as exc:
            if os.environ.get("REQUIRE_SPARK_TESTS") == "1":
                raise
            raise unittest.SkipTest(f"Delta Spark runtime unavailable: {exc}")
        cls.tmp = tempfile.TemporaryDirectory()
        cls.dest = {name: str(Path(cls.tmp.name) / name)
                    for name in ("quarantine", "history", "current", "scd2")}
        cls.data = generate(n=200)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "tmp"):
            cls.tmp.cleanup()
        if hasattr(cls, "spark"):
            cls.spark.stop()

    def test_01_batch_one(self):
        raw = self.spark.createDataFrame(self.data["batch_1"])
        result = apply_cdc_batch(self.spark, raw, self.dest, by_path=True)
        self.assertEqual(result, {"valid_events": 200, "quarantined_events": 0})
        self.assertEqual(read_delta(self.spark, self.dest["current"], True).count(), 200)

    def test_02_batch_two_scd2_and_quarantine(self):
        raw = self.spark.createDataFrame(self.data["batch_2"])
        result = apply_cdc_batch(self.spark, raw, self.dest, by_path=True)
        self.assertEqual(result["quarantined_events"], 1)
        current = read_delta(self.spark, self.dest["current"], True)
        self.assertEqual(current.filter(~F.col("is_deleted")).count(), 180)
        self.assertEqual(current.filter(F.col("is_deleted")).count(), 20)
        self.assertEqual(read_delta(self.spark, self.dest["history"], True).count(), 341)
        self.assertEqual(read_delta(self.spark, self.dest["scd2"], True).count(), 340)
        self.assertEqual(read_delta(self.spark, self.dest["quarantine"], True).count(), 1)
        self.assertEqual(gold_metrics(current).agg(F.sum("rx_count")).first()[0], 180)
        rx1_versions = (read_delta(self.spark, self.dest["scd2"], True)
                        .filter(F.col("rx_id") == "RX000001")
                        .orderBy("event_seq").collect())
        self.assertEqual([r["event_seq"] for r in rx1_versions], [1, 2, 3])
        self.assertEqual([r["is_current"] for r in rx1_versions], [False, False, True])
        self.assertEqual([r["is_deleted"] for r in rx1_versions], [False, False, True])
        self.assertEqual(rx1_versions[0]["effective_to"], rx1_versions[1]["effective_from"])

    def test_03_replay_does_not_change_tables(self):
        previous = {name: read_delta(self.spark, path, True).count()
                    for name, path in self.dest.items()}
        apply_cdc_batch(self.spark, self.spark.createDataFrame(self.data["batch_2"]),
                        self.dest, by_path=True)
        current = {name: read_delta(self.spark, path, True).count()
                   for name, path in self.dest.items()}
        self.assertEqual(previous, current)

    def test_04_salted_join_matches_baseline(self):
        fact, dim = synthetic_join_data(self.spark, rows=1000, keys=20, hot_pct=70)
        base = fact.join(dim, "join_key")
        salted = salted_join(fact, dim, salts=4)
        def score(df):
            return df.agg(F.count("id"), F.sum(F.col("quantity") * F.col("multiplier"))).first()
        self.assertEqual(tuple(score(base)), tuple(score(salted)))


if __name__ == "__main__":
    unittest.main()
