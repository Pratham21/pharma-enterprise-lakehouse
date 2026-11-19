# Databricks notebook source
"""Scalable but deliberately small performance lab. No large-cluster benchmark claim."""
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd().parent / "src"))
from pharma_demo.performance import run_benchmark

dbutils.widgets.text("rows", "200000")
dbutils.widgets.text("keys", "1000")
dbutils.widgets.text("hot_pct", "65")
dbutils.widgets.text("salts", "8")

result = run_benchmark(spark, rows=int(dbutils.widgets.get("rows")),
                       keys=int(dbutils.widgets.get("keys")),
                       hot_pct=int(dbutils.widgets.get("hot_pct")),
                       salts=int(dbutils.widgets.get("salts")))
print(json.dumps(result, indent=2))
print("Open Spark UI > Stages / SQL and compare shuffle bytes, spill, task duration, skew.")
