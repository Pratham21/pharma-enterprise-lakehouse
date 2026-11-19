# Databricks notebook source
"""Provision demo schemas and a governed landing volume. Unity Catalog required."""

dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")
# Only use a trusted catalog name configured by the repository owner.
if not catalog.replace("_", "").isalnum():
    raise ValueError("Catalog must be an alphanumeric/underscore identifier")

for schema in ("pharma_admin", "pharma_bronze", "pharma_silver", "pharma_gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`pharma_admin`.`landing`")
print("Ready:", f"{catalog}.pharma_admin.landing")
print("Permissions required: USE CATALOG, CREATE SCHEMA, CREATE VOLUME, and write access")
