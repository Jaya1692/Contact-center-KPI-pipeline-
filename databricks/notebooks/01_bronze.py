# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze: land raw call-center files as Delta (all text, plus ingestion metadata)

# COMMAND ----------
import sys

# Repo layout: this notebook is in databricks/notebooks, the Python package is in ../../src (Databricks Repos / Git folder)
sys.path.append("../../src")

from kpi_pipeline.bronze import ingest_bronze

dbutils.widgets.text("raw_path", "/Volumes/main/kpi/raw")
dbutils.widgets.text("base_path", "/Volumes/main/kpi/lake")
raw_path, base_path = dbutils.widgets.get("raw_path"), dbutils.widgets.get("base_path")

# COMMAND ----------
rows = ingest_bronze(spark, raw_path, f"{base_path}/bronze")   # `spark` already exists on Databricks
print(f"appended {rows} rows to Bronze")
