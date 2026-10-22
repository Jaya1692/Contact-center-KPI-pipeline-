# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver: typed, validated, de-duplicated contacts; bad rows go to a quarantine table

# COMMAND ----------
import sys

sys.path.append("../../src")
from kpi_pipeline.silver import build_silver

dbutils.widgets.text("base_path", "/Volumes/main/kpi/lake")
base_path = dbutils.widgets.get("base_path")

# COMMAND ----------
result = build_silver(spark, f"{base_path}/bronze", f"{base_path}/silver", f"{base_path}/silver_quarantine")
print(result)

# Fail the task when too many rows are rejected (a data quality gate for the workflow)
total = result["valid"] + result["quarantined"] + result["duplicates_removed"]
if total and result["quarantined"] / total > 0.05:
    raise Exception(f"Quarantine rate too high: {result['quarantined']} of {total} rows")
