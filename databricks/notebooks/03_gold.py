# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold: daily KPIs (bot resolution, SLA breach) and intent re-training candidates

# COMMAND ----------
import sys

sys.path.append("../../src")
from kpi_pipeline.gold import build_gold

dbutils.widgets.text("base_path", "/Volumes/main/kpi/lake")
base_path = dbutils.widgets.get("base_path")

# COMMAND ----------
print(build_gold(spark, f"{base_path}/silver", f"{base_path}/gold"))

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Example: weakest intents first (replace the path with your lake location, or register Gold tables in Unity Catalog)
# MAGIC SELECT * FROM delta.`/Volumes/main/kpi/lake/gold/intent_retraining_candidates` ORDER BY priority_score DESC

# COMMAND ----------
# MAGIC %md
# MAGIC ## Export Silver to Parquet for BigQuery
# MAGIC `spark.read.format("delta").load(f"{base_path}/silver").write.mode("overwrite").parquet("gs://BUCKET/kpi/silver")`
