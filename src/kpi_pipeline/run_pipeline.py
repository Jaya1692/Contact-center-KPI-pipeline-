"""Run Bronze -> Silver -> Gold locally:  python -m kpi_pipeline.run_pipeline data/raw output"""
import sys

from kpi_pipeline.bronze import ingest_bronze
from kpi_pipeline.gold import build_gold
from kpi_pipeline.silver import build_silver
from kpi_pipeline.spark_session import get_spark


def run(raw_path: str, base_path: str, spark=None) -> dict:
    spark = spark or get_spark()
    bronze, silver, quarantine, gold = (f"{base_path}/{x}" for x in ("bronze", "silver", "silver_quarantine", "gold"))
    result = {"bronze_rows": ingest_bronze(spark, raw_path, bronze)}
    result.update(build_silver(spark, bronze, silver, quarantine))
    result.update(build_gold(spark, silver, gold))
    return result


if __name__ == "__main__":
    print(run(sys.argv[1], sys.argv[2]))
