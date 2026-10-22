"""Checks the BigQuery SQL: it must parse as BigQuery SQL and give the same numbers as the Spark Gold layer.

The SELECT inside each CREATE TABLE ... AS is transpiled to DuckDB and run on the Silver data, because a real
BigQuery project is not available in tests.
"""
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import sqlglot
from sqlglot import exp

BQ = Path(__file__).resolve().parent.parent / "bigquery"


def _select_from(file_name: str) -> str:
    tree = sqlglot.parse_one((BQ / file_name).read_text(), read="bigquery")
    assert isinstance(tree, exp.Create), "expected CREATE TABLE ... AS SELECT"
    query = tree.expression
    for t in query.find_all(exp.Table):          # `kpi.silver_contacts` -> silver_contacts for the DuckDB test
        t.set("db", None)
    return query.sql(dialect="duckdb")


# 01_load_silver.sql uses BigQuery LOAD DATA, which sqlglot cannot parse, so it is checked with a BigQuery dry run instead
@pytest.mark.parametrize("name", ["02_gold_daily_kpis.sql", "03_intent_retraining_candidates.sql"])
def test_sql_parses_as_bigquery(name):
    assert sqlglot.parse((BQ / name).read_text(), read="bigquery")


def test_bigquery_gold_matches_spark_gold(spark, pipeline_output):
    base, _ = pipeline_output
    silver = spark.read.format("delta").load(f"{base}/silver").toPandas()
    con = duckdb.connect()
    con.register("silver_contacts", silver)

    bq_daily = con.sql(_select_from("02_gold_daily_kpis.sql")).df()
    spark_daily = spark.read.format("delta").load(f"{base}/gold/daily_kpis").toPandas()
    key = ["contact_date", "queue", "intent"]
    bq_daily["contact_date"] = pd.to_datetime(bq_daily["contact_date"])        # DuckDB returns timestamps, Spark returns dates
    spark_daily["contact_date"] = pd.to_datetime(spark_daily["contact_date"])
    m = bq_daily.merge(spark_daily, on=key, suffixes=("_bq", "_sp"))
    assert len(m) == len(spark_daily) == len(bq_daily)
    for col in ["contacts", "bot_resolved", "escalated", "sla_breaches"]:
        assert (m[f"{col}_bq"] == m[f"{col}_sp"]).all(), col
    for col in ["bot_resolution_rate", "sla_breach_rate", "avg_wait_seconds", "avg_handle_seconds"]:
        a, b = m[f"{col}_bq"].astype(float), m[f"{col}_sp"].astype(float)
        assert ((a - b).abs().fillna(0) < 1e-9).all(), col
        assert (a.isna() == b.isna()).all(), col

    bq_c = con.sql(_select_from("03_intent_retraining_candidates.sql")).df()
    sp_c = spark.read.format("delta").load(f"{base}/gold/intent_retraining_candidates").toPandas()
    assert list(bq_c["intent"]) == list(sp_c["intent"])
    assert ((bq_c["priority_score"] - sp_c["priority_score"]).abs() < 1e-9).all()
