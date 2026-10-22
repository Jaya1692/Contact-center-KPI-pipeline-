import pandas as pd

from kpi_pipeline.silver import build_silver


def test_counts_through_the_layers(pipeline_output):
    _, r = pipeline_output
    assert r["bronze_rows"] == 2910          # every raw row is kept in Bronze
    assert r["quarantined"] == 28            # 8 missing id + 8 negative wait + 6 bad timestamp + 6 unknown outcome
    assert r["duplicates_removed"] == 25     # resent rows are collapsed to one
    assert r["valid"] == 2857                # 2910 - 28 - 25


def test_quarantine_reasons(spark, pipeline_output):
    base, _ = pipeline_output
    q = spark.read.format("delta").load(f"{base}/silver_quarantine").toPandas()
    counts = q["reject_reason"].value_counts().to_dict()
    assert counts == {"missing_contact_id": 8, "invalid_wait_seconds": 8, "invalid_timestamp": 6, "unknown_outcome": 6}


def test_silver_has_no_duplicates_and_correct_types(spark, pipeline_output):
    base, _ = pipeline_output
    s = spark.read.format("delta").load(f"{base}/silver")
    assert s.count() == s.select("contact_id").distinct().count()
    types = dict(s.dtypes)
    assert types["started_at"] == "timestamp" and types["wait_seconds"] == "int" and types["bot_resolved"] == "boolean"


def test_silver_flags_follow_the_business_rules(spark, pipeline_output):
    base, _ = pipeline_output
    s = spark.read.format("delta").load(f"{base}/silver").toPandas()
    expect_bot = (~s["escalated_to_agent"]) & (s["outcome"] == "RESOLVED")
    expect_breach = s["escalated_to_agent"] & (s["wait_seconds"] > s["sla_target_seconds"])
    assert (s["bot_resolved"] == expect_bot).all()
    assert (s["sla_breached"] == expect_breach).all()
    assert not (s["bot_resolved"] & s["escalated_to_agent"]).any()    # a contact cannot be both


def test_gold_kpis_match_independent_pandas_calculation(spark, pipeline_output):
    base, _ = pipeline_output
    s = spark.read.format("delta").load(f"{base}/silver").toPandas()
    g = spark.read.format("delta").load(f"{base}/gold/daily_kpis").toPandas()
    assert g["contacts"].sum() == len(s)
    expected = (s.groupby("contact_date")
                .agg(contacts=("contact_id", "count"), bot=("bot_resolved", "sum"),
                     esc=("escalated_to_agent", "sum"), breach=("sla_breached", "sum")).reset_index())
    got = g.groupby("contact_date").agg(contacts=("contacts", "sum"), bot=("bot_resolved", "sum"),
                                         esc=("escalated", "sum"), breach=("sla_breaches", "sum")).reset_index()
    pd.testing.assert_frame_equal(expected.sort_values("contact_date").reset_index(drop=True),
                                  got.sort_values("contact_date").reset_index(drop=True), check_dtype=False)
    assert g["bot_resolution_rate"].between(0, 1).all()
    assert g["sla_breach_rate"].dropna().between(0, 1).all()


def test_retraining_candidates_pick_the_weak_intents(spark, pipeline_output):
    base, _ = pipeline_output
    c = spark.read.format("delta").load(f"{base}/gold/intent_retraining_candidates").toPandas()
    names = list(c["intent"])
    assert "billing_dispute" in names and "cancel_service" in names
    assert "check_balance" not in names and "reset_password" not in names
    assert list(c["priority_score"]) == sorted(c["priority_score"], reverse=True)     # highest impact first


def test_silver_rerun_is_idempotent(spark, pipeline_output):
    base, result = pipeline_output
    again = build_silver(spark, f"{base}/bronze", f"{base}/silver", f"{base}/silver_quarantine")
    assert again["valid"] == result["valid"]
    assert spark.read.format("delta").load(f"{base}/silver").count() == result["valid"]
