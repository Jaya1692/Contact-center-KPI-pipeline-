"""Gold layer: business KPIs and the intent re-training candidate list."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# An intent becomes a re-training candidate when it has enough volume AND at least one of these problems
MIN_VOLUME = 30
MAX_BOT_RESOLUTION_RATE = 0.60
MAX_AVG_CONFIDENCE = 0.70
MIN_AVG_FALLBACKS = 1.0


def build_gold(spark: SparkSession, silver_path: str, gold_path: str) -> dict:
    silver = spark.read.format("delta").load(silver_path)

    # 1) daily KPIs per queue and intent
    daily = (
        silver.groupBy("contact_date", "queue", "intent")
        .agg(
            F.count("*").alias("contacts"),
            F.sum(F.col("bot_resolved").cast("int")).alias("bot_resolved"),
            F.sum(F.col("escalated_to_agent").cast("int")).alias("escalated"),
            F.sum(F.col("sla_breached").cast("int")).alias("sla_breaches"),
            F.avg("wait_seconds").alias("avg_wait_seconds"),
            F.avg("handle_seconds").alias("avg_handle_seconds"),
        )
        .withColumn("bot_resolution_rate", F.col("bot_resolved") / F.col("contacts"))
        # SLA breach rate is measured only on contacts that were handed to an agent (the SLA applies to them)
        .withColumn("sla_breach_rate", F.when(F.col("escalated") > 0, F.col("sla_breaches") / F.col("escalated")))
    )
    daily.write.format("delta").mode("overwrite").partitionBy("contact_date").save(f"{gold_path}/daily_kpis")

    # 2) intent re-training candidates
    intents = (
        silver.groupBy("intent")
        .agg(
            F.count("*").alias("contacts"),
            F.avg(F.col("bot_resolved").cast("double")).alias("bot_resolution_rate"),
            F.avg(F.col("escalated_to_agent").cast("double")).alias("escalation_rate"),
            F.avg("bot_fallback_count").alias("avg_fallbacks"),
            F.avg("bot_confidence").alias("avg_confidence"),
        )
    )
    candidates = (
        intents.filter(
            (F.col("contacts") >= MIN_VOLUME)
            & (
                (F.col("bot_resolution_rate") < MAX_BOT_RESOLUTION_RATE)
                | (F.col("avg_confidence") < MAX_AVG_CONFIDENCE)
                | (F.col("avg_fallbacks") >= MIN_AVG_FALLBACKS)
            )
        )
        # priority: how many contacts the bot is failing to resolve for this intent
        .withColumn("priority_score", F.col("contacts") * (1 - F.col("bot_resolution_rate")))
        .orderBy(F.col("priority_score").desc())
    )
    candidates.write.format("delta").mode("overwrite").save(f"{gold_path}/intent_retraining_candidates")

    return {"daily_rows": daily.count(), "retraining_candidates": candidates.count()}
