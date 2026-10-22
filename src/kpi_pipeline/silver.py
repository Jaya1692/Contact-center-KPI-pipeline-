"""Silver layer: typed, validated, de-duplicated contacts. Invalid rows go to a quarantine table with a reason."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VALID_OUTCOMES = ["RESOLVED", "UNRESOLVED", "ABANDONED"]


def _typed(bronze: DataFrame) -> DataFrame:
    """Cast text columns to proper types. try_cast / try_to_timestamp return null instead of failing on bad data."""
    return (
        bronze.withColumn("started_ts", F.try_to_timestamp("started_at"))
        .withColumn("bot_confidence_d", F.expr("try_cast(bot_confidence as double)"))
        .withColumn("bot_fallback_count_i", F.expr("try_cast(bot_fallback_count as int)"))
        .withColumn("wait_seconds_i", F.expr("try_cast(wait_seconds as int)"))
        .withColumn("handle_seconds_i", F.expr("try_cast(handle_seconds as int)"))
        .withColumn("sla_target_seconds_i", F.expr("try_cast(sla_target_seconds as int)"))
    )


def _reject_reason() -> F.Column:
    """First validation rule that fails, or null when the row is valid."""
    return (
        F.when(F.col("contact_id").isNull() | (F.trim(F.col("contact_id")) == ""), F.lit("missing_contact_id"))
        .when(F.col("started_ts").isNull(), F.lit("invalid_timestamp"))
        .when(~F.col("outcome").isin(VALID_OUTCOMES), F.lit("unknown_outcome"))
        .when(~F.col("escalated_to_agent").isin("Y", "N"), F.lit("invalid_escalation_flag"))
        .when(F.col("wait_seconds_i").isNull() | (F.col("wait_seconds_i") < 0), F.lit("invalid_wait_seconds"))
        .when(F.col("handle_seconds_i").isNull() | (F.col("handle_seconds_i") < 0), F.lit("invalid_handle_seconds"))
        .when(F.col("sla_target_seconds_i").isNull() | (F.col("sla_target_seconds_i") <= 0), F.lit("invalid_sla_target"))
    )


def build_silver(spark: SparkSession, bronze_path: str, silver_path: str, quarantine_path: str) -> dict:
    """Validate and upsert Bronze data into Silver (Delta MERGE keeps reruns idempotent).

    Returns counts: valid, quarantined, duplicates_removed.
    """
    bronze = spark.read.format("delta").load(bronze_path)
    typed = _typed(bronze).withColumn("reject_reason", _reject_reason())

    # 1) quarantine: keep the original text values and the reason
    quarantine = typed.filter(F.col("reject_reason").isNotNull()).select(
        *[c for c in bronze.columns], "reject_reason"
    )
    quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(quarantine_path)

    # 2) valid rows: de-duplicate on contact_id, keeping the most recently ingested version
    valid = typed.filter(F.col("reject_reason").isNull())
    w = Window.partitionBy("contact_id").orderBy(F.col("_ingest_ts").desc(), F.col("_source_file").desc())
    deduped = valid.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    silver = deduped.select(
        "contact_id",
        F.col("started_ts").alias("started_at"),
        F.to_date("started_ts").alias("contact_date"),
        "channel", "queue", "intent",
        F.col("bot_confidence_d").alias("bot_confidence"),
        F.col("bot_fallback_count_i").alias("bot_fallback_count"),
        (F.col("escalated_to_agent") == "Y").alias("escalated_to_agent"),
        "outcome",
        F.col("wait_seconds_i").alias("wait_seconds"),
        F.col("handle_seconds_i").alias("handle_seconds"),
        F.col("sla_target_seconds_i").alias("sla_target_seconds"),
        # business flags used by the KPIs
        ((F.col("escalated_to_agent") == "N") & (F.col("outcome") == "RESOLVED")).alias("bot_resolved"),
        ((F.col("escalated_to_agent") == "Y") & (F.col("wait_seconds_i") > F.col("sla_target_seconds_i"))).alias("sla_breached"),
        F.current_timestamp().alias("_silver_ts"),
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        (DeltaTable.forPath(spark, silver_path).alias("t")
         .merge(silver.alias("s"), "t.contact_id = s.contact_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        silver.write.format("delta").partitionBy("contact_date").save(silver_path)

    n_valid_rows = valid.count()
    return {
        "valid": silver.count(),
        "quarantined": quarantine.count(),
        "duplicates_removed": n_valid_rows - silver.count(),
    }
