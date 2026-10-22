"""Bronze layer: land raw files exactly as received (all columns as text) plus ingestion metadata."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW_COLUMNS = [
    "contact_id", "started_at", "channel", "queue", "intent", "bot_confidence", "bot_fallback_count",
    "escalated_to_agent", "outcome", "wait_seconds", "handle_seconds", "sla_target_seconds",
]


def ingest_bronze(spark: SparkSession, raw_path: str, bronze_path: str) -> int:
    """Read every CSV under raw_path as strings (no schema guessing) and append it to the Bronze Delta table.

    Nothing is cleaned or dropped here: bad rows are kept so they can be traced and replayed.
    Returns the number of rows appended.
    """
    df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")          # keep everything as string in Bronze
        .csv(raw_path)
        .select(*RAW_COLUMNS, F.col("_metadata.file_path").alias("_source_file"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )
    df.write.format("delta").mode("append").save(bronze_path)
    return df.count()
