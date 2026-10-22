-- Load the Silver contacts exported from the Delta table (Parquet files) into BigQuery.
-- Replace BUCKET with your Cloud Storage bucket. Run in the BigQuery console or with `bq query`.
CREATE SCHEMA IF NOT EXISTS `kpi`;

LOAD DATA OVERWRITE `kpi.silver_contacts`
(
  contact_id STRING, started_at TIMESTAMP, contact_date DATE, channel STRING, queue STRING, intent STRING,
  bot_confidence FLOAT64, bot_fallback_count INT64, escalated_to_agent BOOL, outcome STRING,
  wait_seconds INT64, handle_seconds INT64, sla_target_seconds INT64, bot_resolved BOOL, sla_breached BOOL
)
PARTITION BY contact_date
CLUSTER BY queue, intent
FROM FILES (format = 'PARQUET', uris = ['gs://BUCKET/kpi/silver/*.parquet']);
