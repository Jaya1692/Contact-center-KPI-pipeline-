# Contact-Center KPI Pipeline (Databricks + Delta Lake, BigQuery)

A medallion (Bronze / Silver / Gold) pipeline on **synthetic** call-center data (no real customer data). It computes:

- **Bot-resolution rate:** share of contacts resolved by the bot without an agent.
- **SLA-breach rate:** share of agent-handled contacts whose wait exceeded the queue's SLA target.
- **Intent re-training candidates:** bot intents with enough volume and poor results, ranked by impact.

Built with PySpark and Delta Lake (runs on Databricks or locally), plus the same Gold logic in BigQuery SQL.

## Architecture

```
raw CSV files (two daily drops, with duplicates and bad rows)
      |
 BRONZE  Delta, append-only, all columns as text + _source_file + _ingest_ts        (nothing is dropped)
      |
 SILVER  typed (try_cast), validated, de-duplicated by contact_id (Delta MERGE, idempotent)
      |-----> silver_quarantine  (invalid rows kept with a reject_reason)
      |
 GOLD    daily_kpis (per date, queue, intent)  |  intent_retraining_candidates (ranked)
      |
 BigQuery: silver exported as Parquet -> bigquery/*.sql builds the same Gold tables (partitioned and clustered)
```

| Layer | Table | What it contains |
|---|---|---|
| Bronze | `bronze` | Raw rows as text, source file, ingest time |
| Silver | `silver` | One typed row per contact with `bot_resolved` and `sla_breached` flags |
| Silver | `silver_quarantine` | Rows that failed validation and why |
| Gold | `gold/daily_kpis` | Contacts, bot resolution rate, SLA breach rate, wait and handle time |
| Gold | `gold/intent_retraining_candidates` | Intents to re-train, ranked by `priority_score` |

## Interactive walkthrough
Open [docs/index.html](docs/index.html) in a browser (no server needed): a step-by-step flow, an explanation of every file, and small playgrounds to try the validation and KPI rules.

## KPI definitions
- **Bot resolved:** `escalated_to_agent = N` and `outcome = RESOLVED`.
- **SLA breached:** `escalated_to_agent = Y` and `wait_seconds > sla_target_seconds` (the SLA only applies to contacts that reach an agent).
- **Bot resolution rate:** bot-resolved contacts / all contacts.
- **SLA breach rate:** SLA breaches / contacts handed to an agent.
- **Re-training candidate:** at least 30 contacts and (bot resolution below 60%, or average bot confidence below 0.70, or average fallbacks of 1 or more). **Priority score** = contacts x (1 - bot resolution rate).

## Data quality rules (Silver)
`missing_contact_id`, `invalid_timestamp`, `unknown_outcome`, `invalid_escalation_flag`, `invalid_wait_seconds`,
`invalid_handle_seconds`, `invalid_sla_target`. Rejected rows go to `silver_quarantine`; duplicates (resent files) are
collapsed by keeping the most recently ingested version.

## Run it locally
Requires Java 17 and Python 3.10+ (on Windows, Spark writes need WSL or Docker; Linux and macOS work directly).

```bash
pip install -r requirements.txt
python scripts/generate_data.py data/raw
PYTHONPATH=src python -m kpi_pipeline.run_pipeline data/raw output
python -m pytest -q          # 10 tests
```

Expected on the generated data: 2,910 raw rows -> 28 quarantined, 25 duplicates removed, 2,857 Silver contacts.

## Run it on Databricks
1. Put the repository in a Databricks Git folder and the raw CSVs in a Unity Catalog volume (for example `/Volumes/main/kpi/raw`).
2. Run the notebooks in `databricks/notebooks/` in order (`01_bronze`, `02_silver`, `03_gold`), or deploy the job in
   `databricks/databricks.yml` (a daily workflow with task dependencies and a quarantine-rate gate).

## BigQuery
1. Export Silver as Parquet to Cloud Storage (see the end of `03_gold.py`).
2. Run `bigquery/01_load_silver.sql`, then `02_gold_daily_kpis.sql` and `03_intent_retraining_candidates.sql`.

The tests parse the Gold SQL as BigQuery SQL, run it (translated to DuckDB) on the Silver data, and check that the
numbers equal the Spark Gold results.

## What is verified and what is not
| Verified (automated tests, Linux, Spark 4 + Delta Lake) | Not verified |
|---|---|
| Counts through the layers, quarantine reasons, idempotent re-run, KPI flags and rates, retraining candidates | Running on a real Databricks workspace (notebooks, job bundle) |
| BigQuery Gold SQL gives the same numbers as the Spark Gold layer (via DuckDB) | Running on a real BigQuery project; `01_load_silver.sql` (LOAD DATA) was not parsed or run |

Status: working project on synthetic data; Databricks and BigQuery deployments are provided but not run on the real services.
