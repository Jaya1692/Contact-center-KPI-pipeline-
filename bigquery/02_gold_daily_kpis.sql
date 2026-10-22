-- Daily KPIs per queue and intent. Same logic as the Spark Gold layer.
-- sla_breach_rate is measured only on contacts handed to an agent (the SLA applies to them).
CREATE OR REPLACE TABLE `kpi.gold_daily_kpis`
PARTITION BY contact_date
CLUSTER BY queue, intent
AS
SELECT
  contact_date,
  queue,
  intent,
  COUNT(*)                                          AS contacts,
  COUNTIF(bot_resolved)                             AS bot_resolved,
  COUNTIF(escalated_to_agent)                       AS escalated,
  COUNTIF(sla_breached)                             AS sla_breaches,
  AVG(wait_seconds)                                 AS avg_wait_seconds,
  AVG(handle_seconds)                               AS avg_handle_seconds,
  SAFE_DIVIDE(COUNTIF(bot_resolved), COUNT(*))      AS bot_resolution_rate,
  SAFE_DIVIDE(COUNTIF(sla_breached), COUNTIF(escalated_to_agent)) AS sla_breach_rate
FROM `kpi.silver_contacts`
GROUP BY contact_date, queue, intent;
