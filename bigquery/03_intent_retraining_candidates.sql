-- Intents that need NLU re-training: enough volume and at least one problem
-- (low bot resolution, low confidence, or frequent fallbacks). Highest impact first.
CREATE OR REPLACE TABLE `kpi.gold_intent_retraining_candidates`
AS
WITH intents AS (
  SELECT
    intent,
    COUNT(*)                                  AS contacts,
    AVG(CAST(bot_resolved AS INT64))          AS bot_resolution_rate,
    AVG(CAST(escalated_to_agent AS INT64))    AS escalation_rate,
    AVG(bot_fallback_count)                   AS avg_fallbacks,
    AVG(bot_confidence)                       AS avg_confidence
  FROM `kpi.silver_contacts`
  GROUP BY intent
)
SELECT
  intent, contacts, bot_resolution_rate, escalation_rate, avg_fallbacks, avg_confidence,
  contacts * (1 - bot_resolution_rate) AS priority_score
FROM intents
WHERE contacts >= 30
  AND (bot_resolution_rate < 0.60 OR avg_confidence < 0.70 OR avg_fallbacks >= 1.0)
ORDER BY priority_score DESC;
