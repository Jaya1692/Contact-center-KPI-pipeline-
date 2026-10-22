"""Generate synthetic call-center interaction data (no real customer data).

Writes raw CSV files into data/raw/. Includes realistic problems on purpose:
resent duplicate rows, missing ids, negative durations, unparseable timestamps and unknown outcomes.
"""
import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

random.seed(2026)
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data" / "raw"
OUT.mkdir(parents=True, exist_ok=True)

QUEUES = {"Billing": 30, "Tech Support": 60, "Sales": 20, "Retention": 45}      # SLA target seconds per queue
# intent -> (probability the bot resolves it, average bot confidence)
INTENTS = {
    "check_balance": (0.92, 0.93), "reset_password": (0.85, 0.90), "update_address": (0.70, 0.82),
    "cancel_service": (0.25, 0.60), "billing_dispute": (0.20, 0.55), "upgrade_plan": (0.55, 0.75),
    "outage_report": (0.45, 0.68), "payment_arrangement": (0.35, 0.62),
}
HEADER = ["contact_id", "started_at", "channel", "queue", "intent", "bot_confidence", "bot_fallback_count",
          "escalated_to_agent", "outcome", "wait_seconds", "handle_seconds", "sla_target_seconds"]

start = datetime(2026, 10, 1)
rows = []
n = 0
for day in range(14):
    for _ in range(random.randint(180, 240)):
        n += 1
        intent = random.choice(list(INTENTS))
        p_resolve, conf = INTENTS[intent]
        queue = random.choice(list(QUEUES))
        started = start + timedelta(days=day, seconds=random.randint(7 * 3600, 21 * 3600))
        bot_resolves = random.random() < p_resolve
        fallback = 0 if bot_resolves else random.choice([0, 1, 1, 2, 3])
        confidence = round(min(0.99, max(0.2, random.gauss(conf, 0.06))), 2)
        if bot_resolves:
            escalated, outcome, wait, handle = "N", "RESOLVED", 0, random.randint(30, 240)
        else:
            escalated = "Y"
            wait = max(0, int(random.gauss(QUEUES[queue] * 0.9, QUEUES[queue] * 0.6)))
            abandoned = random.random() < 0.06
            outcome = "ABANDONED" if abandoned else random.choice(["RESOLVED", "RESOLVED", "UNRESOLVED"])
            handle = 0 if abandoned else random.randint(120, 900)
        rows.append([f"C{n:06d}", started.strftime("%Y-%m-%d %H:%M:%S"), random.choice(["CHAT", "VOICE"]), queue,
                     intent, confidence, fallback, escalated, outcome, wait, handle, QUEUES[queue]])

# deliberately dirty rows
for i in random.sample(range(len(rows)), 25):                 # resent duplicates
    rows.append(list(rows[i]))
for i in random.sample(range(len(rows) - 25), 8):             # missing contact id
    r = list(rows[i]); r[0] = ""; rows.append(r)
for i in random.sample(range(len(rows) - 33), 8):             # negative wait
    r = list(rows[i]); r[0] = r[0] + "X"; r[9] = -5; rows.append(r)
for i in random.sample(range(len(rows) - 41), 6):             # unparseable timestamp
    r = list(rows[i]); r[0] = r[0] + "T"; r[1] = "not-a-date"; rows.append(r)
for i in random.sample(range(len(rows) - 47), 6):             # unknown outcome
    r = list(rows[i]); r[0] = r[0] + "O"; r[8] = "WEIRD"; rows.append(r)

random.shuffle(rows)
half = len(rows) // 2                                          # two files, like two daily drops
for name, chunk in (("calls_part1.csv", rows[:half]), ("calls_part2.csv", rows[half:])):
    with open(OUT / name, "w", newline="") as f:
        w = csv.writer(f); w.writerow(HEADER); w.writerows(chunk)
print(f"wrote {len(rows)} rows to {OUT} ({n} clean contacts, 25 duplicates, 28 bad rows)")
