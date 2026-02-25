# Weekly Sales Insights — Monday Update

**Time: ~5 minutes**

## Step 1: Export 4 CSVs from Demandbase

Log into Demandbase and export these 4 reports as CSV:
1. Accounts Moved to MQA in Last Week w/ No Sales Touches
2. Accounts Visiting High Value Pages with Lost Opp in Last 12 Months
3. Newly Engaged People This Week
4. Newly Engaged People This Week - Activity Report

## Step 2: Drop them into the folder

Move/save all 4 CSVs into:

```
Desktop/Cursor Brain/Demandbase weeklys/
```

Just drop them in the top level — the script auto-detects them by filename. Overwrite any leftover files from last week.

## Step 3: Tell the Cursor agent

Paste this into the Cursor agent chat:

> The 4 Demandbase CSVs are in Desktop/Cursor Brain/Demandbase weeklys/. Run the weekly sales insights pipeline and deploy to Quick.

The agent will run from the `sales-insights/` directory:
```bash
cd ~/Desktop/"Cursor Brain"/sales-insights && python3 -m pipeline.ingest --deploy
```

This will:
- Read all 4 CSVs
- Build the JSON data model
- Write to site/data/
- Archive the CSVs
- Deploy to https://weekly-sales-insights.quick.shopify.io

## Step 4: Share the link

Post in Slack:

> New weekly sales insights are live! Find your name and open your report: https://weekly-sales-insights.quick.shopify.io

---

## Troubleshooting

| Problem | Fix |
|---|---|
| "No CSV files found" | CSVs aren't in the folder or wrong directory |
| Script errors | Paste the error into Cursor chat and ask to fix it |
| Need to backfill a past week | Ask: "Run the weekly report for [date] and deploy" |
| Want to preview before deploying | Ask: "Run the weekly report but don't deploy yet" |

---

## Technical Details

- **Pipeline location:** `sales-insights/pipeline/`
- **Site location:** `sales-insights/site/`
- **Config (team mapping):** `sales-insights/pipeline/config.py`
- **Live site:** https://weekly-sales-insights.quick.shopify.io
- **JSON archive:** `sales-insights/site/data/` (one file per week)
- **CSV archive:** `sales-insights/archive/` (raw CSVs stored by date)
