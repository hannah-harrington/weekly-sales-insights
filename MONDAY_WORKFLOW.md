# Weekly Sales Insights — Monday Update

**Time: ~5 minutes**

> **Note for EMEA/APAC:** This workflow is written for the NA region. If you're running a fork for your own region, the steps are identical — just use your own CSV folder, deploy site name, and Slack channel. See the "For Fork Owners" section at the bottom.

## Step 1: Export CSVs from Demandbase

Log into Demandbase and export these reports as CSV:

**Weekly signals (run every week):**
1. New Accounts Moved to MQA in Last Week (New MQA)
2. Accounts Visiting High Value Pages with Lost Opp in Last 12 Months
3. Accounts Visiting High Value Pages (all accounts)
4. Newly Engaged People This Week _(skip if inaccurate that week)_
5. Newly Engaged People This Week - Activity Report

**Snapshot (run once, or refresh periodically):**
6. All Accounts at MQA (ENT_Acq_MQA Journey Stage Account List)

## Step 2: Drop them into a dated subfolder

Create a folder named with the date and save all CSVs into it:

```
Desktop/Cursor Brain/Demandbase weeklys/March 9/
```

The pipeline auto-detects reports by filename. If a report is missing, the pipeline still runs with whatever is there and prints a warning.

## Step 3: Tell the Cursor agent

Paste this into the Cursor agent chat (replace the date with the current week):

> The Demandbase CSVs are in Desktop/Cursor Brain/Demandbase weeklys/March 9/. Run the weekly sales insights pipeline, deploy to Quick, and send Slack DMs.

The agent will run from the `sales-insights/` directory:
```bash
cd ~/Desktop/"Cursor Brain"/sales-insights && .venv/bin/python3 -m pipeline.ingest --input-dir "../Demandbase weeklys/March 16" --deploy --notify
```

> **Important:** Use `.venv/bin/python3` (not plain `python3`) — the virtual environment has `openpyxl` installed, which is required for Sales Nav Top Leads integration.

This will:
- Read all CSVs from the dated subfolder
- Load Sales Nav leads from `CG_Sales_Nav_Leads_CLEAN.xlsx` and cross-reference with Demandbase intent
- Build the JSON data model
- Write to site/data/
- Archive the CSVs
- Deploy to https://sales-insights-hub.quick.shopify.io
- DM every rep who has signals this week with a personalized link to their report

> **Note:** Slack DMs require `SLACK_BOT_TOKEN` in your environment. Add it to `~/.zshrc`:
> ```bash
> export SLACK_BOT_TOKEN="xoxb-your-token-here"
> ```
> If the token is missing, the pipeline still runs — it just skips the DMs and prints a warning.

To deploy without DMs: `.venv/bin/python3 -m pipeline.ingest --deploy`
To send DMs without deploying: `.venv/bin/python3 -m pipeline.ingest --notify`

> **Deploy note:** Quick now requires interactive terminal confirmation. If deploy fails from the agent, run manually:
> ```bash
> cd ~/Desktop/"Cursor Brain"/sales-insights/site && quick deploy . sales-insights-hub
> ```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| "No CSV files found" | CSVs aren't in the folder or wrong directory |
| Script errors | Paste the error into Cursor chat and ask to fix it |
| Need to backfill a past week | Ask: "Run the weekly report for [date] and deploy" |
| Want to preview before deploying | Ask: "Run the weekly report but don't deploy yet" |

---

## For Fork Owners (EMEA, APAC, etc.)

If you're running a forked version for your region, the Monday workflow is the same with these changes:

1. **Your CSVs** come from your region's Demandbase exports (same 4 report types)
2. **Your folder** is whatever you set `CSV_INPUT_DIR` to in your `pipeline/config.py`
3. **Your deploy target** is whatever you set `DEPLOY_SITE_NAME` to (e.g. `emea-sales-insights`)
4. **Your Slack DMs** will link to your site (e.g. `emea-sales-insights.quick.shopify.io`)

### Staying current with upstream

After your Monday deploy, pull in any improvements Hannah has shipped:

> Pull the latest upstream changes from hannah-harrington/weekly-sales-insights into my fork.

This keeps your fork up to date with new features and bug fixes without overwriting your region's config. If you haven't pulled in a while, doing it weekly on Mondays keeps merge conflicts small.

---

## Technical Details

- **Pipeline location:** `sales-insights/pipeline/`
- **Site location:** `sales-insights/site/`
- **Config (team mapping):** `sales-insights/pipeline/config.py`
- **Sales Nav leads:** `Cursor Brain/CG_Sales_Nav_Leads_CLEAN.xlsx` (Consumer team only, 659 leads)
- **Virtual environment:** `sales-insights/.venv/` (has `openpyxl` for Excel reading)
- **Live site (NA):** https://sales-insights-hub.quick.shopify.io
- **JSON archive:** `sales-insights/site/data/` (one file per week)
- **CSV archive:** `sales-insights/archive/` (raw CSVs stored by date)
