# Contributing to Weekly Sales Insights

This site lives at [weekly-sales-insights.quick.shopify.io](https://weekly-sales-insights.quick.shopify.io) and the source code is on GitHub at [hannah-harrington/weekly-sales-insights](https://github.com/hannah-harrington/weekly-sales-insights).

There are two ways to work with this repo: **edit the existing site** or **fork it to build your own version**. Every step below includes the exact Cursor prompt you can paste to have the agent do it for you.

---

## Option A: Edit the Existing Site

Use this when you want to update the team config, fix a bug, or improve the site design.

### 1. Get access

Ask Hannah to add you as a collaborator on [the repo](https://github.com/hannah-harrington/weekly-sales-insights). You'll need a GitHub account.

### 2. Clone the repo

Open Cursor and paste this prompt:

> Clone the repo git@github.com:hannah-harrington/weekly-sales-insights.git to my Desktop and open it.

### 3. Make your changes

Describe what you want in plain English. Here are some example prompts:

**Update the rep list:**
> Add "Jane Smith" and "Alex Rivera" to the ALL_KNOWN_REPS list in pipeline/config.py

**Assign reps to teams:**
> In pipeline/config.py, create a "Team Sam" with these reps: Amanda Avedschmidt, Julien Baunay, Colin Behenna

**Change the site design:**
> In site/index.html, change the accent color from green to blue and update the header title to "Enterprise Weekly Signals"

**Fix a bug:**
> The dark mode toggle isn't saving between page reloads. Fix it in site/index.html.

**Add a new data source:**
> Add a new pipeline source in pipeline/sources/ that reads from a CSV with columns "Account Name", "Score", and "Last Touch Date"

### 4. Preview your changes locally

> Start a local server so I can preview the site in my browser.

### 5. Create a branch, commit, and open a PR

Once you're happy with the changes, paste this:

> Create a new git branch for my changes, commit everything, push to GitHub, and open a pull request with a summary of what I changed.

The agent will handle branching, committing, pushing, and creating the PR for you.

### 6. Get it reviewed

Hannah (or another collaborator) will review your PR on GitHub. Once approved, it gets merged into `main`.

### 7. Deploy

After merging, ask Hannah to redeploy, or if you have Quick access:

> Pull the latest changes from main and deploy the site to Quick.

---

## Option B: Fork It to Build Your Own Version

Use this when you want a similar dashboard for a different team or segment — your own independent copy.

### 1. Fork and clone

> Fork the repo hannah-harrington/weekly-sales-insights on GitHub, clone my fork to the Desktop, and open it in Cursor.

### 2. Customize the config

> Update pipeline/config.py for my team:
> - Change DEPLOY_SITE_NAME to "my-team-insights"
> - Replace ALL_KNOWN_REPS with these names: [paste your rep names]
> - Set ADMINS to ["your.email@shopify.com"]
> - Update CSV_INPUT_DIR to point to my Demandbase CSV folder

### 3. Customize the site

> Update the site branding in site/index.html:
> - Change the title to "[Your Team] Weekly Insights"
> - Update the header subtitle
> - Change the contact info at the bottom to my name

### 4. Update the workflow doc

> Rewrite MONDAY_WORKFLOW.md for my team. The Demandbase CSVs will be in [your folder path] and the site deploys to my-team-insights.quick.shopify.io.

### 5. Run the pipeline with your data

Drop your Demandbase CSVs into the folder you specified, then:

> The 4 Demandbase CSVs are in [your folder]. Run the weekly sales insights pipeline and deploy to Quick.

### 6. Push everything to your fork

> Commit all my changes and push to my GitHub fork.

Your site will be live at `my-team-insights.quick.shopify.io`.

### 7. (Optional) Pull in upstream improvements

If Hannah's version gets updates you want:

> Add hannah-harrington/weekly-sales-insights as an upstream remote and merge any new changes from main into my fork.

---

## Key Files Reference

| File | What it controls |
|---|---|
| `pipeline/config.py` | Rep names, team mapping, admin/coach roles, deploy settings |
| `site/index.html` | The full site (HTML, CSS, JS all in one file) |
| `pipeline/ingest.py` | How Demandbase CSVs get processed into JSON |
| `pipeline/sources/demandbase.py` | Demandbase-specific parsing logic |
| `site/data/current.json` | Latest week's data (auto-generated — don't edit by hand) |
| `MONDAY_WORKFLOW.md` | Step-by-step Monday update process |

---

## Questions?

Reach out to Hannah Harrington on Slack.
