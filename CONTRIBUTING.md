# Contributing to Weekly Sales Insights

This site lives at [weekly-sales-insights.quick.shopify.io](https://weekly-sales-insights.quick.shopify.io) and the source code is on GitHub at [hannah-harrington/weekly-sales-insights](https://github.com/hannah-harrington/weekly-sales-insights).

There are two ways to work with this repo: **edit the existing site** or **fork it to build your own version**.

---

## Option A: Edit the Existing Site

Use this when you want to update the team config, fix a bug, or improve the site design.

### 1. Get access

Ask Hannah to add you as a collaborator on [the repo](https://github.com/hannah-harrington/weekly-sales-insights). You'll need a GitHub account.

### 2. Clone the repo (one-time setup)

```bash
git clone git@github.com:hannah-harrington/weekly-sales-insights.git
cd weekly-sales-insights
```

### 3. Create a branch

Never edit `main` directly. Create a branch with a short, descriptive name:

```bash
git checkout -b your-branch-name
```

Examples: `add-team-sam-mapping`, `fix-dark-mode-toggle`, `update-rep-list`

### 4. Make your changes

The key files:

| File | What it controls |
|---|---|
| `pipeline/config.py` | Rep names, team mapping, admin/coach roles |
| `site/index.html` | The full site (HTML, CSS, JS all in one file) |
| `pipeline/ingest.py` | How Demandbase CSVs get processed into JSON |
| `pipeline/sources/demandbase.py` | Demandbase-specific parsing logic |
| `site/data/current.json` | The latest week's data (auto-generated, don't edit by hand) |

### 5. Commit your changes

```bash
git add -A
git commit -m "Short description of what you changed"
```

### 6. Push your branch

```bash
git push -u origin your-branch-name
```

### 7. Open a Pull Request

Go to [github.com/hannah-harrington/weekly-sales-insights](https://github.com/hannah-harrington/weekly-sales-insights) and click **"Compare & pull request"** on the banner that appears. Add a short description of what you changed and why, then submit.

### 8. Get it reviewed and merged

Hannah (or another collaborator) will review your PR. Once approved, it gets merged into `main`.

### 9. Deploy

After merging, the site still needs to be redeployed to Quick. Ask Hannah to run the deploy, or if you have Quick access:

```bash
cd site
quick deploy . weekly-sales-insights
```

---

## Option B: Fork It to Build Your Own Version

Use this when you want to create a similar dashboard for a different team or segment — your own copy that you control independently.

### 1. Fork the repo

Go to [github.com/hannah-harrington/weekly-sales-insights](https://github.com/hannah-harrington/weekly-sales-insights) and click the **"Fork"** button in the top right. This creates a copy under your own GitHub account.

### 2. Clone your fork

```bash
git clone git@github.com:YOUR-USERNAME/weekly-sales-insights.git
cd weekly-sales-insights
```

### 3. Customize it

Things you'll likely want to change:

- **`pipeline/config.py`** — Replace the rep list, team mapping, and admin emails with your own
- **`site/index.html`** — Update the title, branding, and any team-specific copy
- **`pipeline/config.py` → `DEPLOY_SITE_NAME`** — Change to your own Quick site name (e.g. `"my-team-insights"`)
- **`MONDAY_WORKFLOW.md`** — Update the workflow doc for your team's process

### 4. Set up your data pipeline

You'll need your own Demandbase CSV exports. Follow the same Monday workflow:

1. Export your 4 CSVs from Demandbase
2. Drop them into a local folder
3. Update `CSV_INPUT_DIR` in `pipeline/config.py` to point to that folder
4. Run: `python3 -m pipeline.ingest --deploy`

### 5. Deploy to your own Quick site

```bash
cd site
quick deploy . your-site-name
```

Your site will be live at `your-site-name.quick.shopify.io`.

### 6. (Optional) Pull in upstream updates

If Hannah's version gets improvements you want, you can pull them into your fork:

```bash
git remote add upstream git@github.com:hannah-harrington/weekly-sales-insights.git
git fetch upstream
git merge upstream/main
```

---

## Questions?

Reach out to Hannah Harrington on Slack.
