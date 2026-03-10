# Get Started Here

Weekly Sales Insights is a dashboard that gives each sales rep a personalized weekly report powered by Demandbase data. The original version (NA Enterprise) lives at [sales-insights-hub.quick.shopify.io](https://sales-insights-hub.quick.shopify.io). Any team can set up their own version by following this guide.

Everything below is done through **Cursor** — you just paste a prompt, and it handles the rest. No coding required.

---

## Before You Start

Make sure you have these four things:

- [ ] A **GitHub account** (any Shopify GitHub account works — [sign up here](https://github.com/signup) if you don't have one)
- [ ] **Cursor** installed on your laptop ([download it here](https://cursor.sh))
- [ ] Access to **Demandbase** so you can export your team's weekly reports
- [ ] Access to **Shopify Quick** for publishing your site (ask Hannah if you're not sure)

---

## How Cursor Works

Cursor is a code editor with a built-in AI assistant. You don't need to know how to code — you just type what you want in plain English, and the assistant makes the changes for you.

**To give Cursor an instruction:** look for the chat panel (usually on the right side or bottom of the screen). Click into it, paste one of the prompts from this guide, and press **Enter**. Cursor will do the work and show you what it changed.

---

## Set Up Your Team's Dashboard

This creates your own independent copy of the Sales Insights site. You'll have your own reps, your own data, and your own URL. Nothing you do here can affect the original site.

### Step 1: Create your own copy of the project

Open Cursor and paste this into the chat:

> Fork the repo hannah-harrington/weekly-sales-insights on GitHub, clone my fork to the Desktop, and open it in Cursor.

This creates your own copy on GitHub and downloads it to your computer. Cursor will open the project automatically.

### Step 2: Add your team's reps

Before pasting this prompt, fill in the **bold** parts with your actual info:

> Update pipeline/config.py for my team:
> - Change DEPLOY_SITE_NAME to "**your-team-sales-insights**" (for example, "b2b-sales-insights" or "emea-sales-insights")
> - Replace ALL_KNOWN_REPS with these names: **paste your full list of rep names here, separated by commas**
> - Set ADMINS to ["**your.email@shopify.com**"]
> - Update CSV_INPUT_DIR to point to wherever I'll save my Demandbase CSV files

Cursor will update the configuration file with your team's details.

### Step 3: Update the site branding

> Update the site branding in site/index.html:
> - Change the title to "**Your Team** Weekly Sales Insights"
> - Update the header subtitle to match my team
> - Change the contact info at the bottom to **your name**

### Step 4: Update the weekly workflow instructions

> Rewrite MONDAY_WORKFLOW.md for my team. The Demandbase CSVs will be in **the folder where you'll save your CSVs** and the site will be published at **your-team-sales-insights**.quick.shopify.io.

### Step 5: Test it with your data

First, go to Demandbase and export these reports as CSV files for your team:

1. New Accounts Moved to MQA in Last Week
2. Accounts Visiting High Value Pages with Lost Opp in Last 12 Months
3. Accounts Visiting High Value Pages (all accounts)
4. Newly Engaged People This Week
5. Newly Engaged People This Week - Activity Report

Save them to the folder you specified in Step 2. Then paste this into Cursor:

> The Demandbase CSVs are in **your folder path**. Run the weekly sales insights pipeline and deploy to Quick.

Cursor will process your data and publish the site.

### Step 6: Save and publish your setup

> Commit all my changes and push to my GitHub fork.

Your site is now live! Share the link with your team — it will be at `your-team-sales-insights.quick.shopify.io` (whatever name you chose in Step 2).

---

## Weekly Updates

Every week (we do Mondays), the update takes about 5 minutes:

1. Export the Demandbase CSVs for your team
2. Save them to your CSV folder (overwrite last week's files)
3. Paste this into Cursor: *"The Demandbase CSVs are in **your folder**. Run the weekly sales insights pipeline and deploy to Quick."*
4. Share the link in your team's Slack channel

Full details are in the `MONDAY_WORKFLOW.md` file inside your project.

---

## Getting New Features

When Hannah adds new features or fixes bugs on the original version, you can pull those improvements into your site without losing any of your team's settings. Paste this into Cursor:

> Pull the latest changes from hannah-harrington/weekly-sales-insights into my fork.

Cursor will grab the latest updates and add them to your version. Your rep list, site name, and branding stay exactly as you set them up.

**How often should you do this?** Once a week is a good rhythm — for example, right after your Monday data update. This keeps your version current and avoids any issues from falling too far behind.

If Cursor says there's a conflict (meaning you and Hannah both changed the same part of the site), just ask it: *"Fix the merge conflicts for me."* It will sort it out.

---

## Want to Edit the Original Site Instead?

If you're on the NA Enterprise team and want to suggest a change to the main site (not set up your own), here's the short version:

1. Ask Hannah to give you access to [the project on GitHub](https://github.com/hannah-harrington/weekly-sales-insights)
2. Paste into Cursor: *"Clone the repo hannah-harrington/weekly-sales-insights to my Desktop and open it."*
3. Describe what you want to change in plain English — for example:
   - *"Add Jane Smith and Alex Rivera to the rep list in pipeline/config.py"*
   - *"Change the accent color from green to blue in site/index.html"*
4. When you're happy with the changes, paste: *"Create a new branch for my changes, commit everything, push to GitHub, and open a pull request. Request hannah-harrington as a reviewer."*
5. Hannah will get notified and review your changes on GitHub. Nothing goes live until she approves.

---

## Active Dashboards

| Team | Site URL | Owner |
|---|---|---|
| NA Enterprise | sales-insights-hub.quick.shopify.io | Hannah Harrington |

_Setting up a new one? Add your row here once you're live and push the change to your fork._

---

## What's Inside This Project

You don't need to understand these files to use the tool — Cursor edits them for you. But in case you're curious:

- **The config file** (`pipeline/config.py`) — contains your rep names, team groupings, and site settings. This is what makes the dashboard personalized to your team.
- **The website** (`site/index.html`) — a single file that shows the dashboard. It reads the data and displays each rep's report.
- **The pipeline** (`pipeline/ingest.py`) — a script that takes your Demandbase CSV exports and turns them into the data the website displays.
- **The workflow doc** (`MONDAY_WORKFLOW.md`) — step-by-step instructions for the weekly Monday update.

---

## Need Help?

Reach out to Hannah Harrington on Slack.
