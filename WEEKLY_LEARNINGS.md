# Weekly Learnings & Changelog

Running log of decisions, changes, and lessons learned each week. Add new entries at the top.

---

## Week of March 23, 2026

### What changed this week

- **First Slack DMs to reps** — Pipeline now sends personalized DMs directly to reps (not shared in team channels). Works without a bot token — uses personal session token via `callm` credentials. Run `node ~/pi-backup/refresh-callm-creds.js` before starting each week to keep the token fresh.
- **Lead notify module** (`pipeline/lead_notify.py`) — Added `--notify-leads` flag. Sends per-team signal summaries to each team lead. Brandon Gracey receives a rolled-up all-teams summary every Monday.
- **Signal Hub enrichment** — Pipeline now pulls platform, page visits, G2 activities, and tripwire data from `demandbase-signals-staging.quick.shopify.io` and enriches HVP All, All MQA, and New MQA account rows. New MQA accounts get AI-generated briefs combining platform + pages researched + intent keywords + engagement score.
- **SFDC BigQuery integration** (`pipeline/sources/sfdc_bq.py`) — Stub added for enriching briefs with open opps, last activity date, and engaged contacts from SFDC. Not yet wired to live BigQuery (requires access — contact Jonathan Sinclair).

### Rep feedback received

- **Nathan Frost:** Asked what counts as "engagement" (open, click, form fill?) in the Activity section → add tooltip
- **Nathan Frost:** Asked what "Agentic Commerce Intent" engagement number represents → improve tooltip/description
- **Nathan Frost:** Asked whether reps can see what specific event triggered an account in Intent/MQA sections → gap in data, need to surface event type

### Decisions made

1. **DMs over channel posts** — Switched from posting signals to team Slack channels to sending direct DMs to individual reps. More personal, more actionable, less noise for teammates who don't have signals that week.
2. **Signal Hub enrichment stays out of Git** — `signal_hub_enrichment.json` (1.3MB, 4,491 accounts) excluded from the repo. It contains Shopify internal account data and must be regenerated from the Signal Hub. A fork owner with their own territory would need to generate their own copy.

---

## Week of March 16, 2026

### What changed this week

- **Enterprise Intent Report added** (`EntIntent` CSV) — 5 new intent signal sections per seller: Agentic Commerce (violet), Compete (red), International (teal), Marketing & Growth (orange), B2B (blue). Each section shows top 10 accounts by engagement, filtered by keyword matches. Defined in `INTENT_CATEGORIES` in `pipeline/sources/demandbase.py`.
- **ANZ support** (`pipeline/sources/demandbase_anz.py`) — Added `--anz-input-dir` flag for ANZ team CSV data. ANZ reps: Bronte Hogarth, Chachi Apolinario, Kole Mahan, Lauren Critten, Shane Kilgour (lead: James Johnson). ANZ data runs independently and never interferes with the NA pipeline.
- **CSV count now 6 weekly + 1 snapshot** — Added Enterprise Intent Report. Updated MONDAY_WORKFLOW.md to reflect the full list.

### Decisions made

1. **One account can appear in multiple intent categories** — An account showing intent for both "Agentic Commerce" and "Compete" keywords will appear in both sections. This is intentional — shows breadth of interest.
2. **Capped at 10 per category** — Each intent section shows max 10 accounts per seller, sorted by highest 3-month engagement. Avoids overwhelming reps with a long list.

---

## Week of March 9, 2026

### What changed this week

- Routine pipeline run. No structural changes.
- Verified `Newly Engaged People This Week` file was inaccurate again — skipped, pipeline handled gracefully.
- First week with all 5 weekly signal types confirmed working in production.

---

## Week of March 2, 2026

### What changed this week

**New reports added:**
- `AccountsVisitingHighValuePages.csv` (505 rows) — broader HVP covering all accounts visiting high-value pages, not just lost opps. New column `Enterprise Web Visits (7 days)` contains page URLs, not a numeric score.
- `ENT_Acq_MQA Journey Stage Account List.csv` (842 rows) — replaces old `AllAccountsAtMqa.csv`. Completely different columns: Industry, Billing State, Revenue, Customer Fit Signals.
- `NewAccountsMovedToMqaInLastWeek.csv` — the "New MQA" report that replaced the old no-sales-touches variant.

**Reports dropped:**
- `Newly Engaged People This Week` — excluded this week due to inaccuracy. Pipeline handled the missing file gracefully (0 signals for that type).
- `NewAccountsMovedToMqaInLastWeekWNoSalesTouches.csv` (old "MQA") — kept in folder but no longer processed after consolidation.

### Decisions made

1. **Consolidated MQA reports** — "MQA" (NoSalesTouches) and "MQA New" (NewAccountsMovedToMqa) were duplicate data. Removed `mqa` type entirely. `mqa_new` is now the sole MQA report, labeled "New MQA", using the green accent color. The old "All MQA" (842 records) stays separate as a pipeline snapshot at the bottom.

2. **Spelled out HVP** — Changed from "HVP" to "High-Value Pages" and "HVP All" to "High-Value Pages (All)" across all labels, badges, and filter tags. Reps didn't know what HVP meant.

3. **Restored account/contact notes** — When we removed engagement scores and keywords last time, we lost useful context. Added back `keywords` (labeled "Topics") and `categories` (labeled "Categories") to display columns. Combined with dynamic empty-column hiding so blank columns don't show.

4. **Dynamic empty-column hiding** — Frontend now scans all rows before rendering each table and skips columns where every value is empty. This means we can be generous with `display_columns` in the pipeline without worrying about blank columns cluttering the UI.

5. **Archive on seller pages** — Added the same archive browser that exists on the master dashboard to each individual seller's report page. `loadWeek()` now accepts an optional seller ID to preserve context when switching weeks from a personal view.

6. **Weekly summary per seller** — Added a "Your Week at a Glance" callout card at the top of each seller's personal page. Dynamically composes sentences based on their signal counts (MQA, HVP, new contacts).

7. **MQA visual emphasis** — Multiple treatments to make MQA stand out:
   - Green left-border accent + gradient background on MQA signal sections
   - "Priority" banner: "These accounts are warm and waiting for your outreach"
   - Always-visible green glow border on MQA stat card (even when not highest)
   - Bold green border on MQA filter tag in master dashboard
   - Green dot indicator next to seller names who have MQA signals

### Lessons learned

- **Don't remove descriptive columns when removing numeric ones.** The user asked to "remove columns mentioning keywords or engagement score or numbers" — we removed keywords AND engagement scores. But keywords contained valuable context like "data security, retail, data encryption" that reps found useful. Next time, clarify which specific columns to remove vs. keep.

- **Duplicate reports happen.** Demandbase can export overlapping reports (NoSalesTouches vs NewAccountsMovedToMqa had identical data). Always check for duplicates when new reports are added.

- **Abbreviations don't land.** "HVP" meant nothing to reps. Always spell out abbreviations in user-facing labels, even if it makes pills/badges wider.

- **Empty columns need dynamic handling.** Different reports have different column completeness. Rather than manually curating display_columns per report, it's better to include all potentially useful columns and let the frontend hide empty ones automatically.

- **Sellers want archive too.** The archive browser was only on the master dashboard, but individual sellers also want to see their past weeks. Any navigation feature on the master view should be considered for the personal view too.

- **MQA is the most important signal type.** The user explicitly asked to emphasize it. When there's a "most important" data type, give it visual priority everywhere — not just in its own section but in stat cards, filter bars, and seller cards.

### CSV detection patterns (current)

| Signal Type | CSV Pattern | Exclude Pattern |
|---|---|---|
| `mqa_new` | `NewAccountsMovedToMqa` | — |
| `hvp` | `WithLostOpp` | — |
| `hvp_all` | `AccountsVisitingHighValuePages` | `WithLostOpp` |
| `new_people` | `NewlyEngagedPeopleThisWeek` | `ActivityReport` |
| `activity` | `ActivityReport` | — |
| `all_mqa` | `ENT_Acq_MQA` or `AllAccountsAtMqa` | — |

### Signal type display order (top to bottom)

1. New MQA (green) — weekly, most important
2. High-Value Pages / Lost Opp (rose) — weekly
3. High-Value Pages / All (rose) — weekly
4. People (indigo) — weekly
5. Activity (amber) — weekly
6. All MQA (slate) — snapshot, always last

### Pending items

- **Slack bot approval** — "Sales Insights Bot" submitted for workspace approval. Once approved, add the `xoxb-` token to `~/.zshrc` and use `--notify` flag to auto-DM reps.
- **Coach dashboard** — Identity type exists in data model (`COACHES` in config.py with 5 team leads). Coach VIEW is deferred to Phase 5.
- **Week-over-week comparison** — Design exists in V4 mockup (repeat badges, delta indicators). Not yet wired to real data.

---

## Week of Feb 24, 2026

### What changed

- **Full rebuild from scratch** — Replaced monolithic Python script with decoupled pipeline (`pipeline/`) + single-page app (`site/index.html`).
- **Identity-first via IAP** — Site auto-detects who's visiting via Google Cloud IAP email. Admin sees master dashboard, sellers see their own report automatically.
- **V4 "Warm & Human" design** — Selected from 4 design explorations. Instrument Serif + Inter fonts, card-based layout, scroll animations, personalized greetings.
- **Phase 2b polish** — Mobile optimization, accessibility audit (WCAG AA), dark mode toggle, archive browser UX.
- **Phase 3a Slack DMs** — Built `slack_notify.py` module. Sends personalized Block Kit DMs to each rep with signals after pipeline run.
- **Team structure** — Loaded enterprise sales team assignments from Excel spreadsheet. 6 teams, 5 coaches, seller cards grouped by team on master dashboard.

### Lessons learned

- **IAP needs `Accept: application/json` header** — The identity fetch failed silently until we added the correct header and parsed the response as text then JSON.
- **`quick deploy` uses the site name, not URL** — Deploy command is `quick deploy . site-name`, not the full URL.
- **Quicklytics is IAP-protected too** — Analytics dashboard at quicklytics.quick.shopify.io requires the same Google SSO authentication as the main site.
- **Local development needs HTTP server** — `file://` protocol blocks `fetch()` calls. Always use `python3 -m http.server` for local testing.
