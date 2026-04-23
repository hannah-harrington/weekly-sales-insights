# Sales Insights Hub — Product Log

A running record of what's been built, changed, or fixed. Updated each time we ship something new.

---

## Week of February 24, 2026

The hub was rebuilt from scratch. Replaced a monolithic Python script with a proper decoupled pipeline and a single-page app. The site now auto-detects who's visiting via Google Cloud IAP and shows admins a master dashboard while individual reps see only their own signals. The design was overhauled to a card-based "warm and human" layout with personalised greetings. Slack DM notifications were built into the pipeline so reps get a Block Kit message after each Monday run. The full enterprise sales team structure — 6 teams, 5 coaches — was loaded in and seller cards are grouped by team on the master view.

---

## Week of March 2, 2026

Three new Demandbase reports were added: a broader High-Value Pages report covering all accounts (not just lost opps), a new All MQA snapshot with richer account data, and a cleaner New MQA weekly report. The old duplicate MQA reports were consolidated into one. HVP was spelled out in full after reps didn't know what the abbreviation meant. Keyword and category columns were restored to give reps outreach context. The frontend was updated to hide empty columns dynamically so blank data never clutters the view. Each seller's personal page got an archive browser and a "Your Week at a Glance" summary card. MQA was given strong visual priority across the whole UI — green accents, a priority banner, and a green dot on seller cards that have MQA signals.

---

## Week of March 16, 2026

Five new intent signal categories were added to each rep's report: Agentic Commerce, Compete, International, Marketing & Growth, and B2B. Each section pulls the top 10 accounts by 3-month engagement from a new Enterprise Intent CSV export. ANZ territory support was added as a fully independent pipeline track covering 5 ANZ reps and their team lead. The weekly CSV count moved to 6 regular exports plus 1 periodic snapshot.

---

## Week of March 23, 2026

The pipeline started sending personalised Slack DMs directly to reps rather than posting to shared channels — more personal, less noise. A lead notify module was added so each team lead gets a per-team signal summary every Monday, with Brandon Gracey receiving a rolled-up all-teams view. The Signal Hub enrichment layer was wired in, pulling platform, page visit, G2 activity, and tripwire data to enrich account rows — New MQA accounts now get AI-generated briefs combining all available signals. An SFDC BigQuery integration stub was built to add deal status, last activity, and engaged contacts, though it wasn't yet connected to live data.

---

## Week of March 24, 2026

We expanded SFDC enrichment so deal status, last activity, and contact titles now pull for every signal type — not just MQA. We built a contact seniority classifier that reads job titles and assigns a tier (C-Suite through IC), then surfaces those as colour-coded badges in the dashboard alongside SFDC deal status. We also built a new personalised Slack DM mode that picks the 2–3 best accounts for each rep and writes a tailored "start here" message rather than a generic summary. Rounding it out, we added plain-English descriptions to each intent signal type so reps actually understand what they're looking at and how to act on it.

---

## Week of March 27, 2026

Google News integration added — each rep's top accounts now surface 3 relevant news articles inline, giving reps a real-time talking point before outreach. G2 intent was also integrated as a new signal source, feeding into account prioritisation alongside existing Demandbase data. Slack DM copy was overhauled for both rep and coach messages — rep DMs now highlight the top 2–3 accounts with a personalised "start here" section, and coach DMs link directly to the coach view. Both `slack_notify.py` and `lead_notify.py` were updated with the new copy (ready to send Monday).

---

## Week of March 27, 2026 (afternoon)

Full seller page rebuild shipped to production. The page structure was redesigned from signal tables to a prioritised action flow: Top Accounts to Act On first, then Top Engaged People, then HVP, then intent tables collapsed at the bottom. Top Accounts are scored by signal weight + SFDC deal status + days cold + contact seniority — up to 5 accounts per rep. Top Engaged People surfaces up to 8 people ranked by seniority and account signal strength. HVP and HVP All were merged into a single section with Previously CL accounts highlighted in pink. Outreach angle blocks added to both account and people cards — smart templates covering signal context, platform migration angle, industry angle, deal context, and intent keyword hooks. 20+ role patterns supported for people-level angles. Named SFDC contacts with email + SFDC links added to account cards (requires sdp-pii permit). Full SFDC activity history (last 6 months) and rich account briefs (merchant_overview, industry, revenue, tech stack, competitor contract end date, risk notes) now appear in expanded account cards. Compact centred seller page header with 4-column stat grid and a "This week" contextual summary (pure logic, no API). "Previously CL" used everywhere instead of "Closed Lost".

---

## Week of April 6, 2026

Three additions shipped. First, a new Demandbase CSV type was added: People Visiting High Value Pages. It identifies named individuals (not just companies) who engaged with high-value Shopify pages, filtered to anyone with more than 2 engagement points. These people show up inside expanded HVP account cards with name, title, email, seniority badge, and engagement score. Second, a SUPPRESSION filter was added to the pipeline — any activity or new_people row containing the word "SUPPRESSION" is dropped at parse time before reaching any rep report (60 rows removed in the first run). Third, Top Engaged People cards were redesigned to use the same expand/collapse pattern as Top Accounts — collapsed by default showing name, title, company and badges; expanded showing full engagement detail and outreach angle.

---

## Week of April 13, 2026

Blacklist expanded from 293 accounts to 19,559. The full 2026 Enterprise Blacklist CSV was imported from SFDC and merged with the existing list — covering Big Tech, Big-Box Retail, Telecom, Airlines, CPG parent entities, holding companies, Apple resellers, defunct companies, and 189 universities (student research inflates intent scores). A new Demandbase export type was added: G2 Intent, which surfaces accounts actively researching on G2. `people_contact_data()` enrichment now also matches SFDC contacts by title overlap when exact name match fails. Slack personal token handling was fixed — `conversations.open` now runs before `chat.postMessage` so rep DMs land correctly.

---

## Week of April 17, 2026

LinkedIn Campaign Manager integrated as a new signal source. A new pipeline plugin (`pipeline/sources/linkedin.py`) detects and routes LinkedIn exports automatically. The pipeline filters to High and Very High engagement accounts, merges multiple LinkedIn files if more than one is dropped in the folder, deduplicates by account name, and cross-references against the Book of Business to route accounts to the correct rep. Matched accounts surface a LinkedIn badge (blue `in` icon) on their Hub card. A dedicated "LinkedIn Activity This Week" section was added to every seller page — after HVP, before intent tables — showing account name, journey stage, engagement level, paid impressions, organic engagements, Google News headlines, and SFDC badges. LinkedIn accounts are now fully included in SFDC enrichment and Google News fetching. Admin master view shows LinkedIn-only accounts not routed to any rep. History files save weekly for future delta tracking. Result from first run: 29 BoB accounts routed to reps across all teams.

---

## Week of April 20, 2026 — Founder Review + Reliability Sprint

Full founder (gstack) review of the system was run against the live codebase. Six critical gaps were identified:

1. **Silent Slack DM failure** — if the token expired, all 44 rep DMs failed silently with no alert
2. **Friday launchd failure invisible** — errors went to `/tmp` (wiped on reboot) with no notification
3. **BOB file missing = silent 0 LinkedIn routes** — `load_bob_owner_map()` returned `{}` with no warning
4. **BQ JSON parse error = pipeline crash** — `JSONDecodeError` from BigQuery not caught anywhere
5. **Test run overwrites prod JSON** — any `--no-sfdc --deploy` debugging run silently clobbered `current.json`
6. **Zero tests** — no automated coverage, every change relied on manual verification

All 6 gaps were fixed in the same session. Seven total fixes shipped:

| Fix | What it does |
|---|---|
| BOB_FILE startup check | Fails immediately with a clear error if the file is missing or renamed |
| BQ JSON error handling | Catches `JSONDecodeError`, logs raw output, pipeline continues without crashing |
| Persistent logs | Logs moved from `/tmp` to `sales-insights/logs/` — survive reboots |
| Friday Slack DM | Pipeline DMs Hannah on every Friday run — pass or fail — with what happened |
| Token validation | `validate_token()` runs before DM sends — fails fast with fix instructions instead of 44 silent failures |
| `--dry-run` flag | Builds JSON to a temp file, never touches `current.json`, blocks `--deploy` — safe for debugging |
| 8 automated tests | Routing, blacklist, email derivation, config integrity — all green with pytest |

Founder review doc: [Sales Insights Hub — Founder Review](https://docs.google.com/document/d/1gNYYaB6HvgIE0o1Sifi2CqB7Q_QBLG-QxyMFhWrGt7E/edit)

Two additional improvements shipped in the same session:

**Signal type list centralisation** — adding a new signal type previously required updating 5 separate hardcoded lists across 2 files (a known source of bugs — LinkedIn was missed on both lists when first added). Each signal type now carries its own flags (`sfdc_enrich`, `news_fetch`, `hub_enrich`) inside `SIGNAL_TYPE_META`. All downstream lists are derived automatically. One flag change is now all it takes to add a new signal type to the pipeline.

**Frontend extraction** — `index.html` was 3,510 lines of HTML, CSS, and JavaScript in a single file. The CSS (667 lines) was extracted to `styles.css` and the JavaScript (2,669 lines) to `app.js`. `index.html` is now 44 lines of pure structure. No logic was changed. Future edits to styles or JS no longer require navigating a 3,500-line file.

**Rollback procedure** documented in `MONDAY_WORKFLOW.md` — step-by-step instructions for restoring any previous week's data to the live site in under a minute.

---

---

## Remaining — from April 20 Founder Review

These were identified in the gstack review and deferred. Pick up in order.

**1. Signal-to-pipeline tracking** (roadmap #8)
The missing ROI proof. Did last week's signals turn into SFDC opportunities? Currently there is no way to answer this question. Requires BigQuery write access to log which accounts were surfaced each week and then query whether an opp was created or progressed. This is the single highest-value thing to build next — it proves the system is working and gives Hannah data to show leadership.

**2. Async Google News fetch**
First run of each week fetches news for 500+ accounts sequentially — takes ~5 minutes. Switching to `asyncio` + `aiohttp` (parallel fetching) drops it to ~20 seconds. Cache still applies on re-runs so this only matters once per week. Low risk, high convenience.

**3. Week-over-week LinkedIn delta**
History files are being collected weekly in `pipeline/linkedin_history/`. After 2 weeks of data, the pipeline can calculate paid impression/click spikes (e.g. 100%+ jump week-over-week = worth surfacing separately). Not buildable until there are at least 2 history files — check after the next Friday run.

**4. Frontend blank page protection**
If a JavaScript error occurs in `app.js`, reps see a completely blank page with no explanation. An error boundary (try/catch around `init()` with a fallback message) would at minimum tell reps something went wrong and give them a link to contact Hannah. Small change, high user impact if it ever fires.

---

## April 23, 2026

Built product log system — PRODUCT_LOG.md caught up through April 20 (founder review + 7 fixes), log.sh script created for auto-dated entries, HTML viewer built at outputs/plans/sales-insights-product-log.html, product log rule added to AGENTS.md

---
