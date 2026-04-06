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
