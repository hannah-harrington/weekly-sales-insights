"""
SFDC enrichment via BigQuery.

Pulls open opps, last activity date, and engaged contacts for a set of
accounts. Returns a dict keyed by lowercase website domain (with account
name as a fallback key) so callers can do fast lookups during brief generation.

Usage:
    from pipeline.sources import sfdc_bq

    sfdc = sfdc_bq.load(names=["Nike", "Patagonia"], websites=["nike.com", "patagonia.com"])
    info = sfdc_bq.lookup(sfdc, account_name="Nike", website="nike.com")
    # info = {"open_opps": [...], "last_activity_date": "2026-03-15", "engaged_contacts": [...]}
"""

import json
import re
from datetime import date
from subprocess import run, PIPE

# Billing project that has BigQuery job creation permissions
_BILLING_PROJECT = "sdp-for-analysts-platform"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run_bq(sql: str) -> list[dict]:
    """Execute a BigQuery SQL query via the bq CLI. Returns list of row dicts."""
    result = run(
        [
            "bq", "query",
            f"--project_id={_BILLING_PROJECT}",
            "--use_legacy_sql=false",
            "--format=json",
            "--max_rows=10000",
            sql,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"BigQuery query failed: {result.stderr.strip()}")
    output = result.stdout.strip()
    if not output or output == "[]":
        return []
    return json.loads(output)


def _normalize_domain(website: str) -> str:
    """Strip protocol/www/path from a URL and return a bare domain."""
    if not website:
        return ""
    w = website.lower().strip()
    w = re.sub(r"^https?://", "", w)
    w = re.sub(r"^www\.", "", w)
    w = w.split("/")[0].split("?")[0].split("#")[0]
    return w.strip()


def _escape_sql_string(s: str) -> str:
    """Escape single quotes in a SQL string literal."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _sql_string_list(items: list[str]) -> str:
    """Format a Python list as a SQL IN-list of quoted strings."""
    return ", ".join(f"'{_escape_sql_string(i)}'" for i in items if i)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load(names: list[str], websites: list[str]) -> dict:
    """
    Query SFDC data from BigQuery for the given accounts.

    Matches accounts first by website domain, then by name (case-insensitive).

    Returns a dict where each key is either:
      - a bare domain (e.g. "nike.com")  ← preferred
      - a lowercase account name          ← fallback

    Each value contains:
      - open_opps:          list of {name, stage, acv_usd, close_date}
      - last_activity_date: "YYYY-MM-DD" string or None
      - engaged_contacts:   list of {name, title} (active in last 90 days)
    """
    if not names and not websites:
        return {}

    domains = list({_normalize_domain(w) for w in websites if w})
    clean_names = list({n.strip() for n in names if n.strip()})

    if not domains and not clean_names:
        return {}

    domain_filter = f"LOWER(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(IFNULL(website,'')),r'https?://',''),r'^www\\.','')) IN ({_sql_string_list(domains)})" if domains else "FALSE"
    name_filter = f"LOWER(name) IN ({_sql_string_list([n.lower() for n in clean_names])})" if clean_names else "FALSE"

    sql = f"""
WITH matched_accounts AS (
  SELECT
    account_id,
    name AS account_name,
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(LOWER(IFNULL(website, '')), r'https?://', ''),
        r'^www\\.', ''
      )
    ) AS domain
  FROM `shopify-dw.sales.sales_accounts_v1`
  WHERE ({domain_filter})
     OR ({name_filter})
),

open_opps AS (
  SELECT
    salesforce_account_id AS account_id,
    name                  AS opp_name,
    current_stage_name    AS stage,
    total_acv_amount_usd  AS acv_usd,
    CAST(close_date AS STRING) AS close_date
  FROM `shopify-dw.sales.sales_opportunities_v1`
  WHERE is_closed = FALSE
    AND salesforce_account_id IN (SELECT account_id FROM matched_accounts)
),

last_activity AS (
  SELECT
    account_id,
    CAST(MAX(DATE(activity_date)) AS STRING) AS last_activity_date
  FROM `shopify-dw.intermediate.salesforce_activities_v2`
  WHERE account_id IN (SELECT account_id FROM matched_accounts)
  GROUP BY account_id
),

engaged_contacts AS (
  SELECT
    c.account_id,
    c.name,
    c.title,
    COUNT(DISTINCT c.contact_id) AS contact_count
  FROM `shopify-dw.sales.sales_contacts_v1` c
  JOIN `shopify-dw.intermediate.salesforce_activities_v2` a
    ON c.contact_id = a.contact_id
  WHERE c.account_id IN (SELECT account_id FROM matched_accounts)
    AND a.activity_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY c.account_id, c.name, c.title
)

SELECT
  ma.account_id,
  ma.account_name,
  ma.domain,
  la.last_activity_date,
  oo.opp_name,
  oo.stage       AS opp_stage,
  oo.acv_usd,
  oo.close_date,
  ec.name        AS contact_name,
  ec.title       AS contact_title,
  ec.contact_count
FROM matched_accounts ma
LEFT JOIN open_opps        oo ON ma.account_id = oo.account_id
LEFT JOIN last_activity    la ON ma.account_id = la.account_id
LEFT JOIN engaged_contacts ec ON ma.account_id = ec.account_id
"""

    rows = _run_bq(sql)

    # Aggregate flat rows into per-account dicts
    # Both domain and lowercase name point to the same object (same reference)
    accounts: dict[str, dict] = {}

    def _get_or_create(domain: str, account_name: str) -> dict:
        key = domain or account_name.lower()
        if key not in accounts:
            rec: dict = {
                "account_name": account_name,
                "domain": domain,
                "open_opps": [],
                "last_activity_date": None,
                "engaged_contacts": [],
                "_seen_opps": set(),
                "_seen_contacts": set(),
            }
            accounts[key] = rec
            # Also register under lowercase name as a fallback
            name_key = account_name.lower()
            if name_key and name_key != key:
                accounts[name_key] = rec
        return accounts[key]

    for row in rows:
        raw_domain = (row.get("domain") or "").strip()
        # BQ domain may still have trailing slash from SQL
        domain = raw_domain.split("/")[0]
        account_name = (row.get("account_name") or "").strip()

        if not domain and not account_name:
            continue

        rec = _get_or_create(domain, account_name)

        # Last activity — keep the most recent value seen across rows
        la = row.get("last_activity_date")
        if la and (rec["last_activity_date"] is None or la > rec["last_activity_date"]):
            rec["last_activity_date"] = la

        # Open opp (deduplicated by name)
        opp_name = row.get("opp_name")
        if opp_name and opp_name not in rec["_seen_opps"]:
            rec["_seen_opps"].add(opp_name)
            rec["open_opps"].append({
                "name": opp_name,
                "stage": row.get("opp_stage") or "",
                "acv_usd": row.get("acv_usd"),
                "close_date": row.get("close_date") or "",
            })

        # Engaged contacts (grouped by title — no PII name access without permit)
        contact_title = row.get("contact_title")
        contact_name = row.get("contact_name") or ""
        contact_key = contact_name or contact_title
        if contact_key and contact_key not in rec["_seen_contacts"]:
            rec["_seen_contacts"].add(contact_key)
            rec["engaged_contacts"].append({
                "name":  contact_name,
                "title": contact_title,
                "count": int(row.get("contact_count") or 1),
            })

    # Clean up internal tracking sets before returning
    for rec in set(id(v) for v in accounts.values()):
        pass  # can't remove from dict while iterating shared objects
    seen_ids: set[int] = set()
    for rec in accounts.values():
        if id(rec) not in seen_ids:
            seen_ids.add(id(rec))
            rec.pop("_seen_opps", None)
            rec.pop("_seen_contacts", None)

    return accounts


def lookup(sfdc_data: dict, account_name: str, website: str) -> dict | None:
    """
    Look up SFDC enrichment for one account.

    Tries domain match first, then falls back to account name (case-insensitive).
    Returns the enrichment dict or None if no match found.
    """
    domain = _normalize_domain(website)
    if domain and domain in sfdc_data:
        return sfdc_data[domain]
    name_key = (account_name or "").lower().strip()
    if name_key and name_key in sfdc_data:
        return sfdc_data[name_key]
    return None


def days_since(date_str: str | None) -> int | None:
    """Return days elapsed since a YYYY-MM-DD date string. None if unparseable."""
    if not date_str:
        return None
    try:
        return (date.today() - date.fromisoformat(date_str)).days
    except ValueError:
        return None


def _strip_html(text: str | None, max_len: int = 2500) -> str | None:
    """Strip HTML tags and truncate for display."""
    if not text:
        return None
    import re as _re
    clean = _re.sub(r'<[^>]+>', ' ', text)
    clean = _re.sub(r'&amp;', '&', clean)
    clean = _re.sub(r'&lt;', '<', clean)
    clean = _re.sub(r'&gt;', '>', clean)
    clean = _re.sub(r'&quot;', '"', clean)
    clean = _re.sub(r'&#39;', "'", clean)
    clean = _re.sub(r'\s+', ' ', clean).strip()
    return clean[:max_len] if len(clean) > max_len else clean


def load_account_details(account_names: list[str]) -> dict:
    """
    Pull rich SFDC account details for a list of account names.

    Returns a dict keyed by lowercase account name with:
      industry, employees, revenue, ecomm_platform, pos_solution,
      competitor_contract_end_date, merchant_overview, description,
      city, country, why_at_risk, account_url, new_opportunity_url,
      plus_status, plan_name, account_priority_d2c

    When multiple SFDC records exist for the same account name, picks
    the richest one (merchant_overview > description > highest revenue).
    """
    if not account_names:
        return {}

    clean = list({n.strip() for n in account_names if n and n.strip()})
    if not clean:
        return {}

    sql = f"""
SELECT
  name,
  industry,
  number_of_employees,
  annual_total_revenue_usd,
  annual_online_revenue,
  ecomm_platform,
  pos_solution,
  CAST(competitor_contract_end_date AS STRING)  AS competitor_contract_end_date,
  merchant_overview,
  description,
  billing_city,
  billing_country,
  why_at_risk,
  account_url,
  new_opportunity_url,
  plus_status,
  plan_name,
  account_priority_d2c
FROM `shopify-dw.sales.sales_accounts_v1`
WHERE LOWER(name) IN ({_sql_string_list([n.lower() for n in clean])})
ORDER BY
  -- prefer records with the richest data
  (CASE WHEN merchant_overview IS NOT NULL THEN 0 ELSE 1 END),
  (CASE WHEN description       IS NOT NULL THEN 0 ELSE 1 END),
  COALESCE(annual_total_revenue_usd, 0) DESC
"""

    try:
        rows = _run_bq(sql)
    except RuntimeError:
        return {}

    result: dict[str, dict] = {}
    for row in rows:
        name_key = (row.get("name") or "").strip().lower()
        if not name_key or name_key in result:
            continue  # keep first (richest) record per name
        result[name_key] = {
            "industry":                    row.get("industry"),
            "employees":                   row.get("number_of_employees"),
            "revenue_usd":                 row.get("annual_total_revenue_usd"),
            "annual_online_revenue":       row.get("annual_online_revenue"),
            "ecomm_platform":              row.get("ecomm_platform"),
            "pos_solution":                row.get("pos_solution"),
            "competitor_contract_end":     row.get("competitor_contract_end_date"),
            "merchant_overview":           _strip_html(row.get("merchant_overview"), 3000),
            "description":                 _strip_html(row.get("description"), 500),
            "city":                        row.get("billing_city"),
            "country":                     row.get("billing_country"),
            "why_at_risk":                 _strip_html(row.get("why_at_risk"), 300),
            "account_url":                 row.get("account_url"),
            "new_opportunity_url":         row.get("new_opportunity_url"),
            "plus_status":                 row.get("plus_status"),
            "plan_name":                   row.get("plan_name"),
            "account_priority_d2c":        row.get("account_priority_d2c"),
        }
    return result


def load_account_activities(account_names: list[str], months_back: int = 6) -> dict:
    """
    Pull recent SFDC activity log for a list of accounts.

    Returns a dict keyed by lowercase account name, where each value is a list of:
      {activity_type, activity_date, subject, status, contact_title}

    Sorted most-recent first, capped at 10 per account.
    """
    if not account_names:
        return {}

    clean = list({n.strip() for n in account_names if n and n.strip()})
    if not clean:
        return {}

    sql = f"""
WITH ranked AS (
  SELECT
    LOWER(a.name)                           AS account_key,
    act.activity_type,
    CAST(DATE(act.activity_date) AS STRING) AS activity_date,
    act.subject,
    act.status,
    c.name                                  AS contact_name,
    c.title                                 AS contact_title,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(a.name)
      ORDER BY act.activity_date DESC
    ) AS rn
  FROM `shopify-dw.intermediate.salesforce_activities_v2` act
  JOIN `shopify-dw.sales.sales_accounts_v1` a
    ON act.account_id = a.account_id
  LEFT JOIN `shopify-dw.sales.sales_contacts_v1` c
    ON act.contact_id = c.contact_id
  WHERE LOWER(a.name) IN ({_sql_string_list([n.lower() for n in clean])})
    AND act.activity_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {months_back * 30} DAY)
)
SELECT account_key, activity_type, activity_date, subject, status, contact_name, contact_title
FROM ranked
WHERE rn <= 10
ORDER BY account_key, activity_date DESC
"""

    # Batch into chunks of 100 to avoid CLI arg length limits
    BATCH = 100
    all_rows = []
    for i in range(0, len(clean), BATCH):
        batch = clean[i:i + BATCH]
        batch_sql = sql.replace(
            f"IN ({_sql_string_list([n.lower() for n in clean])})",
            f"IN ({_sql_string_list([n.lower() for n in batch])})"
        )
        try:
            all_rows.extend(_run_bq(batch_sql))
        except RuntimeError:
            continue

    result: dict[str, list] = {}
    for row in all_rows:
        key = (row.get("account_key") or "").strip()
        if not key:
            continue
        if key not in result:
            result[key] = []
        if len(result[key]) < 10:  # cap at 10 per account
            result[key].append({
                "type":          row.get("activity_type") or "Activity",
                "date":          row.get("activity_date") or "",
                "subject":       row.get("subject") or "",
                "status":        row.get("status") or "",
                "contact_name":  row.get("contact_name") or "",
                "contact_title": row.get("contact_title") or "",
            })
    return result


_SFDC_BASE = "https://banff.lightning.force.com/lightning/r"

def load_people_contact_data(account_names: list[str]) -> dict:
    """
    For a list of account names, return SFDC contact data matched by name + title.

    Returns a dict keyed by lowercase account name, where each value is a list of:
      {name, title, email, contact_url, last_contact_date, days_since_contact, in_sfdc}

    Used to enrich Demandbase activity/new_people rows with SFDC contact details.
    Requires sdp-pii permit for name + email columns.
    """
    if not account_names:
        return {}

    clean = list({n.strip() for n in account_names if n and n.strip()})
    if not clean:
        return {}

    sql = f"""
SELECT
  a.name                                                       AS account_name,
  c.name                                                       AS contact_name,
  c.title,
  c.email,
  c.contact_id,
  CAST(MAX(DATE(act.activity_date)) AS STRING)                 AS last_contact_date,
  DATE_DIFF(CURRENT_DATE(), MAX(DATE(act.activity_date)), DAY) AS days_since_contact
FROM `shopify-dw.sales.sales_contacts_v1` c
JOIN `shopify-dw.sales.sales_accounts_v1` a
  ON c.account_id = a.account_id
LEFT JOIN `shopify-dw.intermediate.salesforce_activities_v2` act
  ON c.contact_id = act.contact_id
WHERE LOWER(a.name) IN ({_sql_string_list([n.lower() for n in clean])})
  AND c.title IS NOT NULL
GROUP BY a.name, c.name, c.title, c.email, c.contact_id
ORDER BY a.name, days_since_contact ASC NULLS LAST
"""

    try:
        rows = _run_bq(sql)
    except RuntimeError:
        return {}

    result: dict[str, list] = {}
    for row in rows:
        acct = (row.get("account_name") or "").strip().lower()
        if not acct:
            continue
        if acct not in result:
            result[acct] = []
        contact_id = row.get("contact_id") or ""
        result[acct].append({
            "name":               row.get("contact_name") or "",
            "title":              row.get("title") or "",
            "email":              row.get("email") or "",
            "contact_url":        f"{_SFDC_BASE}/Contact/{contact_id}/view" if contact_id else "",
            "last_contact_date":  row.get("last_contact_date"),
            "days_since_contact": int(row["days_since_contact"]) if row.get("days_since_contact") is not None else None,
            "in_sfdc":            True,
        })
    return result


def match_person_contact(people_data: dict, account_name: str, title: str, full_name: str = "") -> dict | None:
    """
    Given a Demandbase person's account + name + title, find the best SFDC contact match.

    Priority: exact name match → exact title match → title word-overlap.
    Returns the enriched contact dict (name, title, email, contact_url, days_since_contact, in_sfdc) or None.
    """
    contacts = people_data.get(account_name.lower().strip(), [])
    if not contacts:
        return None

    # 1. Exact name match (most reliable — requires sdp-pii)
    if full_name:
        name_lower = full_name.lower().strip()
        for c in contacts:
            if c.get("name", "").lower().strip() == name_lower:
                return c

    title_lower = (title or "").lower().strip()

    # 2. Exact title match
    for c in contacts:
        if c["title"].lower().strip() == title_lower:
            return c

    # 3. Word-overlap match — at least 2 significant words in common
    title_words = set(w for w in title_lower.split() if len(w) > 3)
    best = None
    best_overlap = 0
    for c in contacts:
        c_words = set(w for w in c["title"].lower().split() if len(w) > 3)
        overlap = len(title_words & c_words)
        if overlap >= 2 and overlap > best_overlap:
            best_overlap = overlap
            best = c

    return best
