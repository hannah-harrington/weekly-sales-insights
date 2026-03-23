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
    c.title,
    COUNT(DISTINCT c.contact_id) AS contact_count
  FROM `shopify-dw.sales.sales_contacts_v1` c
  JOIN `shopify-dw.intermediate.salesforce_activities_v2` a
    ON c.contact_id = a.contact_id
  WHERE c.account_id IN (SELECT account_id FROM matched_accounts)
    AND a.activity_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY c.account_id, c.title
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
        if contact_title and contact_title not in rec["_seen_contacts"]:
            rec["_seen_contacts"].add(contact_title)
            rec["engaged_contacts"].append({
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
