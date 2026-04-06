"""
salesloft.py — Salesloft email click signals source module.

Pulls email click events from the Salesloft API for the past N days.
Identifies named contacts at BoB accounts who clicked links in rep emails.

Requirements:
  - SALESLOFT_API_KEY env var (Bearer token)
    → Get it from: Salesloft → Settings → API → API Keys → Create key
    → Needs admin-level key to see ALL reps' emails (not just your own)

Returns the standard source plugin interface:
  {
    "signals_by_seller": { seller_name: { "email_clicks": [...] } },
    "raw_signals": { "email_clicks": [...] },
    "signal_types": { "email_clicks": { "label": "...", ... } }
  }

Each email_click row:
  {
    "person_name":   "Josh Smith",
    "title":         "CTO",
    "email":         "josh.smith@levis.com",
    "account":       "Levi Strauss & Co",
    "click_count":   2,
    "last_clicked":  "2026-04-01T14:23:00Z",
    "cadence":       "Q2 Enterprise Outbound",
    "rep_name":      "Colin Behenna",
    "sfdc_url":      "https://shopify.lightning.force.com/...",  # if available
  }
"""

import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

BASE_URL = "https://api.salesloft.com/v2"


def _get(path: str, params: dict, api_key: str) -> dict:
    """Make a single GET request to the Salesloft API."""
    url = f"{BASE_URL}{path}?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Salesloft API error {e.code} on {path}: {body}") from e


def _paginate(path: str, base_params: dict, api_key: str) -> list[dict]:
    """Fetch all pages from a Salesloft list endpoint (100 records/page max)."""
    results = []
    page = 1
    per_page = 100
    while True:
        params = {**base_params, "page": page, "per_page": per_page}
        data = _get(path, params, api_key)
        records = data.get("data", [])
        results.extend(records)
        meta = data.get("metadata", {}).get("paging", {})
        total_pages = meta.get("total_pages", 1)
        if page >= total_pages or not records:
            break
        page += 1
    return results


# ---------------------------------------------------------------------------
# Main fetch
# ---------------------------------------------------------------------------

def fetch_email_clicks(api_key: str, days_back: int = 7, debug: bool = False) -> list[dict]:
    """
    Fetch emails with at least one link click from the past N days.
    Returns raw Salesloft email records.
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat()
    params = {
        "has_clicks": "true",
        "sent_at[gt]": since,
        "include_paging_counts": "true",
    }
    records = _paginate("/activities/emails", params, api_key)

    if debug:
        print(f"[salesloft] Fetched {len(records)} clicked emails", file=sys.stderr)
        if records:
            print("[salesloft] Sample record keys:", list(records[0].keys()), file=sys.stderr)
            # Print first record for inspection
            print("[salesloft] Sample record:\n",
                  json.dumps(records[0], indent=2, default=str)[:2000], file=sys.stderr)

    return records


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _extract_person(record: dict) -> dict:
    """Extract person/contact details from an email record."""
    recipient = record.get("recipient") or {}
    # Salesloft nests person details under recipient.links or as flat fields
    # depending on API version — handle both shapes defensively
    first = recipient.get("first_name", "")
    last = recipient.get("last_name", "")
    email = (
        recipient.get("email")
        or recipient.get("email_address")
        or record.get("recipient_email_address", "")
    )
    title = recipient.get("title", "")
    # Account is nested inside recipient
    account_obj = recipient.get("account") or {}
    account_name = account_obj.get("name", "")
    # Try to get SFDC URL from CRM links
    crm_url = ""
    crm_activity = record.get("crm_activity") or {}
    if crm_activity:
        crm_url = crm_activity.get("salesforce_id", "")

    return {
        "first_name": first,
        "last_name": last,
        "person_name": f"{first} {last}".strip() or email,
        "title": title,
        "email": email,
        "account": account_name,
        "sfdc_url": crm_url,
    }


def _extract_rep(record: dict) -> str:
    """Extract the rep's full name from the 'user' object."""
    user = record.get("user") or {}
    first = user.get("first_name", "")
    last = user.get("last_name", "")
    return f"{first} {last}".strip()


def _extract_counts(record: dict) -> dict:
    """Extract engagement counts (clicks, views, replies)."""
    counts = record.get("counts") or {}
    return {
        "clicks": counts.get("clicks", 1),   # default 1 — record only exists if clicked
        "views": counts.get("views", 0),
        "replies": counts.get("replies", 0),
    }


def _extract_cadence(record: dict) -> str:
    """Extract cadence name if present."""
    cadence = record.get("cadence") or {}
    return cadence.get("name", "")


# ---------------------------------------------------------------------------
# Normalise into pipeline rows
# ---------------------------------------------------------------------------

def normalise(records: list[dict]) -> list[dict]:
    """
    Convert raw Salesloft email records into normalised signal rows.
    Deduplicates: if the same person clicks multiple emails, merge into one row
    with cumulative click count and latest timestamp.
    """
    # Key: (rep_name, email) → merged row
    merged: dict[tuple, dict] = {}

    for rec in records:
        person = _extract_person(rec)
        rep_name = _extract_rep(rec)
        counts = _extract_counts(rec)
        cadence = _extract_cadence(rec)
        sent_at = rec.get("sent_at") or rec.get("updated_at") or ""

        key = (rep_name, person["email"])
        if key in merged:
            existing = merged[key]
            existing["click_count"] += counts["clicks"]
            existing["view_count"] += counts["views"]
            # Keep most recent timestamp
            if sent_at > existing["last_activity"]:
                existing["last_activity"] = sent_at
            # Collect all cadences touched (deduped)
            if cadence and cadence not in existing["cadences"]:
                existing["cadences"].append(cadence)
        else:
            merged[key] = {
                "person_name": person["person_name"],
                "first_name": person["first_name"],
                "last_name": person["last_name"],
                "title": person["title"],
                "email": person["email"],
                "account": person["account"],
                "click_count": counts["clicks"],
                "view_count": counts["views"],
                "last_activity": sent_at,
                "cadences": [cadence] if cadence else [],
                "rep_name": rep_name,
                "sfdc_url": person["sfdc_url"],
                "signal_type": "email_click",
            }

    rows = list(merged.values())
    # Sort by click count desc
    rows.sort(key=lambda r: r["click_count"], reverse=True)
    return rows


# ---------------------------------------------------------------------------
# Public interface (matches existing source module pattern)
# ---------------------------------------------------------------------------

def load(api_key: str | None = None, days_back: int = 7, debug: bool = False) -> dict:
    """
    Main entry point. Called by ingest.py.

    Args:
        api_key:   Salesloft Bearer token. Falls back to SALESLOFT_API_KEY env var.
        days_back: How many days of click history to fetch. Default 7.
        debug:     If True, print raw API response samples to stderr.

    Returns:
        Standard source plugin dict:
        {
          "signals_by_seller": { seller_name: { "email_clicks": [...] } },
          "raw_signals":       { "email_clicks": [...] },
          "signal_types":      { "email_clicks": { ... } },
        }
    """
    key = api_key or os.environ.get("SALESLOFT_API_KEY", "")
    if not key:
        print("[salesloft] WARNING: No API key found. Set SALESLOFT_API_KEY env var.", file=sys.stderr)
        return _empty()

    raw_records = fetch_email_clicks(key, days_back=days_back, debug=debug)
    if not raw_records:
        print("[salesloft] No clicked emails found in the past", days_back, "days.", file=sys.stderr)
        return _empty()

    rows = normalise(raw_records)

    # Group by rep
    signals_by_seller: dict[str, dict] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        rep = row["rep_name"]
        if rep:
            signals_by_seller[rep]["email_clicks"].append(row)

    # Flatten defaultdicts
    signals_by_seller = {k: dict(v) for k, v in signals_by_seller.items()}

    return {
        "signals_by_seller": signals_by_seller,
        "raw_signals": {"email_clicks": rows},
        "signal_types": {
            "email_clicks": {
                "label": "Email Clicks",
                "description": "Contacts at your accounts who clicked a link in your Salesloft emails",
                "icon": "✉️",
                "priority": 2,
            }
        },
    }


def _empty() -> dict:
    return {
        "signals_by_seller": {},
        "raw_signals": {"email_clicks": []},
        "signal_types": {
            "email_clicks": {
                "label": "Email Clicks",
                "description": "Contacts at your accounts who clicked a link in your Salesloft emails",
                "icon": "✉️",
                "priority": 2,
            }
        },
    }
