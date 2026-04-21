"""
LinkedIn Campaign Manager source plugin.

Reads a LinkedIn companies export CSV (7-day window) and extracts:
  - li_very_high_set: set of lowercase account names with 'Very High' engagement
  - li_all_rows: dict keyed by lowercase account name with raw data

Saves a weekly copy to pipeline/linkedin_history/<date>.csv for future
week-over-week delta tracking.
"""

import csv
import shutil
from pathlib import Path

SIGNAL_TYPE_META = {
    "li_very_high": {
        "label": "LinkedIn Very High Engagement",
        "short_label": "LinkedIn Activity",
        "color": "blue",
        "source": "linkedin",
        "description": (
            "These accounts are in your book of business and showing Very High "
            "engagement with Shopify\u2019s LinkedIn content this week \u2014 ads, "
            "organic posts, or both. No website visit data available from this source. "
            "Use as a warm signal that Shopify is top of mind at this account."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "paid_impressions", "label": "Paid Imp."},
            {"key": "organic_engagements", "label": "Org. Engagements"},
        ],
        # Pipeline participation flags — read by ingest.py to derive enrichment lists
        "sfdc_enrich": True,
        "news_fetch": True,
        "hub_enrich": False,
    }
}

# Columns that identify a LinkedIn companies export (not a Demandbase file)
_REQUIRED_COLUMNS = {"Company Name", "Engagement Level", "Paid Impressions", "Paid Clicks"}


def _is_linkedin_file(path: Path) -> bool:
    """Detect a LinkedIn CSV by filename or column headers."""
    name_lower = path.name.lower()
    if "linkedin" in name_lower or name_lower.startswith("li ") or name_lower.startswith("li_"):
        return True
    # Fallback: check headers match LinkedIn companies export format
    try:
        with open(path, encoding="utf-8-sig") as f:
            first_line = f.readline()
        headers = {h.strip().strip('"') for h in first_line.split(",")}
        return _REQUIRED_COLUMNS.issubset(headers)
    except Exception:
        return False


def load_bob_owner_map(bob_path: Path) -> dict:
    """
    Load the BoB CSV and return a dict:
      { lowercase_account_name: { "owner": rep_name, "journey_stage": stage, "territory": territory } }
    """
    result = {}
    if not bob_path or not bob_path.exists():
        return result
    try:
        with open(bob_path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                name = (row.get("Account Name") or "").strip()
                owner = (row.get("Account Owner") or "").strip()
                if not name or not owner:
                    continue
                result[name.lower()] = {
                    "owner":        owner,
                    "journey_stage": (row.get("Journey Stage [Enterprise Acquisition Journey]") or "").strip(),
                    "territory":    (row.get("Territory Name") or "").strip(),
                }
    except Exception:
        pass
    return result


def load(
    input_dir: Path,
    week_date: str | None = None,
    history_dir: Path | None = None,
    blacklist: set | None = None,
) -> dict:
    """
    Load LinkedIn CSV from input_dir.

    Returns dict with:
      file_found          — str | None (filename if found)
      li_very_high_set    — set of lowercase account names (Very High engagement only)
      li_all_rows         — dict[str, dict] keyed by lowercase account name (all rows)
    """
    _ENGAGEMENT_RANK = {"Very High": 3, "High": 2, "Medium": 1, "Low": 0}

    result: dict = {
        "file_found": None,
        "li_very_high_set": set(),
        "li_all_rows": {},
    }

    # Find ALL LinkedIn files — skip files that match Demandbase patterns
    _demandbase_patterns = [
        "nosalestouches", "withllostopp", "withlost", "accountsvisiting",
        "activityreport", "newlyengaged", "entintent", "allaccountsatmqa",
        "ent_acq_mqa", "peoplevisiting", "g2",
    ]
    li_files: list[Path] = []
    for f in sorted(input_dir.iterdir()):
        if f.suffix.lower() != ".csv":
            continue
        name_lower = f.name.lower()
        if any(pat in name_lower for pat in _demandbase_patterns):
            continue
        if _is_linkedin_file(f):
            li_files.append(f)

    if not li_files:
        return result

    result["file_found"] = ", ".join(f.name for f in li_files)

    # Save first file to history dir for future delta tracking
    if week_date and history_dir:
        history_dir.mkdir(parents=True, exist_ok=True)
        dest = history_dir / f"{week_date}.csv"
        if not dest.exists():
            try:
                shutil.copy2(str(li_files[0]), str(dest))
            except Exception:
                pass  # non-fatal

    # Parse all files — merge by account name, keeping highest engagement level
    for li_file in li_files:
        with open(li_file, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                name = (row.get("Company Name") or "").strip()
                if not name:
                    continue
                key = name.lower()

                # Apply blacklist if provided
                if blacklist and key in blacklist:
                    continue

                new_level = (row.get("Engagement Level") or "").strip()
                new_rank = _ENGAGEMENT_RANK.get(new_level, 0)

                # If already seen, keep whichever has higher engagement level
                if key in result["li_all_rows"]:
                    existing_rank = _ENGAGEMENT_RANK.get(
                        result["li_all_rows"][key].get("engagement_level", ""), 0
                    )
                    if new_rank <= existing_rank:
                        continue  # existing entry is better or equal

                result["li_all_rows"][key] = {
                    "account":              name,
                    "company_url":          (row.get("Company Page URL") or "").strip(),
                    "engagement_level":     new_level,
                    "organic_impressions":  (row.get("Organic Impressions") or "").strip(),
                    "organic_engagements":  (row.get("Organic Engagements") or "").strip(),
                    "paid_impressions":     (row.get("Paid Impressions") or "").strip(),
                    "paid_clicks":          (row.get("Paid Clicks") or "").strip(),
                    "paid_conversions":     (row.get("Paid Conversions") or "").strip(),
                }
                if new_level in ("Very High", "High"):
                    result["li_very_high_set"].add(key)

    return result
