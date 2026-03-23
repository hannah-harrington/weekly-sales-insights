"""
Demandbase ANZ source plugin.

Reads ANZ-specific CSV report types, normalizes the data,
and returns structured signals grouped by seller — same interface
as demandbase.py so the pipeline can merge both regions cleanly.
"""

import csv
from pathlib import Path

CSV_TYPES = {
    "anz_high_intent": {
        "pattern": "HighIntentAndNoSalesTouches",
        "exclude_pattern": None,
        "owner_fields": ["Account Owner", "Owner Name"],
        "columns": {
            "Account Name": "account",
            "Industry": "industry",
            "Engagement Points (7 days)": "engagement_7d",
            "Engagement Points (3 mo.)": "engagement_3mo",
            "Account Tier": "tier",
            "All Pipeline Predict Score": "pipeline_score",
            "All Qualification Score": "qual_score",
            "country": "country",
            "Billing Country": "billing_country",
            "Territory Name": "territory",
            "Website": "website",
            "Total Annual Revenue (USD)": "revenue",
        },
    },
    "anz_new_people": {
        "pattern": "NewlyEngagedPeopleThisWeek",
        "exclude_pattern": "ActivityReport",
        "owner_fields": ["Account Owner"],
        "columns": {
            "Account Name": "account",
            "Full Name": "full_name",
            "Title": "title",
            "Email": "email",
            "Engagement Points (7 days)": "engagement_7d",
            "First Engagement Date (All Time)": "first_engagement_date",
            "Account Tier": "tier",
            "Account Grade": "grade",
        },
    },
    "anz_activity": {
        "pattern": "AllActivitiesContact",
        "exclude_pattern": None,
        "owner_fields": [],
        "columns": {
            "Account Name": "account",
            "Full Name": "full_name",
            "Title": "title",
            "Email": "email",
            "Category": "category",
            "Details": "details",
            "Journey Stage": "journey_stage",
            "Activity Date": "activity_date",
        },
    },
    "anz_website_visits": {
        "pattern": "WebsiteVisitsIntentSignals",
        "exclude_pattern": None,
        "owner_fields": ["Account Owner"],
        "columns": {
            "Account Name": "account",
            "Visited Web Page": "page",
            "Details": "details",
            "Journey Stage": "journey_stage",
            "Engagement Points (7 days)": "engagement_7d",
            "Engagement Points (3 mo.)": "engagement_3mo",
            "Billing Country": "country",
            "Total Annual Revenue (USD)": "revenue",
        },
    },
}

SIGNAL_TYPE_META = {
    "anz_high_intent": {
        "label": "High Intent Accounts (No Sales Touches)",
        "short_label": "High Intent",
        "color": "green",
        "source": "demandbase_anz",
        "description": (
            "Accounts showing strong engagement signals in the last 7 days "
            "that have not been contacted by sales yet. These are your "
            "warmest untouched accounts — prioritize outreach here."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "industry", "label": "Industry"},
            {"key": "engagement_7d", "label": "Engagement (7d)"},
            {"key": "engagement_3mo", "label": "Engagement (3mo)"},
            {"key": "territory", "label": "Territory"},
        ],
    },
    "anz_new_people": {
        "label": "Newly Engaged People This Week",
        "short_label": "New People",
        "color": "indigo",
        "source": "demandbase_anz",
        "description": (
            "Contacts who engaged with Shopify for the first time this week. "
            "These are fresh inbound signals from people who weren't "
            "previously in our orbit."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "full_name", "label": "Name"},
            {"key": "title", "label": "Title"},
            {"key": "email", "label": "Email"},
            {"key": "grade", "label": "Grade"},
        ],
    },
    "anz_activity": {
        "label": "Contact Activity Details",
        "short_label": "Activity",
        "color": "amber",
        "source": "demandbase_anz",
        "description": (
            "Specific activities (events, webinars, email clicks, campaigns) "
            "from contacts at your accounts. Use these details to personalize "
            "outreach based on what they actually interacted with."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "full_name", "label": "Name"},
            {"key": "title", "label": "Title"},
            {"key": "category", "label": "Category"},
            {"key": "details", "label": "Details"},
        ],
    },
    "anz_website_visits": {
        "label": "Website Visits & Intent Signals (Last 7 Days)",
        "short_label": "Website Visits",
        "color": "rose",
        "source": "demandbase_anz",
        "description": (
            "Accounts visiting Shopify pages in the last 7 days with intent "
            "signal details. Grouped by account to show which companies are "
            "actively researching Shopify right now."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "visit_count", "label": "Visits"},
            {"key": "engagement_7d", "label": "Engagement (7d)"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "country", "label": "Country"},
        ],
    },
}


def _safe_float(val: str | None) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def detect_csv_files(directory: Path) -> dict[str, Path]:
    """Find which CSV files in the directory match each ANZ report type."""
    csv_files = [f for f in directory.iterdir() if f.suffix.lower() == ".csv"]
    matched: dict[str, Path] = {}

    for csv_type, cfg in CSV_TYPES.items():
        pattern = cfg["pattern"].lower()
        exclude = (cfg["exclude_pattern"] or "").lower()
        for fpath in csv_files:
            fname_lower = fpath.name.lower()
            if pattern in fname_lower:
                if exclude and exclude in fname_lower:
                    continue
                matched[csv_type] = fpath
                break

    return matched


def _read_csv(filepath: Path) -> list[dict[str, str]]:
    with open(filepath, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _normalize_row(row: dict[str, str], column_map: dict[str, str]) -> dict[str, str]:
    return {
        norm_key: (row.get(csv_col) or "").strip()
        for csv_col, norm_key in column_map.items()
    }


def _resolve_owner(row: dict[str, str], owner_fields: list[str]) -> str:
    """Try multiple owner field names, return first non-empty value."""
    for field in owner_fields:
        val = (row.get(field) or "").strip()
        if val:
            return val
    return ""


def _aggregate_website_visits(
    rows: list[dict[str, str]], owner_fields: list[str], column_map: dict[str, str]
) -> tuple[dict[str, dict[str, list]], list[dict]]:
    """
    Group website visits by (owner, account) and produce one summary row
    per account instead of one row per page visit.
    """
    by_owner_account: dict[str, dict[str, list]] = {}

    for row in rows:
        owner = _resolve_owner(row, owner_fields)
        if not owner:
            continue
        account = (row.get("Account Name") or "").strip()
        if not account:
            continue
        key = (owner, account)
        if key not in by_owner_account:
            by_owner_account[key] = {"rows": [], "owner": owner, "account": account}
        by_owner_account[key]["rows"].append(row)

    signals_by_seller: dict[str, dict[str, list]] = {}
    all_normalized: list[dict] = []

    for (owner, account), info in by_owner_account.items():
        visit_rows = info["rows"]
        sample = visit_rows[0]
        norm = _normalize_row(sample, column_map)
        norm["visit_count"] = str(len(visit_rows))
        max_eng = max(_safe_float(r.get("Engagement Points (7 days)", "0")) for r in visit_rows)
        norm["engagement_7d"] = f"{max_eng:.1f}" if max_eng else ""

        all_normalized.append(norm)
        if owner not in signals_by_seller:
            signals_by_seller[owner] = {}
        if "anz_website_visits" not in signals_by_seller[owner]:
            signals_by_seller[owner]["anz_website_visits"] = []
        signals_by_seller[owner]["anz_website_visits"].append(norm)

    return signals_by_seller, all_normalized


def load(directory: Path) -> dict:
    """
    Load all ANZ Demandbase CSVs from the directory.

    Returns the same schema as demandbase.load():
      - "signal_types": metadata for each signal type
      - "signals_by_seller": {seller_name: {signal_type: [normalized_rows]}}
      - "raw_signals": {signal_type: [normalized_rows]}
      - "files_found": {signal_type: filename}
    """
    file_map = detect_csv_files(directory)

    signals_by_seller: dict[str, dict[str, list]] = {}
    raw_signals: dict[str, list] = {}
    files_found: dict[str, str] = {}

    for csv_type, filepath in file_map.items():
        cfg = CSV_TYPES[csv_type]
        rows = _read_csv(filepath)
        files_found[csv_type] = filepath.name
        owner_fields = cfg["owner_fields"]
        column_map = cfg["columns"]

        if csv_type == "anz_website_visits":
            wv_sellers, wv_normalized = _aggregate_website_visits(rows, owner_fields, column_map)
            for seller, sigs in wv_sellers.items():
                if seller not in signals_by_seller:
                    signals_by_seller[seller] = {}
                signals_by_seller[seller]["anz_website_visits"] = sigs["anz_website_visits"]
            raw_signals["anz_website_visits"] = wv_normalized
            continue

        if csv_type == "anz_activity" and not owner_fields:
            normalized = []
            for row in rows:
                norm_row = _normalize_row(row, column_map)
                if not norm_row.get("full_name"):
                    continue
                normalized.append(norm_row)
            raw_signals[csv_type] = normalized
            # Defer seller attribution — done below via account crossref
            continue

        normalized = []
        for row in rows:
            owner = _resolve_owner(row, owner_fields)
            if not owner:
                continue
            norm_row = _normalize_row(row, column_map)
            normalized.append(norm_row)

            if owner not in signals_by_seller:
                signals_by_seller[owner] = {}
            if csv_type not in signals_by_seller[owner]:
                signals_by_seller[owner][csv_type] = []
            signals_by_seller[owner][csv_type].append(norm_row)

        raw_signals[csv_type] = normalized

    # Cross-reference anz_activity by account name using other ANZ reports
    if "anz_activity" in raw_signals and raw_signals["anz_activity"]:
        account_to_seller: dict[str, str] = {}
        for seller_name, sigs in signals_by_seller.items():
            for sig_type in ("anz_high_intent", "anz_new_people", "anz_website_visits"):
                for sig in sigs.get(sig_type, []):
                    acct = (sig.get("account") or "").strip().lower()
                    if acct and acct not in account_to_seller:
                        account_to_seller[acct] = seller_name
        for norm_row in raw_signals["anz_activity"]:
            acct = (norm_row.get("account") or "").strip().lower()
            seller = account_to_seller.get(acct)
            if not seller:
                continue
            if seller not in signals_by_seller:
                signals_by_seller[seller] = {}
            if "anz_activity" not in signals_by_seller[seller]:
                signals_by_seller[seller]["anz_activity"] = []
            signals_by_seller[seller]["anz_activity"].append(norm_row)

    return {
        "signal_types": SIGNAL_TYPE_META,
        "signals_by_seller": signals_by_seller,
        "raw_signals": raw_signals,
        "files_found": files_found,
    }


def build_highlights(raw_signals: dict[str, list], max_count: int = 5) -> list[dict]:
    """Pick top ANZ signals for the highlights section."""
    scored: list[tuple[float, dict]] = []

    for row in raw_signals.get("anz_high_intent", []):
        pts = _safe_float(row.get("engagement_7d", "0"))
        scored.append((pts * 2, {
            "type": "anz_high_intent",
            "score": pts,
            "title": row.get("account", ""),
            "subtitle": row.get("industry", ""),
            "detail": "High intent, no sales touches",
        }))

    for row in raw_signals.get("anz_new_people", []):
        pts = _safe_float(row.get("engagement_7d", "0"))
        scored.append((pts, {
            "type": "anz_new_people",
            "score": pts,
            "title": row.get("full_name", ""),
            "subtitle": row.get("title", ""),
            "detail": row.get("account", ""),
        }))

    for row in raw_signals.get("anz_website_visits", []):
        visits = _safe_float(row.get("visit_count", "0"))
        eng = _safe_float(row.get("engagement_7d", "0"))
        scored.append(((visits * 10 + eng), {
            "type": "anz_website_visits",
            "score": eng,
            "title": row.get("account", ""),
            "subtitle": f'{int(visits)} visits this week',
            "detail": row.get("journey_stage", ""),
        }))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:max_count]]
