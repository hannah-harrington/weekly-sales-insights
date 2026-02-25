"""
Demandbase source plugin.

Reads 4 CSV report types exported from Demandbase, normalizes the data,
and returns structured signals grouped by seller.
"""

import csv
from pathlib import Path

# Each CSV type: how to detect it, which field holds the owner, and
# which columns to extract (original CSV name -> normalized key).
CSV_TYPES = {
    "mqa": {
        "pattern": "AccountsMovedToMqa",
        "exclude_pattern": None,
        "owner_field": "Owner Name",
        "columns": {
            "Account Name": "account",
            "Website": "website",
            "Account Grade": "grade",
            "Journey Stage": "journey_stage",
            "Engagement Points (3 mo.)": "engagement_3mo",
            "High Intent Keywords": "keywords",
            "Top Account Categories": "categories",
            "Priority Summary": "priority",
            "Territory Name": "territory",
        },
    },
    "hvp": {
        "pattern": "AccountsVisitingHighValuePages",
        "exclude_pattern": None,
        "owner_field": "Owner Name",
        "columns": {
            "Account Name": "account",
            "Website": "website",
            "Account Grade": "grade",
            "Journey Stage": "journey_stage",
            "Engagement Points (7 days)": "engagement_7d",
            "High Intent Keywords": "keywords",
            "Top Account Categories": "categories",
            "Priority Summary": "priority",
            "Territory Name": "territory",
        },
    },
    "new_people": {
        "pattern": "NewlyEngagedPeopleThisWeek",
        "exclude_pattern": "ActivityReport",
        "owner_field": "Account Owner",
        "columns": {
            "Account Name": "account",
            "Full Name": "full_name",
            "Title": "title",
            "Email": "email",
            "Engagement Points (7 days)": "engagement_7d",
            "First Engagement Date (All Time)": "first_engagement_date",
            "Account Tier - Depreciated": "account_tier",
            "Top Account Categories": "categories",
            "Territory Name": "territory",
        },
    },
    "activity": {
        "pattern": "ActivityReport",
        "exclude_pattern": None,
        "owner_field": "Account Owner",
        "columns": {
            "Account Name": "account",
            "Full Name": "full_name",
            "Title": "title",
            "Category": "category",
            "Details": "details",
            "Territory Name": "territory",
        },
    },
}

# Signal type metadata (used in the JSON output and by the frontend)
SIGNAL_TYPE_META = {
    "mqa": {
        "label": "Accounts Moved to MQA (No Sales Touches)",
        "short_label": "MQA",
        "color": "green",
        "source": "demandbase",
        "description": (
            "A Marketing Qualified Account (MQA) is an account that crossed a "
            "significant engagement threshold through marketing activity — not "
            "sales outreach. An account reaches MQA when it either: "
            "(1) accumulates 200+ marketing engagement points from campaigns, "
            "form fills, and visits to key pages in the last 3 months, or "
            "(2) has 2+ senior contacts (Director, VP, C-suite) each with 30+ "
            "marketing engagement points. These accounts were flagged this week "
            "and have had zero sales engagement in the last 30 days."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "grade", "label": "Grade"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "engagement_3mo", "label": "Engagement (3 mo.)"},
            {"key": "keywords", "label": "Intent Keywords"},
            {"key": "priority", "label": "Priority"},
        ],
    },
    "hvp": {
        "label": "Accounts Visiting High-Value Pages (Lost Opp in Last 12 Mo.)",
        "short_label": "HVP",
        "color": "rose",
        "source": "demandbase",
        "description": (
            "Accounts that had a Closed Lost opportunity in the last 12 months "
            "but are back visiting Shopify Plus pages this week. These are "
            "re-engagement opportunities — the timing is right to reopen "
            "the conversation."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "grade", "label": "Grade"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "engagement_7d", "label": "Engagement (7 days)"},
            {"key": "keywords", "label": "Intent Keywords"},
            {"key": "priority", "label": "Priority"},
        ],
    },
    "new_people": {
        "label": "Newly Engaged People This Week",
        "short_label": "People",
        "color": "indigo",
        "source": "demandbase",
        "description": (
            "Brand-new contacts from target buying-committee titles (marketing, "
            "ecommerce, digital, C-suite) who engaged with Shopify for the "
            "first time ever this week. Fresh inbound signals from people who "
            "weren't previously in our orbit."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "full_name", "label": "Name"},
            {"key": "title", "label": "Title"},
            {"key": "email", "label": "Email"},
            {"key": "engagement_7d", "label": "Engagement (7 days)"},
        ],
    },
    "activity": {
        "label": "Newly Engaged People — Activity Details",
        "short_label": "Activity",
        "color": "amber",
        "source": "demandbase",
        "description": (
            "The specific activities (webinars, events, email clicks, campaigns) "
            "that drove engagement from the newly engaged people above. Use this "
            "to personalize your outreach based on what they actually interacted with."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "full_name", "label": "Name"},
            {"key": "title", "label": "Title"},
            {"key": "category", "label": "Category"},
            {"key": "details", "label": "Details"},
        ],
    },
}


def _safe_float(val: str | None) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def detect_csv_files(directory: Path) -> dict[str, Path]:
    """Find which CSV files in the directory match each report type."""
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
    """Read a CSV file and return a list of row dicts."""
    with open(filepath, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _normalize_row(row: dict[str, str], column_map: dict[str, str]) -> dict[str, str]:
    """Extract and rename columns from a CSV row using the column map."""
    return {
        norm_key: (row.get(csv_col) or "").strip()
        for csv_col, norm_key in column_map.items()
    }


def load(directory: Path) -> dict:
    """
    Load all Demandbase CSVs from the directory.

    Returns a dict with:
      - "signal_types": metadata for each signal type
      - "signals_by_seller": {seller_name: {signal_type: [normalized_rows]}}
      - "raw_signals": {signal_type: [normalized_rows]} (all rows, for highlights)
      - "files_found": {signal_type: filename} (for logging)
    """
    file_map = detect_csv_files(directory)

    signals_by_seller: dict[str, dict[str, list]] = {}
    raw_signals: dict[str, list] = {}
    files_found: dict[str, str] = {}

    for csv_type, filepath in file_map.items():
        cfg = CSV_TYPES[csv_type]
        rows = _read_csv(filepath)
        files_found[csv_type] = filepath.name

        owner_field = cfg["owner_field"]
        column_map = cfg["columns"]
        normalized = []

        for row in rows:
            owner = (row.get(owner_field) or "").strip()
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

    return {
        "signal_types": SIGNAL_TYPE_META,
        "signals_by_seller": signals_by_seller,
        "raw_signals": raw_signals,
        "files_found": files_found,
    }


def build_highlights(raw_signals: dict[str, list], max_count: int = 5) -> list[dict]:
    """
    Pick the top signals across all types, ranked by engagement score.
    Returns a list of highlight dicts for the master dashboard.
    """
    scored: list[tuple[float, dict]] = []

    for row in raw_signals.get("mqa", []):
        pts = _safe_float(row.get("engagement_3mo", "0"))
        scored.append((pts, {
            "type": "mqa",
            "score": pts,
            "title": row.get("account", ""),
            "subtitle": f'{pts:.0f} engagement pts (3 mo.)',
            "detail": row.get("keywords", "") or "Moved to MQA, no sales touches",
        }))

    for row in raw_signals.get("hvp", []):
        pts = _safe_float(row.get("engagement_7d", "0"))
        scored.append((pts * 3, {
            "type": "hvp",
            "score": pts,
            "title": row.get("account", ""),
            "subtitle": f'{pts:.0f} engagement pts this week',
            "detail": row.get("keywords", "") or "Lost opp — back on Plus pages",
        }))

    for row in raw_signals.get("new_people", []):
        pts = _safe_float(row.get("engagement_7d", "0"))
        scored.append((pts, {
            "type": "new_people",
            "score": pts,
            "title": row.get("full_name", ""),
            "subtitle": row.get("title", ""),
            "detail": row.get("account", ""),
        }))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [item for _, item in scored[:max_count]]
