"""
Demandbase source plugin.

Reads 4 CSV report types exported from Demandbase, normalizes the data,
and returns structured signals grouped by seller.
"""

import csv
import json
from pathlib import Path

# Each CSV type: how to detect it, which field holds the owner, and
# which columns to extract (original CSV name -> normalized key).
CSV_TYPES = {
    "mqa_new": {
        "pattern": "NoSalesTouches",
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
        "pattern": "WithLostOpp",
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
    "hvp_all": {
        "pattern": "AccountsVisitingHighValuePages",
        "exclude_pattern": "WithLostOpp",
        "owner_field": "Owner Name",
        "columns": {
            "Account Name": "account",
            "Website": "website",
            "Account Grade": "grade",
            "Journey Stage": "journey_stage",
            "Priority Summary": "priority",
            "Territory Name": "territory",
            "Enterprise Web Visits (7 days)": "web_visits",
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
        "exclude_pattern": "Newly",
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
    "all_mqa": {
        "pattern": ["ENT_Acq_MQA", "AllAccountsAtMqa"],
        "exclude_pattern": None,
        "owner_field": "Account Owner",
        "columns": {
            "Account Name": "account",
            "Territory Name": "territory",
            "Industry": "industry",
            "Billing State/Province": "state",
            "Total Annual Revenue (USD)": "revenue",
            "Customer Fit Signals": "fit_signals",
        },
    },
    "intent": {
        "pattern": "EntIntent",
        "exclude_pattern": None,
        "owner_field": "Owner Name",
        "columns": {
            "Account Name": "account",
            "Journey Stage": "journey_stage",
            "Engaged Known People": "engaged_people",
            "Engagement Points (3 mo.)": "engagement_3mo",
            "High Intent Keywords": "high_intent_keywords",
            "Enterprise Intent Keyword Sets (7 days)": "intent_sets",
            "Competitive Intent Keywords (High)": "competitive_keywords",
        },
    },
}

# Intent category definitions — keyword → category signal type
INTENT_CATEGORIES = {
    "intent_agentic": {
        "keywords": [
            "agentic commerce", "ai chatbot", "ai search", "large language models",
            "conversational commerce", "ai shopping assistant", "ai powered shopping",
            "ai driven personalization", "ai product recommendations", "ai commerce",
            "ai search visibility", "ai generated product recommendations", "customer experience ai",
        ],
    },
    "intent_compete": {
        "keywords": [
            "odoo", "woocommerce", "bigcommerce", "sfcc", "salesforce commerce cloud",
            "adobe commerce", "sap commerce cloud", "commercetools", "shopware", "vtex",
            "optimizely", "cegid", "sitoo", "square pos", "square point of sale",
            "toast point of sale", "dynamics 365 commerce",
        ],
    },
    "intent_international": {
        "keywords": [
            "cross border commerce", "import and export", "international business transactions",
            "cross border business", "global shipping services", "expansion into emerging markets",
        ],
    },
    "intent_marketing": {
        "keywords": [
            "shop pay", "shop campaigns", "retail technology", "digital transformation strategy",
            "shopify plus", "shopify shipping",
        ],
    },
    "intent_b2b": {
        "keywords": [
            "credit management", "erp integration", "pos terminal", "point of sale system",
            "point of sale terminal", "mobile pos",
        ],
    },
}

# Flat keyword → category lookup
_KW_TO_INTENT_CAT: dict[str, str] = {
    kw: cat
    for cat, cfg in INTENT_CATEGORIES.items()
    for kw in cfg["keywords"]
}

# Signal type metadata (used in the JSON output and by the frontend)
SIGNAL_TYPE_META = {
    "mqa_new": {
        "label": "New MQA Accounts This Week",
        "short_label": "New MQA",
        "color": "green",
        "source": "demandbase",
        "description": (
            "A Marketing Qualified Account (MQA) is an account that crossed a "
            "significant engagement threshold through marketing activity. "
            "An account reaches MQA when it either: "
            "(1) accumulates 200+ marketing engagement points from campaigns, "
            "form fills, and visits to key pages in the last 3 months, or "
            "(2) has 2+ senior contacts (Director, VP, C-suite) each with 30+ "
            "marketing engagement points. These are all accounts that became "
            "marketing-qualified in the last 7 days."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "grade", "label": "Grade"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "priority", "label": "Priority"},
            {"key": "keywords", "label": "Topics"},
            {"key": "categories", "label": "Categories"},
        ],
    },
    "hvp": {
        "label": "Accounts Visiting High-Value Pages (Lost Opp in Last 12 Mo.)",
        "short_label": "High-Value Pages",
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
            {"key": "priority", "label": "Priority"},
            {"key": "keywords", "label": "Topics"},
            {"key": "categories", "label": "Categories"},
        ],
    },
    "hvp_all": {
        "label": "Accounts Visiting High-Value Pages",
        "short_label": "High-Value Pages (All)",
        "color": "rose",
        "source": "demandbase",
        "description": (
            "All accounts visiting Shopify Plus and enterprise pages this week — "
            "not just lost opps. This is the full picture of who is browsing "
            "high-value content right now, regardless of deal history."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "platform", "label": "Platform"},
            {"key": "grade", "label": "Grade"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "pages_visited", "label": "Pages Visited"},
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
            {"key": "categories", "label": "Categories"},
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
    "all_mqa": {
        "label": "All Accounts at MQA Status",
        "short_label": "All MQA",
        "color": "slate",
        "source": "demandbase",
        "description": (
            "A snapshot of every account currently at MQA status in your book. "
            "This is your full MQA universe — not just what moved this week, "
            "but every account that has qualified and is still active. Use this "
            "as a reference to prioritize across your pipeline."
        ),
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "platform", "label": "Platform"},
            {"key": "industry", "label": "Industry"},
            {"key": "state", "label": "State"},
            {"key": "revenue", "label": "Revenue"},
            {"key": "pages_visited", "label": "Pages Visited"},
        ],
    },
}


_INTENT_META = {
    "intent_agentic": {
        "label": "Agentic Commerce Intent",
        "short_label": "Agentic Commerce",
        "color": "violet",
        "description": "Accounts in your book showing high intent around AI, agentic commerce, LLMs, and conversational commerce. These accounts are researching where AI fits in their commerce stack.",
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "matched_keywords", "label": "Signals"},
            {"key": "engagement_3mo", "label": "Engagement (3mo)"},
        ],
    },
    "intent_compete": {
        "label": "Competitive Intent",
        "short_label": "Compete",
        "color": "red",
        "description": "Accounts actively researching Shopify's competitors. They're evaluating their options — good time to get in front of them.",
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "matched_keywords", "label": "Researching"},
            {"key": "engagement_3mo", "label": "Engagement (3mo)"},
        ],
    },
    "intent_international": {
        "label": "International Commerce Intent",
        "short_label": "International",
        "color": "teal",
        "description": "Accounts showing intent around cross-border commerce, international expansion, and global selling. Shopify Markets is the angle.",
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "matched_keywords", "label": "Signals"},
            {"key": "engagement_3mo", "label": "Engagement (3mo)"},
        ],
    },
    "intent_marketing": {
        "label": "Marketing & Growth Intent",
        "short_label": "Marketing",
        "color": "orange",
        "description": "Accounts researching marketing, growth, and commerce platform topics aligned to Shopify's marketing and retail offerings.",
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "matched_keywords", "label": "Signals"},
            {"key": "engagement_3mo", "label": "Engagement (3mo)"},
        ],
    },
    "intent_b2b": {
        "label": "B2B Commerce Intent",
        "short_label": "B2B",
        "color": "blue",
        "description": "Accounts researching B2B commerce topics — POS, ERP integration, credit management. Shopify B2B and POS are the angles.",
        "display_columns": [
            {"key": "account", "label": "Account"},
            {"key": "journey_stage", "label": "Stage"},
            {"key": "matched_keywords", "label": "Signals"},
            {"key": "engagement_3mo", "label": "Engagement (3mo)"},
        ],
    },
}

# Merge intent metadata into the main signal type registry
SIGNAL_TYPE_META.update({
    k: {**v, "source": "demandbase"}
    for k, v in _INTENT_META.items()
})


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
        raw_pattern = cfg["pattern"]
        patterns = [p.lower() for p in raw_pattern] if isinstance(raw_pattern, list) else [raw_pattern.lower()]
        exclude = (cfg["exclude_pattern"] or "").lower()
        for fpath in csv_files:
            fname_lower = fpath.name.lower()
            if any(p in fname_lower for p in patterns):
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


def _format_revenue(val: str) -> str:
    """Format a revenue value (possibly scientific notation) as a readable dollar string."""
    try:
        n = float(val)
        if n >= 1_000_000_000:
            return f"${n / 1_000_000_000:.1f}B"
        elif n >= 1_000_000:
            return f"${n / 1_000_000:.0f}M"
        elif n >= 1_000:
            return f"${n / 1_000:.0f}K"
        else:
            return f"${n:,.0f}"
    except (ValueError, TypeError):
        return val


def _normalize_row(row: dict[str, str], column_map: dict[str, str]) -> dict[str, str]:
    """Extract and rename columns from a CSV row using the column map."""
    result = {
        norm_key: (row.get(csv_col) or "").strip()
        for csv_col, norm_key in column_map.items()
    }
    if "revenue" in result and result["revenue"]:
        result["revenue"] = _format_revenue(result["revenue"])
    return result


def _load_enrichment(directory: Path) -> dict:
    """Load Signal Hub enrichment data if available alongside the CSVs."""
    # Look in pipeline dir (sibling of sources/)
    candidates = [
        Path(__file__).parent.parent / "signal_hub_enrichment.json",
        directory / "signal_hub_enrichment.json",
    ]
    for path in candidates:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    return {}


def _enrich_row(row: dict, enrichment: dict) -> dict:
    """Add platform and page visit data from Signal Hub enrichment."""
    if not enrichment:
        return row
    key = row.get("account", "").lower().strip()
    match = enrichment.get(key)
    if not match:
        return row
    row = dict(row)
    row["platform"] = match.get("currentPlatform") or ""
    pages = match.get("pageVisits") or []
    # Deduplicate and clean up page paths
    seen = set()
    clean = []
    for p in pages:
        p = p.replace("shopify.com", "").strip("/")
        if p and p not in seen:
            seen.add(p)
            clean.append(p)
    row["pages_visited"] = ", ".join(clean[:4]) if clean else ""
    return row


# Page path → human-readable research signal
_PAGE_SIGNALS = {
    "pricing": "pricing pages",
    "compare": "competitor comparison pages",
    "plus": "Shopify Plus pages",
    "enterprise": "enterprise solution pages",
    "pos": "POS / retail pages",
    "free-trial": "free trial pages",
    "capital": "Shopify Capital pages",
    "b2b": "B2B pages",
    "sell": "general commerce pages",
}


def _summarise_pages(pages: list[str]) -> str:
    """Turn a list of page paths into a readable research summary."""
    matched = []
    seen = set()
    for page in pages:
        for key, label in _PAGE_SIGNALS.items():
            if key in page and label not in seen:
                matched.append(label)
                seen.add(label)
    if not matched:
        return ""
    if len(matched) == 1:
        return matched[0]
    return ", ".join(matched[:-1]) + " and " + matched[-1]


def generate_mqa_brief(row: dict, enrichment: dict, sfdc_info: dict | None = None) -> str:
    """
    Generate a short account brief for a newly-qualified MQA account.
    Uses Demandbase signals + Signal Hub enrichment + optional SFDC data from BigQuery.

    sfdc_info (optional): result of sfdc_bq.lookup() for this account. When present,
    adds open opp status, last rep activity, and engaged contacts to the brief.
    """
    account = row.get("account", "")
    grade = row.get("grade", "")
    keywords = row.get("keywords", "")
    try:
        eng = float(row.get("engagement_3mo") or 0)
        eng_str = f"{eng:,.0f}"
    except ValueError:
        eng_str = ""

    hub = enrichment.get(account.lower().strip(), {})
    platform = hub.get("currentPlatform", "")
    pages = hub.get("pageVisits") or []
    g2 = hub.get("g2Activities") or []

    parts = []

    # ── SFDC context (most actionable — goes first) ───────────────────────
    if sfdc_info:
        # Open opportunities
        open_opps = sfdc_info.get("open_opps") or []
        if open_opps:
            opp = open_opps[0]
            stage = opp.get("stage") or ""
            acv = opp.get("acv_usd")
            try:
                acv_str = f"${float(acv):,.0f}" if acv is not None else ""
            except (ValueError, TypeError):
                acv_str = ""
            if stage and acv_str:
                parts.append(f"Open opp in {stage} ({acv_str} ACV).")
            elif stage:
                parts.append(f"Open opp in {stage}.")
            if len(open_opps) > 1:
                parts[-1] = parts[-1].rstrip(".") + f" (+{len(open_opps) - 1} more opp)."

        # Last rep activity
        from pipeline.sources.sfdc_bq import days_since
        days = days_since(sfdc_info.get("last_activity_date"))
        if days is not None:
            if days == 0:
                parts.append("Rep active today.")
            elif days <= 7:
                parts.append(f"Last rep touch {days}d ago.")
            elif days <= 30:
                parts.append(f"Last rep touch {days}d ago — warm.")
            elif days <= 90:
                parts.append(f"Last rep touch {days}d ago — worth a check-in.")
            else:
                parts.append(f"No rep activity in {days}d — cold account.")
        else:
            parts.append("No rep activity on record.")

        # Engaged contacts (active in last 90 days, grouped by title)
        contacts = sfdc_info.get("engaged_contacts") or []
        if contacts:
            total_count = sum(c.get("count", 1) for c in contacts)
            titles = [c["title"] for c in contacts[:3] if c.get("title")]
            if total_count == 1 and titles:
                parts.append(f"1 engaged contact: {titles[0]}.")
            elif titles:
                parts.append(
                    f"{total_count} engaged contact{'s' if total_count > 1 else ''} incl. {', '.join(titles[:2])}."
                )

    # ── Platform ──────────────────────────────────────────────────────────
    if platform and platform not in ("", "Other"):
        parts.append(f"Currently on {platform}.")

    # ── Research signals from page visits ─────────────────────────────────
    page_summary = _summarise_pages(pages)
    if page_summary:
        parts.append(f"Recently researching {page_summary} on Shopify.com.")

    # ── G2 activity ───────────────────────────────────────────────────────
    if g2:
        competitors = set()
        for activity in g2[:3]:
            for vendor in ["Adobe", "Magento", "BigCommerce", "Salesforce", "SAP", "Netsuite",
                           "WooCommerce", "Pimcore", "FastSpring", "Shopaccino"]:
                if vendor.lower() in activity.lower():
                    competitors.add(vendor)
        if competitors:
            parts.append(f"Evaluating {', '.join(sorted(competitors))} on G2.")
        else:
            parts.append("Active on G2 reviewing commerce platforms.")

    # ── Demandbase keywords ───────────────────────────────────────────────
    if keywords:
        kw_list = [k.strip() for k in keywords.split(",") if k.strip()]
        if kw_list:
            parts.append(f"Showing intent around: {', '.join(kw_list[:3])}.")

    # ── Engagement score ──────────────────────────────────────────────────
    if eng_str:
        grade_label = {"A": "top-fit", "B": "strong-fit", "C": "mid-fit", "D": "lower-fit"}.get(grade, "")
        grade_str = f" ({grade_label} account)" if grade_label else ""
        parts.append(f"{eng_str} engagement points in the last 3 months{grade_str}.")

    # ── Suggested angle ───────────────────────────────────────────────────
    if platform and "Salesforce" in platform:
        parts.append("Lead with migration complexity and speed-to-market vs SFCC.")
    elif platform and "SAP" in platform:
        parts.append("Lead with total cost of ownership and modern stack.")
    elif platform and "BigCommerce" in platform:
        parts.append("Lead with Shopify's enterprise ecosystem and checkout performance.")
    elif pages and any("pos" in p for p in pages):
        parts.append("POS interest — worth leading with unified online + retail story.")
    elif pages and any("capital" in p for p in pages):
        parts.append("Capital interest — flag Shopify Capital as part of the conversation.")
    elif pages and any("plus" in p or "enterprise" in p for p in pages):
        parts.append("Actively evaluating Plus/Enterprise — outreach timing is strong.")

    if not parts:
        return ""

    return " ".join(parts)


def enrich_briefs_with_sfdc(source_data: dict, sfdc_data: dict) -> None:
    """
    Post-process: re-generate MQA briefs with SFDC data and update in place.

    Mutates source_data["raw_signals"]["mqa_new"] rows and all corresponding
    rows in source_data["signals_by_seller"] so every brief includes SFDC context.
    """
    from pipeline.sources.sfdc_bq import lookup as sfdc_lookup

    enrichment = _load_enrichment(Path("."))  # fallback — enrichment already applied

    updated = 0
    for row in source_data.get("raw_signals", {}).get("mqa_new", []):
        sfdc_info = sfdc_lookup(sfdc_data, row.get("account", ""), row.get("website", ""))
        if sfdc_info:
            row["brief"] = generate_mqa_brief(row, {}, sfdc_info)
            row["sfdc"] = {
                "open_opp_count": len(sfdc_info.get("open_opps") or []),
                "last_activity_date": sfdc_info.get("last_activity_date"),
                "engaged_contact_count": sum(c.get("count", 1) for c in (sfdc_info.get("engaged_contacts") or [])),
            }
            updated += 1

    # Mirror updates into per-seller signal rows (same account name = same data)
    for seller_signals in source_data.get("signals_by_seller", {}).values():
        for row in seller_signals.get("mqa_new", []):
            sfdc_info = sfdc_lookup(sfdc_data, row.get("account", ""), row.get("website", ""))
            if sfdc_info:
                row["brief"] = generate_mqa_brief(row, {}, sfdc_info)
                row["sfdc"] = {
                    "open_opp_count": len(sfdc_info.get("open_opps") or []),
                    "last_activity_date": sfdc_info.get("last_activity_date"),
                    "engaged_contact_count": sum(c.get("count", 1) for c in (sfdc_info.get("engaged_contacts") or [])),
                }

    return updated


def _process_intent_csv(
    rows: list[dict],
    signals_by_seller: dict,
    raw_signals: dict,
) -> None:
    """
    Split intent CSV rows into per-category signal buckets.
    One account can appear in multiple categories.
    Capped at top 10 per category per seller by engagement score.
    """
    MAX_PER_SELLER = 10

    for row in rows:
        owner = (row.get("owner") or "").strip()
        if not owner:
            continue

        all_kws = set()
        for field in ("intent_sets", "high_intent_keywords", "competitive_keywords"):
            for kw in (row.get(field) or "").split(","):
                kw = kw.strip().lower()
                if kw:
                    all_kws.add(kw)

        # Find which categories this account matches
        cat_keywords: dict[str, list[str]] = {}
        for kw in all_kws:
            cat = _KW_TO_INTENT_CAT.get(kw)
            if cat:
                cat_keywords.setdefault(cat, []).append(kw)

        for cat, matched_kws in cat_keywords.items():
            norm = {
                "account": row.get("account", ""),
                "journey_stage": row.get("journey_stage", ""),
                "engagement_3mo": row.get("engagement_3mo", ""),
                "engaged_people": row.get("engaged_people", ""),
                "matched_keywords": ", ".join(sorted(set(matched_kws))),
            }

            # Add to raw signals
            raw_signals.setdefault(cat, []).append(norm)

            # Add to seller bucket
            if owner not in signals_by_seller:
                signals_by_seller[owner] = {}
            signals_by_seller[owner].setdefault(cat, []).append(norm)

    # Sort each seller's intent buckets by engagement descending, cap at MAX
    for seller in signals_by_seller:
        for cat in INTENT_CATEGORIES:
            rows_list = signals_by_seller[seller].get(cat, [])
            if rows_list:
                rows_list.sort(key=lambda r: _safe_float(r.get("engagement_3mo")), reverse=True)
                signals_by_seller[seller][cat] = rows_list[:MAX_PER_SELLER]


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
    enrichment = _load_enrichment(directory)
    enrich_types = {"hvp_all", "all_mqa", "mqa_new"}

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
            if csv_type in enrich_types:
                norm_row = _enrich_row(norm_row, enrichment)
            if csv_type == "mqa_new":
                norm_row["brief"] = generate_mqa_brief(norm_row, enrichment)
            if csv_type == "intent":
                norm_row["owner"] = owner  # preserve owner for intent processing
            normalized.append(norm_row)

            if csv_type == "intent":
                continue  # handled separately below

            if owner not in signals_by_seller:
                signals_by_seller[owner] = {}
            if csv_type not in signals_by_seller[owner]:
                signals_by_seller[owner][csv_type] = []
            signals_by_seller[owner][csv_type].append(norm_row)

        if csv_type == "intent":
            _process_intent_csv(normalized, signals_by_seller, raw_signals)
        else:
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

    grade_score = {"A": 100, "B": 70, "C": 40, "D": 20}

    for row in raw_signals.get("mqa_new", []):
        pts = _safe_float(row.get("engagement_3mo", "0"))
        g = grade_score.get(row.get("grade", ""), 0)
        scored.append((pts + g, {
            "type": "mqa_new",
            "score": pts,
            "title": row.get("account", ""),
            "subtitle": f'Grade {row.get("grade", "—")} · {row.get("journey_stage", "")}',
            "detail": "New MQA this week",
        }))

    for row in raw_signals.get("hvp", []):
        pts = _safe_float(row.get("engagement_7d", "0"))
        g = grade_score.get(row.get("grade", ""), 0)
        scored.append(((pts + g) * 3, {
            "type": "hvp",
            "score": pts,
            "title": row.get("account", ""),
            "subtitle": f'Grade {row.get("grade", "—")} · Lost opp re-engagement',
            "detail": row.get("priority", "") or "Back on Plus pages",
        }))

    for row in raw_signals.get("hvp_all", []):
        g = grade_score.get(row.get("grade", ""), 0)
        scored.append((g * 2, {
            "type": "hvp_all",
            "score": g,
            "title": row.get("account", ""),
            "subtitle": f'Grade {row.get("grade", "—")} · Visiting high-value pages',
            "detail": row.get("priority", "") or "Active on enterprise pages",
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
