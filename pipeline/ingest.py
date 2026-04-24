#!/usr/bin/env python3
"""
Weekly Sales Insights — Data Pipeline

Reads source data (currently Demandbase CSVs), builds a unified JSON
data file, archives raw inputs, and optionally deploys to Quick.

Usage:
    python -m pipeline.ingest
    python -m pipeline.ingest --date 2026-02-23
    python -m pipeline.ingest --deploy
    python -m pipeline.ingest --no-archive
"""

import argparse
import json
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import csv
from pipeline.config import (
    ARCHIVE_DIR,
    CSV_INPUT_DIR,
    DATA_DIR,
    DEPLOY_SITE_NAME,
    PROJECT_ROOT,
    REGION_MAP,
    SALES_NAV_LEADS_FILE,
    BOB_FILE,
    SITE_DIR,
    SITE_URL,
    TEAM_LEADS,
    TEAM_ORDER,
    build_identity_map,
    seller_id_for,
    seller_record,
    team_for,
)
from pipeline.sources import demandbase
from pipeline.sources import demandbase_anz
from pipeline.sources import salesnav
from pipeline.sources import sfdc_bq
from pipeline.sources import news as news_fetcher
from pipeline.sources import linkedin as linkedin_source
from pipeline import slack_notify
from pipeline import lead_notify
from pipeline import signal_tracker


def get_monday_date() -> date:
    """Return the most recent Monday (today if Monday, else last Monday)."""
    today = date.today()
    return today - timedelta(days=today.weekday())


def build_json(week_date: str, source_data: dict, anz_source_data: dict | None = None) -> dict:
    """
    Build the unified JSON structure from source plugin output.

    Takes the raw output from demandbase.load() and optionally
    demandbase_anz.load(), merges them, and transforms into the
    canonical data model that the SPA reads.
    """
    signals_by_seller = dict(source_data["signals_by_seller"])
    raw_signals = dict(source_data["raw_signals"])
    signal_types = dict(source_data["signal_types"])

    # Merge ANZ data if provided (additive — does not touch NA data)
    if anz_source_data:
        for seller_name, seller_signals in anz_source_data["signals_by_seller"].items():
            if seller_name not in signals_by_seller:
                signals_by_seller[seller_name] = {}
            for sig_type, rows in seller_signals.items():
                signals_by_seller[seller_name][sig_type] = rows
        for sig_type, rows in anz_source_data["raw_signals"].items():
            raw_signals[sig_type] = rows
        signal_types.update(anz_source_data["signal_types"])

    sellers: dict[str, dict] = {}
    all_signal_type_keys = list(signal_types.keys())

    for seller_name, seller_signals in signals_by_seller.items():
        sid = seller_id_for(seller_name)
        rec = seller_record(seller_name)

        summary = {}
        total = 0
        for st in all_signal_type_keys:
            count = len(seller_signals.get(st, []))
            summary[st] = count
            total += count
        summary["total"] = total

        team = rec["team"] or "Unassigned"
        region = REGION_MAP.get(team, "NA")

        # Infer region for unassigned sellers based on their signal types
        if team == "Unassigned" and region == "NA":
            has_anz = any(len(seller_signals.get(st, [])) > 0 for st in all_signal_type_keys if st.startswith("anz_"))
            has_na = any(len(seller_signals.get(st, [])) > 0 for st in all_signal_type_keys if not st.startswith("anz_"))
            if has_anz and not has_na:
                region = "ANZ"

        sellers[sid] = {
            "name": rec["name"],
            "email": rec["email"],
            "team": team,
            "segment": rec["segment"],
            "region": region,
            "summary": summary,
            "signals": {
                st: seller_signals.get(st, [])
                for st in all_signal_type_keys
            },
        }

    # Build team rollups
    teams: dict[str, dict] = defaultdict(
        lambda: {"lead": None, "sellers": [], "summary": {}, "region": "NA"}
    )
    for sid, seller in sellers.items():
        team_name = seller["team"]
        if teams[team_name]["lead"] is None:
            teams[team_name]["lead"] = TEAM_LEADS.get(team_name)
        teams[team_name]["region"] = REGION_MAP.get(team_name, "NA")
        teams[team_name]["sellers"].append(sid)
        for st in all_signal_type_keys:
            teams[team_name]["summary"][st] = (
                teams[team_name]["summary"].get(st, 0) + seller["summary"].get(st, 0)
            )

    ordered_teams: dict[str, dict] = {}
    for tn in TEAM_ORDER:
        if tn in teams:
            ordered_teams[tn] = teams[tn]
    for tn in teams:
        if tn not in ordered_teams:
            ordered_teams[tn] = teams[tn]

    # Inject Sales Nav top leads for Consumer team reps
    nav_leads = salesnav.load(SALES_NAV_LEADS_FILE, signals_by_seller)
    nav_count = 0
    for seller_name, leads in nav_leads.items():
        sid = seller_id_for(seller_name)
        if sid in sellers:
            sellers[sid]["signals"]["top_leads"] = leads
            sellers[sid]["summary"]["top_leads"] = len(leads)
            nav_count += len(leads)

    # Build highlights (NA + ANZ combined)
    highlights = demandbase.build_highlights(raw_signals)
    if anz_source_data:
        anz_highlights = demandbase_anz.build_highlights(anz_source_data["raw_signals"])
        highlights = sorted(
            highlights + anz_highlights,
            key=lambda h: h.get("score", 0),
            reverse=True,
        )[:5]

    # Attach seller info to highlights
    name_to_id = {s["name"]: sid for sid, s in sellers.items()}
    for h in highlights:
        h["seller_id"] = None
        h["seller_name"] = None
        for seller_name, seller_signals in signals_by_seller.items():
            type_signals = seller_signals.get(h["type"], [])
            for sig in type_signals:
                match_key = "account" if h["type"] in (
                    "mqa_new", "mqa", "hvp", "hvp_all", "all_mqa",
                    "anz_high_intent", "anz_website_visits",
                ) else "full_name"
                if sig.get(match_key) == h["title"]:
                    h["seller_id"] = name_to_id.get(seller_name)
                    h["seller_name"] = seller_name
                    break
            if h["seller_id"]:
                break

    all_signal_types = dict(signal_types)
    all_signal_types.update(salesnav.SIGNAL_TYPE_META)

    sources = ["demandbase"]
    if anz_source_data and anz_source_data["files_found"]:
        sources.append("demandbase_anz")
    if nav_count > 0:
        sources.append("salesnav")

    return {
        "meta": {
            "week_of": week_date,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sources": sources,
            "total_sellers": len(sellers),
            "sellers_with_signals": sum(
                1 for s in sellers.values() if s["summary"]["total"] > 0
            ),
        },
        "identity": build_identity_map(),
        "signal_types": {
            st: {k: v for k, v in meta.items()}
            for st, meta in all_signal_types.items()
        },
        "sellers": dict(sorted(sellers.items(), key=lambda x: x[1]["name"])),
        "highlights": highlights,
        "teams": ordered_teams,
        "account_details":        source_data.get("account_details", {}),
        "account_activities":     source_data.get("account_activities", {}),
        "account_news":           source_data.get("account_news", {}),
        "hvp_people_by_account":  source_data.get("hvp_people_by_account", {}),
    }


def write_json(data: dict, week_date: str) -> Path:
    """Write the JSON data file to site/data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    dated_path = DATA_DIR / f"{week_date}.json"
    with open(dated_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    current_path = DATA_DIR / "current.json"
    with open(current_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # current-v2.json is the file the site loads (supports BQ-enriched and non-enriched runs)
    current_v2_path = DATA_DIR / "current-v2.json"
    with open(current_v2_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    update_weeks_index()
    return dated_path


# Signal types that carry account-level data (used for featured accounts logging)
_ACCOUNT_SIGNAL_TYPES = [
    "mqa_new", "hvp", "hvp_all", "all_mqa",
    "intent_agentic", "intent_compete", "intent_international",
    "intent_marketing", "intent_b2b", "g2_intent", "activity",
]

_FEATURED_LOG_PATH = PROJECT_ROOT / "data" / "featured_accounts_log.csv"
_FEATURED_LOG_FIELDNAMES = [
    "week", "account", "website", "seller_name", "seller_email",
    "team", "segment", "region", "signal_types",
    "journey_stage", "has_open_opp", "sfdc_opp_count",
]


def log_featured_accounts(data: dict, week_date: str) -> int:
    """
    Append this week's featured accounts to the running history log.

    Skips any (week, account, seller) rows already in the file so re-runs
    are safe. Returns the number of new rows written.
    """
    _FEATURED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Build set of existing (week, account, seller) keys to avoid duplicates
    existing: set[tuple[str, str, str]] = set()
    if _FEATURED_LOG_PATH.exists():
        with open(_FEATURED_LOG_PATH, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing.add((row["week"], row["account"], row["seller_name"]))

    new_rows = []
    for seller in data.get("sellers", {}).values():
        seller_name  = seller.get("name", "")
        seller_email = seller.get("email", "")
        team         = seller.get("team", "")
        segment      = seller.get("segment", "")
        region       = seller.get("region", "")

        account_map: dict[str, dict] = {}
        for st in _ACCOUNT_SIGNAL_TYPES:
            for sig in seller.get("signals", {}).get(st, []):
                name = sig.get("account", "").strip()
                if not name:
                    continue
                entry = account_map.setdefault(name, {
                    "website": "", "journey_stage": "",
                    "has_open_opp": False, "sfdc_opp_count": 0,
                    "signal_types": [],
                })
                if st not in entry["signal_types"]:
                    entry["signal_types"].append(st)
                if not entry["website"] and sig.get("website"):
                    entry["website"] = sig["website"]
                if not entry["journey_stage"] and sig.get("journey_stage"):
                    entry["journey_stage"] = sig["journey_stage"]
                sfdc = sig.get("sfdc") or {}
                opp_count = sfdc.get("open_opp_count", 0) or 0
                if opp_count > entry["sfdc_opp_count"]:
                    entry["sfdc_opp_count"] = opp_count
                    entry["has_open_opp"] = True

        for account_name, entry in account_map.items():
            key = (week_date, account_name, seller_name)
            if key in existing:
                continue
            new_rows.append({
                "week":           week_date,
                "account":        account_name,
                "website":        entry["website"],
                "seller_name":    seller_name,
                "seller_email":   seller_email,
                "team":           team,
                "segment":        segment,
                "region":         region,
                "signal_types":   ",".join(entry["signal_types"]),
                "journey_stage":  entry["journey_stage"],
                "has_open_opp":   "yes" if entry["has_open_opp"] else "no",
                "sfdc_opp_count": entry["sfdc_opp_count"],
            })

    write_header = not _FEATURED_LOG_PATH.exists() or _FEATURED_LOG_PATH.stat().st_size == 0
    with open(_FEATURED_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FEATURED_LOG_FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(new_rows)

    return len(new_rows)


def update_weeks_index():
    """Update the weeks.json file listing all available weeks."""
    weeks = sorted(
        [
            f.stem
            for f in DATA_DIR.iterdir()
            if f.suffix == ".json" and f.stem not in ("current", "weeks")
            and f.stem[:4].isdigit()
        ],
        reverse=True,
    )
    weeks_path = DATA_DIR / "weeks.json"
    with open(weeks_path, "w", encoding="utf-8") as f:
        json.dump(weeks, f, indent=2)


def archive_csvs(input_dir: Path, week_date: str) -> int:
    """Move raw CSVs into archive/YYYY-MM-DD/."""
    archive_dest = ARCHIVE_DIR / week_date
    archive_dest.mkdir(parents=True, exist_ok=True)

    csv_files = [f for f in input_dir.iterdir() if f.suffix.lower() == ".csv"]
    for f in csv_files:
        shutil.move(str(f), str(archive_dest / f.name))
    return len(csv_files)


def deploy(site_dir: Path) -> str | None:
    """Deploy the site directory to Quick."""
    print(f"Deploying to {DEPLOY_SITE_NAME}.quick.shopify.io ...")
    result = subprocess.run(
        ["quick", "deploy", ".", DEPLOY_SITE_NAME, "--force"],
        cwd=str(site_dir),
        input="y\n",
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        url = f"https://{DEPLOY_SITE_NAME}.quick.shopify.io"
        print(f"  Live at: {url}")
        return url
    else:
        print(f"  Deploy failed (exit {result.returncode}):", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr.strip()}", file=sys.stderr)
        if result.stdout:
            print(f"  {result.stdout.strip()}", file=sys.stderr)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Weekly Sales Insights — Data Pipeline"
    )
    parser.add_argument(
        "--date", default=None,
        help="Report date (YYYY-MM-DD). Defaults to this Monday.",
    )
    parser.add_argument(
        "--input-dir", default=None,
        help=f"Directory containing source CSVs. Defaults to: {CSV_INPUT_DIR}",
    )
    parser.add_argument(
        "--no-archive", action="store_true",
        help="Skip archiving CSV files (useful for testing).",
    )
    parser.add_argument(
        "--deploy", action="store_true",
        help="Deploy site to quick.shopify.io after generating.",
    )
    parser.add_argument(
        "--anz-input-dir", default=None,
        help="Directory containing ANZ Demandbase CSVs (optional).",
    )
    parser.add_argument(
        "--notify", action="store_true",
        help="Send Slack DMs to reps with signals (requires SLACK_BOT_TOKEN).",
    )
    parser.add_argument(
        "--notify-leads", action="store_true",
        help="Send weekly summary DMs to team leads and Brandon Gracey.",
    )
    parser.add_argument(
        "--notify-personal", action="store_true",
        help="Send personalised 'Start here' DMs to reps (account-level callouts). Replaces --notify when ready.",
    )
    parser.add_argument(
        "--no-sfdc", action="store_true",
        help="Skip SFDC enrichment from BigQuery (useful for offline/test runs).",
    )
    parser.add_argument(
        "--no-news", action="store_true",
        help="Skip Google News fetch (useful for quick/offline runs).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help=(
            "Build JSON to a temp file only. Does NOT write current.json, "
            "does NOT deploy. Safe for testing without touching production data."
        ),
    )
    args = parser.parse_args()

    if args.dry_run and args.deploy:
        print("ERROR: --dry-run and --deploy are mutually exclusive.", file=sys.stderr)
        sys.exit(1)

    week_date = args.date or get_monday_date().isoformat()
    input_dir = Path(args.input_dir) if args.input_dir else CSV_INPUT_DIR
    anz_input_dir = Path(args.anz_input_dir) if args.anz_input_dir else None

    # --- Startup validation (fail fast before any work) ---
    if not BOB_FILE.exists():
        print(
            f"ERROR: Book of Business file not found at:\n"
            f"  {BOB_FILE}\n\n"
            f"LinkedIn routing requires this file. Update BOB_FILE in pipeline/config.py\n"
            f"or copy the BoB CSV to the expected path before running.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Weekly Sales Insights — Pipeline")
    print(f"  Week:      {week_date}")
    print(f"  Input dir: {input_dir}")
    if anz_input_dir:
        print(f"  ANZ dir:   {anz_input_dir}")
    print(f"  Output:    {DATA_DIR}")
    print()

    # --- Load NA source data ---
    print("Loading Demandbase CSVs (NA)...")
    source_data = demandbase.load(input_dir)

    if not source_data["files_found"]:
        print("ERROR: No NA CSV files found.", file=sys.stderr)
        sys.exit(1)

    for csv_type, fname in source_data["files_found"].items():
        if csv_type == "intent":
            # intent rows are split into sub-categories — sum them all
            count = sum(
                len(source_data["raw_signals"].get(cat, []))
                for cat in demandbase.INTENT_CATEGORIES
            )
        elif csv_type == "hvp_people":
            # hvp_people stored as by-account lookup, not raw_signals
            count = sum(len(v) for v in source_data.get("hvp_people_by_account", {}).values())
        else:
            count = len(source_data["raw_signals"].get(csv_type, []))
        print(f"  {count:>4} rows  <- {fname}")

    missing = [
        st for st in demandbase.CSV_TYPES if st not in source_data["files_found"]
    ]
    if missing:
        labels = [demandbase.SIGNAL_TYPE_META[m]["short_label"] if m in demandbase.SIGNAL_TYPE_META else m for m in missing]
        print(f"  Warning: Missing CSVs for: {', '.join(labels)}")
    print()

    # --- Load LinkedIn data ---
    _li_history_dir = PROJECT_ROOT / "pipeline" / "linkedin_history"
    _li_blacklist = set(json.loads((PROJECT_ROOT / "pipeline" / "blacklist.json").read_text()).get("accounts", []))
    li_data = linkedin_source.load(
        input_dir,
        week_date=week_date,
        history_dir=_li_history_dir,
        blacklist=_li_blacklist,
    )
    if li_data["file_found"]:
        _li_set = li_data["li_very_high_set"]
        print(f"LinkedIn: {len(_li_set)} Very High engagement accounts  <- {li_data['file_found']}")
        # Annotate matching signal rows — same dict objects used in signals_by_seller
        for _sig_rows in source_data["raw_signals"].values():
            for _row in _sig_rows:
                if isinstance(_row, dict) and _row.get("account", "").lower() in _li_set:
                    _row["li_very_high"] = True
        # Snapshot of accounts already in Hub (before routing LinkedIn)
        _hub_accounts_pre: set[str] = {
            _row.get("account", "").lower()
            for _st_rows in source_data["raw_signals"].values()
            for _row in _st_rows
            if isinstance(_row, dict) and _row.get("account") and _st_rows is not source_data["raw_signals"].get("li_very_high", [])
        }
        # --- Route LinkedIn Very High BoB accounts to sellers ---
        _bob_map = linkedin_source.load_bob_owner_map(BOB_FILE)
        # Load name aliases — maps LI names (lowercase) → BoB names (lowercase)
        _alias_file = PROJECT_ROOT / "pipeline" / "linkedin_aliases.json"
        _li_aliases: dict[str, str] = {}
        if _alias_file.exists():
            try:
                _li_aliases = json.loads(_alias_file.read_text()).get("aliases", {})
            except Exception:
                pass
        _routed = 0
        for _key in list(_li_set):
            if _key in _hub_accounts_pre:
                continue  # already in Hub via Demandbase — badge only
            # Apply alias: if LI name maps to a different BoB name, use that for lookup
            _bob_key = _li_aliases.get(_key, _key)
            _bob_entry = _bob_map.get(_bob_key)
            if not _bob_entry:
                continue
            _owner = _bob_entry["owner"]
            # Match owner name to a known seller
            _matched = None
            for _sname in source_data["signals_by_seller"]:
                if _sname.lower().strip() == _owner.lower().strip():
                    _matched = _sname
                    break
            if not _matched:
                # Try partial first-name + last-name match
                for _sname in source_data["signals_by_seller"]:
                    if _owner.lower() in _sname.lower() or _sname.lower() in _owner.lower():
                        _matched = _sname
                        break
            if not _matched:
                # Seller not in this week's signals_by_seller — add them
                _matched = _owner
            li_row = dict(li_data["li_all_rows"][_key])
            li_row["journey_stage"] = _bob_entry["journey_stage"]
            li_row["territory"] = _bob_entry["territory"]
            if _matched not in source_data["signals_by_seller"]:
                source_data["signals_by_seller"][_matched] = {}
            if "li_very_high" not in source_data["signals_by_seller"][_matched]:
                source_data["signals_by_seller"][_matched]["li_very_high"] = []
            source_data["signals_by_seller"][_matched]["li_very_high"].append(li_row)
            if "li_very_high" not in source_data["raw_signals"]:
                source_data["raw_signals"]["li_very_high"] = []
            source_data["raw_signals"]["li_very_high"].append(li_row)
            _routed += 1
        # Add signal type metadata
        source_data["signal_types"].update(linkedin_source.SIGNAL_TYPE_META)
        print(f"  Routed {_routed} LinkedIn Very High BoB accounts to sellers")
    else:
        _li_set = set()
        print("LinkedIn: no LinkedIn CSV found in input dir — skipping")
    print()

    # --- Load ANZ source data ---
    anz_source_data = None
    if anz_input_dir:
        if anz_input_dir.is_dir():
            print("Loading Demandbase CSVs (ANZ)...")
            anz_source_data = demandbase_anz.load(anz_input_dir)
            for csv_type, fname in anz_source_data["files_found"].items():
                count = len(anz_source_data["raw_signals"].get(csv_type, []))
                print(f"  {count:>4} rows  <- {fname}")
            anz_missing = [
                st for st in demandbase_anz.CSV_TYPES if st not in anz_source_data["files_found"]
            ]
            if anz_missing:
                labels = [demandbase_anz.SIGNAL_TYPE_META[m]["short_label"] for m in anz_missing]
                print(f"  Warning: Missing ANZ CSVs for: {', '.join(labels)}")
            print()
        else:
            print(f"  ANZ input dir not found ({anz_input_dir}) — skipping ANZ")
            print()

    # --- SFDC enrichment from BigQuery ---
    # Derive signal type lists from metadata flags — no hardcoded lists to maintain.
    # To add a new signal type: set sfdc_enrich/news_fetch in its SIGNAL_TYPE_META entry.
    _all_signal_meta = {**demandbase.SIGNAL_TYPE_META, **linkedin_source.SIGNAL_TYPE_META}
    _sfdc_signal_types = [k for k, v in _all_signal_meta.items() if v.get("sfdc_enrich")]
    _news_signal_types = [k for k, v in _all_signal_meta.items() if v.get("news_fetch")]

    if not args.no_sfdc:
        all_sfdc_names: list[str] = []
        all_sfdc_websites: list[str] = []
        for _st in _sfdc_signal_types:
            for r in source_data["raw_signals"].get(_st, []):
                if r.get("account"):
                    all_sfdc_names.append(r["account"])
                if r.get("website"):
                    all_sfdc_websites.append(r["website"])

        if all_sfdc_names or all_sfdc_websites:
            print("Loading SFDC enrichment from BigQuery...")
            try:
                sfdc_data = sfdc_bq.load(names=all_sfdc_names, websites=all_sfdc_websites)
                updated = demandbase.enrich_briefs_with_sfdc(source_data, sfdc_data)
                total_accounts = len({n.lower() for n in all_sfdc_names if n})
                print(f"  Enriched {updated} rows across {total_accounts} accounts with SFDC data")
            except Exception as exc:
                print(f"  Warning: SFDC enrichment failed ({exc}) — reports will use Demandbase-only data")

            # Pull rich account details (overview, platform, revenue, SFDC links, etc.)
            print("Loading SFDC account details from BigQuery...")
            try:
                source_data["account_details"] = sfdc_bq.load_account_details(all_sfdc_names)
                print(f"  Loaded details for {len(source_data['account_details'])} accounts")
            except Exception as exc:
                print(f"  Warning: Account details failed ({exc}) — skipping")
                source_data["account_details"] = {}

            # Pull recent SFDC activity log per account
            print("Loading SFDC activity history from BigQuery...")
            try:
                source_data["account_activities"] = sfdc_bq.load_account_activities(all_sfdc_names)
                print(f"  Loaded activity history for {len(source_data['account_activities'])} accounts")
            except Exception as exc:
                print(f"  Warning: Activity history failed ({exc}) — skipping")
                source_data["account_activities"] = {}

        # --- People contact enrichment (new_people + activity rows) ---
        people_accounts = list({
            r["account"]
            for st in ("new_people", "activity")
            for r in source_data["raw_signals"].get(st, [])
            if r.get("account")
        })
        if people_accounts:
            print("Loading SFDC people contact data from BigQuery...")
            try:
                people_contact_data = sfdc_bq.load_people_contact_data(people_accounts)
                enriched_people = 0
                for st in ("new_people", "activity"):
                    for row in source_data["raw_signals"].get(st, []):
                        match = sfdc_bq.match_person_contact(
                            people_contact_data,
                            row.get("account", ""),
                            row.get("title", ""),
                            row.get("full_name", ""),
                        )
                        if match:
                            row["sfdc_contact"] = match
                            enriched_people += 1
                        else:
                            row["sfdc_contact"] = {"in_sfdc": False}
                print(f"  Enriched {enriched_people} people rows with SFDC contact data")
            except Exception as exc:
                print(f"  Warning: People contact enrichment failed ({exc}) — skipping")
        print()
    else:
        print("Skipping SFDC enrichment (--no-sfdc).")
        print()

    # --- Google News fetch ---
    if not args.no_news:
        news_account_names = list({
            r["account"]
            for _st in _news_signal_types
            for r in source_data["raw_signals"].get(_st, [])
            if r.get("account")
        })
        if news_account_names:
            print("Fetching Google News for top accounts...")
            try:
                source_data["account_news"] = news_fetcher.fetch_account_news(
                    news_account_names, week_date=week_date
                )
                accounts_with_news = sum(
                    1 for v in source_data["account_news"].values() if v
                )
                print(f"  Got news for {accounts_with_news}/{len(news_account_names)} accounts")
            except Exception as exc:
                print(f"  Warning: News fetch failed ({exc}) — skipping")
                source_data["account_news"] = {}
        else:
            source_data["account_news"] = {}
        print()
    else:
        print("Skipping Google News fetch (--no-news).")
        source_data["account_news"] = {}
        print()

    # --- Load Sales Nav leads ---
    if SALES_NAV_LEADS_FILE.exists():
        print(f"Loading Sales Nav leads from {SALES_NAV_LEADS_FILE.name}...")
    else:
        print(f"  Sales Nav file not found ({SALES_NAV_LEADS_FILE.name}) — skipping Top Leads")

    # --- Build JSON ---
    print("Building JSON data model...")
    data = build_json(week_date, source_data, anz_source_data=anz_source_data)

    # --- LinkedIn Very High accounts not already in any seller signal ---
    _hub_accounts: set[str] = set()
    for _seller in data["sellers"].values():
        for _st_rows in _seller["signals"].values():
            for _row in _st_rows:
                if isinstance(_row, dict) and _row.get("account"):
                    _hub_accounts.add(_row["account"].lower())
    data["li_very_high_new"] = [
        li_data["li_all_rows"][key]
        for key in li_data["li_very_high_set"]
        if key not in _hub_accounts
    ]
    if data["li_very_high_new"]:
        print(f"  LinkedIn Very High (not in Hub): {len(data['li_very_high_new'])} accounts")

    seller_count = data["meta"]["total_sellers"]
    signal_count = data["meta"]["sellers_with_signals"]
    print(f"  {seller_count} sellers total, {signal_count} with signals this week")
    print(f"  {len(data['highlights'])} highlights")
    for team_name, team_info in data["teams"].items():
        print(f"  {team_name}: {len(team_info['sellers'])} sellers")
    print()

    # --- Write JSON ---
    if args.dry_run:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(
            suffix=f"-{week_date}-dry-run.json", delete=False, mode="w", encoding="utf-8"
        )
        json.dump(data, tmp, indent=2, ensure_ascii=False)
        tmp.close()
        print(f"DRY RUN — JSON written to temp file (current.json NOT touched):")
        print(f"  {tmp.name}")
        print()
        print("Dry run complete. No files were modified. No deploy. No Slacks.")
        return
    else:
        print("Writing JSON...")
        json_path = write_json(data, week_date)
        print(f"  {json_path}")
        print(f"  {DATA_DIR / 'current.json'}")
        print(f"  {DATA_DIR / 'weeks.json'}")
        print()

    # --- Log featured accounts ---
    print("Logging featured accounts...")
    new_rows = log_featured_accounts(data, week_date)
    print(f"  {new_rows} new rows appended to {_FEATURED_LOG_PATH.name}")
    print()

    # --- Archive ---
    if not args.no_archive:
        print("Archiving CSVs...")
        moved = archive_csvs(input_dir, week_date)
        print(f"  Moved {moved} files to archive/{week_date}/")
    else:
        print("Skipping CSV archive (--no-archive).")
    print()

    # --- Signal tracker ---
    if not args.dry_run:
        print("Updating signal pipeline tracker...")
        try:
            signal_tracker.run_tracker(window_days=180, dry_run=False)
            print("  signal_tracker.json written.")
        except Exception as exc:
            print(f"  [warn] Signal tracker failed: {exc}")
        print()

    # --- Deploy ---
    if args.deploy:
        deploy(SITE_DIR)
        print()

    # --- Slack DMs to reps ---
    if args.notify:
        token = slack_notify.get_token()
        if token:
            print("Sending Slack DMs to reps...")
            notify_stats = slack_notify.notify_all(data, SITE_URL, token)
            print(f"  Sent {notify_stats['sent']} DMs, skipped {notify_stats['skipped']} (no signals/email), {notify_stats['failed']} failed")
        else:
            print("WARNING: --notify requested but no Slack token found. Run: node ~/pi-backup/refresh-callm-creds.js")
        print()

    # --- Personalised Slack DMs to reps ---
    if args.notify_personal:
        token = slack_notify.get_token()
        if token:
            print("Sending personalised Slack DMs to reps...")
            p_stats = slack_notify.notify_all_personal(data, SITE_URL, token)
            print(f"  Sent {p_stats['sent']}, skipped {p_stats['skipped']}, failed {p_stats['failed']}")
        else:
            print("WARNING: --notify-personal requested but no Slack token found. Run: node ~/pi-backup/refresh-callm-creds.js")
        print()

    # --- Slack DMs to leads ---
    if args.notify_leads:
        token = slack_notify.get_token()
        if token:
            print("Sending Slack DMs to team leads...")
            lead_stats = lead_notify.notify_leads(data, token, week_date)
            print(f"  Sent {lead_stats['sent']}, skipped {lead_stats['skipped']}, failed {lead_stats['failed']}")
        else:
            print("WARNING: --notify-leads requested but no Slack token found. Run: node ~/pi-backup/refresh-callm-creds.js")
        print()

    print("Done!")
    if not args.deploy:
        print(f"  JSON: {json_path}")
        print(f"  To deploy: python -m pipeline.ingest --deploy")


if __name__ == "__main__":
    main()
