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

from pipeline.config import (
    ARCHIVE_DIR,
    CSV_INPUT_DIR,
    DATA_DIR,
    DEPLOY_SITE_NAME,
    PROJECT_ROOT,
    REGION_MAP,
    SALES_NAV_LEADS_FILE,
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
from pipeline import slack_notify
from pipeline import lead_notify


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

    update_weeks_index()
    return dated_path


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
        ["quick", "deploy", ".", DEPLOY_SITE_NAME],
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
        "--no-sfdc", action="store_true",
        help="Skip SFDC enrichment from BigQuery (useful for offline/test runs).",
    )
    args = parser.parse_args()

    week_date = args.date or get_monday_date().isoformat()
    input_dir = Path(args.input_dir) if args.input_dir else CSV_INPUT_DIR
    anz_input_dir = Path(args.anz_input_dir) if args.anz_input_dir else None

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
        count = len(source_data["raw_signals"].get(csv_type, []))
        print(f"  {count:>4} rows  <- {fname}")

    missing = [
        st for st in demandbase.CSV_TYPES if st not in source_data["files_found"]
    ]
    if missing:
        labels = [demandbase.SIGNAL_TYPE_META[m]["short_label"] for m in missing]
        print(f"  Warning: Missing CSVs for: {', '.join(labels)}")
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
    if not args.no_sfdc:
        mqa_rows = source_data["raw_signals"].get("mqa_new", [])
        if mqa_rows:
            print("Loading SFDC enrichment from BigQuery...")
            try:
                websites = [r.get("website", "") for r in mqa_rows]
                names = [r.get("account", "") for r in mqa_rows]
                sfdc_data = sfdc_bq.load(names=names, websites=websites)
                updated = demandbase.enrich_briefs_with_sfdc(source_data, sfdc_data)
                print(f"  Enriched {updated} of {len(mqa_rows)} MQA briefs with SFDC data")
            except Exception as exc:
                print(f"  Warning: SFDC enrichment failed ({exc}) — briefs will use Demandbase-only data")
        print()
    else:
        print("Skipping SFDC enrichment (--no-sfdc).")
        print()

    # --- Load Sales Nav leads ---
    if SALES_NAV_LEADS_FILE.exists():
        print(f"Loading Sales Nav leads from {SALES_NAV_LEADS_FILE.name}...")
    else:
        print(f"  Sales Nav file not found ({SALES_NAV_LEADS_FILE.name}) — skipping Top Leads")

    # --- Build JSON ---
    print("Building JSON data model...")
    data = build_json(week_date, source_data, anz_source_data=anz_source_data)

    seller_count = data["meta"]["total_sellers"]
    signal_count = data["meta"]["sellers_with_signals"]
    print(f"  {seller_count} sellers total, {signal_count} with signals this week")
    print(f"  {len(data['highlights'])} highlights")
    for team_name, team_info in data["teams"].items():
        print(f"  {team_name}: {len(team_info['sellers'])} sellers")
    print()

    # --- Write JSON ---
    print("Writing JSON...")
    json_path = write_json(data, week_date)
    print(f"  {json_path}")
    print(f"  {DATA_DIR / 'current.json'}")
    print(f"  {DATA_DIR / 'weeks.json'}")
    print()

    # --- Archive ---
    if not args.no_archive:
        print("Archiving CSVs...")
        moved = archive_csvs(input_dir, week_date)
        print(f"  Moved {moved} files to archive/{week_date}/")
    else:
        print("Skipping CSV archive (--no-archive).")
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
