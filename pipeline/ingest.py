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
from pipeline.sources import salesnav
from pipeline import slack_notify


def get_monday_date() -> date:
    """Return the most recent Monday (today if Monday, else last Monday)."""
    today = date.today()
    return today - timedelta(days=today.weekday())


def build_json(week_date: str, source_data: dict) -> dict:
    """
    Build the unified JSON structure from source plugin output.

    Takes the raw output from demandbase.load() and transforms it into
    the canonical data model that the SPA reads.
    """
    signals_by_seller = source_data["signals_by_seller"]
    raw_signals = source_data["raw_signals"]
    signal_types = source_data["signal_types"]

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

        sellers[sid] = {
            "name": rec["name"],
            "email": rec["email"],
            "team": rec["team"] or "Unassigned",
            "segment": rec["segment"],
            "summary": summary,
            "signals": {
                st: seller_signals.get(st, [])
                for st in all_signal_type_keys
            },
        }

    # Build team rollups
    teams: dict[str, dict] = defaultdict(
        lambda: {"lead": None, "sellers": [], "summary": {}}
    )
    for sid, seller in sellers.items():
        team_name = seller["team"]
        if teams[team_name]["lead"] is None:
            teams[team_name]["lead"] = TEAM_LEADS.get(team_name)
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

    highlights = demandbase.build_highlights(raw_signals)

    # Attach seller info to highlights
    name_to_id = {s["name"]: sid for sid, s in sellers.items()}
    for h in highlights:
        # Find which seller owns this highlight by matching against signal data
        h["seller_id"] = None
        h["seller_name"] = None
        for seller_name, seller_signals in signals_by_seller.items():
            type_signals = seller_signals.get(h["type"], [])
            for sig in type_signals:
                match_key = "account" if h["type"] in ("mqa_new", "mqa", "hvp", "hvp_all", "all_mqa") else "full_name"
                if sig.get(match_key) == h["title"]:
                    h["seller_id"] = name_to_id.get(seller_name)
                    h["seller_name"] = seller_name
                    break
            if h["seller_id"]:
                break

    all_signal_types = dict(signal_types)
    all_signal_types.update(salesnav.SIGNAL_TYPE_META)

    sources = ["demandbase"]
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
        "--notify", action="store_true",
        help="Send Slack DMs to reps with signals (requires SLACK_BOT_TOKEN).",
    )
    args = parser.parse_args()

    week_date = args.date or get_monday_date().isoformat()
    input_dir = Path(args.input_dir) if args.input_dir else CSV_INPUT_DIR

    print("Weekly Sales Insights — Pipeline")
    print(f"  Week:      {week_date}")
    print(f"  Input dir: {input_dir}")
    print(f"  Output:    {DATA_DIR}")
    print()

    # --- Load source data ---
    print("Loading Demandbase CSVs...")
    source_data = demandbase.load(input_dir)

    if not source_data["files_found"]:
        print("ERROR: No CSV files found.", file=sys.stderr)
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

    # --- Load Sales Nav leads ---
    if SALES_NAV_LEADS_FILE.exists():
        print(f"Loading Sales Nav leads from {SALES_NAV_LEADS_FILE.name}...")
    else:
        print(f"  Sales Nav file not found ({SALES_NAV_LEADS_FILE.name}) — skipping Top Leads")

    # --- Build JSON ---
    print("Building JSON data model...")
    data = build_json(week_date, source_data)

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

    # --- Slack DMs ---
    if args.notify:
        token = slack_notify.get_token()
        if token:
            print("Sending Slack DMs...")
            stats = slack_notify.notify_all(data, SITE_URL, token)
            print(f"  Sent {stats['sent']} DMs, skipped {stats['skipped']} (no signals/email), {stats['failed']} failed")
        else:
            print("WARNING: --notify requested but SLACK_BOT_TOKEN not set. Skipping Slack DMs.")
        print()

    print("Done!")
    if not args.deploy:
        print(f"  JSON: {json_path}")
        print(f"  To deploy: python -m pipeline.ingest --deploy")


if __name__ == "__main__":
    main()
