#!/usr/bin/env python3
"""
Extract Featured Accounts Log

Parses all weekly JSON data files and produces a flat CSV log of every
account that was featured in a rep's Sales Insights Hub report.

Columns:
  week          — ISO date of the report (e.g. 2026-03-30)
  account       — Account name from Demandbase
  website       — Account website (if available)
  seller_name   — Rep who received the signal
  seller_email  — Rep email
  team          — Rep's team
  segment       — Rep's segment (Enterprise, LA, MM, etc.)
  region        — Rep's region (NA, ANZ, EMEA)
  signal_types  — Comma-separated list of signal types that surfaced this account
  journey_stage — Demandbase journey stage (if available)
  has_open_opp  — Whether account had an open SFDC opp at time of report
  sfdc_opp_count — Number of open SFDC opps at time of report

Usage:
    python -m pipeline.extract_featured_accounts
    python -m pipeline.extract_featured_accounts --output /path/to/output.csv
    python -m pipeline.extract_featured_accounts --week 2026-03-30  # single week
"""

import argparse
import csv
import json
from pathlib import Path
from collections import defaultdict

# Signal types that carry account-level data
ACCOUNT_SIGNAL_TYPES = [
    "mqa_new", "hvp", "hvp_all", "all_mqa",
    "intent_agentic", "intent_compete", "intent_international",
    "intent_marketing", "intent_b2b", "g2_intent",
    "activity",
]

DATA_DIR = Path(__file__).parent.parent / "site" / "data"
DEFAULT_OUTPUT = Path(__file__).parent.parent / "data" / "featured_accounts_log.csv"


def extract_week(week_date: str, data: dict) -> list[dict]:
    """Extract featured accounts from a single week's JSON data."""
    rows = []

    for seller_id, seller in data.get("sellers", {}).items():
        seller_name = seller.get("name", "")
        seller_email = seller.get("email", "")
        team = seller.get("team", "")
        segment = seller.get("segment", "")
        region = seller.get("region", "")

        # Collect all accounts this seller saw this week, grouped by account name
        # key: account_name → {website, journey_stage, has_open_opp, opp_count, signal_types[]}
        account_map: dict[str, dict] = defaultdict(lambda: {
            "website": "",
            "journey_stage": "",
            "has_open_opp": False,
            "sfdc_opp_count": 0,
            "signal_types": [],
        })

        for st in ACCOUNT_SIGNAL_TYPES:
            for sig in seller.get("signals", {}).get(st, []):
                account_name = sig.get("account", "").strip()
                if not account_name:
                    continue

                entry = account_map[account_name]
                if st not in entry["signal_types"]:
                    entry["signal_types"].append(st)

                # Prefer non-empty website
                if not entry["website"] and sig.get("website"):
                    entry["website"] = sig["website"]

                # Prefer non-empty journey stage
                if not entry["journey_stage"] and sig.get("journey_stage"):
                    entry["journey_stage"] = sig["journey_stage"]

                # SFDC enrichment
                sfdc = sig.get("sfdc", {})
                if sfdc:
                    opp_count = sfdc.get("open_opp_count", 0) or 0
                    if opp_count > entry["sfdc_opp_count"]:
                        entry["sfdc_opp_count"] = opp_count
                        entry["has_open_opp"] = opp_count > 0

        # Emit one row per account per seller per week
        for account_name, entry in account_map.items():
            rows.append({
                "week": week_date,
                "account": account_name,
                "website": entry["website"],
                "seller_name": seller_name,
                "seller_email": seller_email,
                "team": team,
                "segment": segment,
                "region": region,
                "signal_types": ",".join(entry["signal_types"]),
                "journey_stage": entry["journey_stage"],
                "has_open_opp": "yes" if entry["has_open_opp"] else "no",
                "sfdc_opp_count": entry["sfdc_opp_count"],
            })

    return rows


def load_weeks(data_dir: Path, week_filter: str | None = None) -> list[tuple[str, dict]]:
    """Load all weekly JSON files, optionally filtered to a single week."""
    files = sorted(data_dir.glob("2[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9].json"))
    results = []
    for f in files:
        week_date = f.stem
        if week_filter and week_date != week_filter:
            continue
        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)
        results.append((week_date, data))
    return results


def write_csv(rows: list[dict], output_path: Path) -> None:
    """Write rows to CSV, creating parent directories if needed."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "week", "account", "website", "seller_name", "seller_email",
        "team", "segment", "region", "signal_types",
        "journey_stage", "has_open_opp", "sfdc_opp_count",
    ]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(
        description="Extract featured accounts from weekly JSON data files."
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"Output CSV path. Default: {DEFAULT_OUTPUT}",
    )
    parser.add_argument(
        "--week", default=None,
        help="Only extract a single week (YYYY-MM-DD). Default: all weeks.",
    )
    parser.add_argument(
        "--data-dir", default=str(DATA_DIR),
        help=f"Directory containing weekly JSON files. Default: {DATA_DIR}",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_path = Path(args.output)

    print("Sales Insights Hub — Featured Accounts Extractor")
    print(f"  Data dir: {data_dir}")
    print(f"  Output:   {output_path}")
    if args.week:
        print(f"  Week:     {args.week} only")
    print()

    weeks = load_weeks(data_dir, week_filter=args.week)
    if not weeks:
        print("ERROR: No matching JSON files found.")
        return

    all_rows = []
    for week_date, data in weeks:
        rows = extract_week(week_date, data)
        # Deduplicate: same account can appear in multiple signal types for same seller
        unique_accounts = len({r["account"] for r in rows})
        unique_sellers = len({r["seller_name"] for r in rows})
        print(f"  {week_date}: {unique_accounts} accounts across {unique_sellers} sellers → {len(rows)} rows")
        all_rows.extend(rows)

    print()
    print(f"Total rows: {len(all_rows)}")
    print(f"Total unique accounts (all time): {len({r['account'] for r in all_rows})}")
    print(f"Total unique sellers (all time):  {len({r['seller_name'] for r in all_rows})}")

    write_csv(all_rows, output_path)
    print()
    print(f"Written to: {output_path}")


if __name__ == "__main__":
    main()
