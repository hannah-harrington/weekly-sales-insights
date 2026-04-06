#!/usr/bin/env python3
"""
test_salesloft_api.py — Validate Salesloft API connection and inspect data.

Run this BEFORE wiring Salesloft into the pipeline.
It hits the API, prints what comes back, and tells you if the data
looks right.

Usage:
    SALESLOFT_API_KEY=your_key_here python3 test_salesloft_api.py
    # or:
    export SALESLOFT_API_KEY=your_key_here
    python3 test_salesloft_api.py

    # Check more days of history:
    python3 test_salesloft_api.py --days 30
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Make sure the pipeline module is importable
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.sources.salesloft import fetch_email_clicks, normalise


def main():
    parser = argparse.ArgumentParser(description="Test Salesloft API connection")
    parser.add_argument("--days", type=int, default=7, help="Days of history to fetch (default 7)")
    parser.add_argument("--raw", action="store_true", help="Print the raw first API record in full")
    args = parser.parse_args()

    api_key = os.environ.get("SALESLOFT_API_KEY", "")
    if not api_key:
        print("❌  No API key. Set SALESLOFT_API_KEY env var.")
        print("    Get one from: Salesloft → Settings → API → API Keys → Create key")
        sys.exit(1)

    print(f"🔑  API key found ({len(api_key)} chars)")
    print(f"📅  Fetching email clicks from past {args.days} days...\n")

    # ── Fetch ──────────────────────────────────────────────────────────────
    try:
        records = fetch_email_clicks(api_key, days_back=args.days, debug=True)
    except RuntimeError as e:
        print(f"\n❌  API call failed: {e}")
        print("\nCommon causes:")
        print("  • API key expired or invalid")
        print("  • Personal key — can only see YOUR emails, not all reps'")
        print("  • Needs admin API key for full team access")
        sys.exit(1)

    if not records:
        print("⚠️   No clicked emails returned.")
        print("    Possible reasons:")
        print("    • No emails were sent in the past", args.days, "days")
        print("    • No emails had click tracking enabled")
        print("    • Personal API key (only your emails visible)")
        print("\n    Try: --days 30 to check a wider window")
        sys.exit(0)

    print(f"\n✅  Found {len(records)} clicked emails\n")

    # ── Inspect raw structure ───────────────────────────────────────────────
    first = records[0]
    print("── Raw record keys ──────────────────────────────────────")
    print(sorted(first.keys()))

    if args.raw:
        print("\n── Full first record ────────────────────────────────────")
        print(json.dumps(first, indent=2, default=str))
    else:
        # Print key fields only
        print("\n── First record (key fields) ────────────────────────────")
        for field in ["id", "sent_at", "recipient_email_address", "status",
                      "click_tracking", "recipient", "user", "counts", "cadence"]:
            val = first.get(field)
            if val is not None:
                if isinstance(val, dict):
                    print(f"  {field}:")
                    for k, v in val.items():
                        print(f"      {k}: {v}")
                else:
                    print(f"  {field}: {val}")

    # ── Normalise ──────────────────────────────────────────────────────────
    print("\n── Normalised rows ──────────────────────────────────────")
    rows = normalise(records)
    print(f"  {len(rows)} unique person+rep combinations (deduped from {len(records)} emails)")

    print("\n── Top 10 most-clicked contacts ─────────────────────────")
    for row in rows[:10]:
        name    = row.get("person_name") or "(no name)"
        title   = row.get("title") or "—"
        account = row.get("account") or "(unknown company)"
        rep     = row.get("rep_name") or "(unknown rep)"
        clicks  = row.get("click_count", 0)
        cadences = ", ".join(row.get("cadences", [])) or "—"
        print(f"  {name} ({title}) @ {account}")
        print(f"    Rep: {rep}  |  Clicks: {clicks}  |  Cadence: {cadences}")
        print()

    # ── Check if recipient object has the data we need ─────────────────────
    print("── Recipient object quality check ───────────────────────")
    has_name = sum(1 for r in rows if r.get("person_name") and "@" not in r["person_name"])
    has_title = sum(1 for r in rows if r.get("title"))
    has_account = sum(1 for r in rows if r.get("account"))
    total = len(rows)

    print(f"  Named people:   {has_name}/{total} ({100*has_name//total if total else 0}%)")
    print(f"  Has title:      {has_title}/{total} ({100*has_title//total if total else 0}%)")
    print(f"  Has company:    {has_account}/{total} ({100*has_account//total if total else 0}%)")

    if has_account == 0:
        print("\n  ⚠️  No company names found in recipient object.")
        print("     The API may not return account nesting by default.")
        print("     We may need to fetch /v2/people/{id} separately for company data.")

    print("\n── Rep coverage ─────────────────────────────────────────")
    from collections import Counter
    rep_counts = Counter(r.get("rep_name", "(unknown)") for r in rows)
    for rep, count in rep_counts.most_common(15):
        print(f"  {rep}: {count} clicks")

    print("\n✅  Test complete. If data looks right, we're ready to wire into the pipeline.")


if __name__ == "__main__":
    main()
