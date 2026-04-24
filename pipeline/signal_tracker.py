#!/usr/bin/env python3
"""
Sales Insights Hub — Signal Pipeline Tracker

Answers: of accounts surfaced in the hub as signals, what % opened SFDC
pipeline after being featured?

Reads:   data/featured_accounts_log.csv
Queries: BigQuery (mart_revenue_data.sales_opportunity_reporting_metrics)
Outputs: site/data/signal_tracker.json

Usage:
    python -m pipeline.signal_tracker
    python -m pipeline.signal_tracker --window 180
    python -m pipeline.signal_tracker --dry-run
"""

import argparse
import csv
import json
from datetime import date, timedelta
from pathlib import Path
from subprocess import run

_BILLING_PROJECT = "sdp-for-analysts-platform"
_PROJECT_ROOT    = Path(__file__).parent.parent
_LOG_PATH        = _PROJECT_ROOT / "data" / "featured_accounts_log.csv"
_OUTPUT_PATH     = _PROJECT_ROOT / "site" / "data" / "signal_tracker.json"

BOB_SEGMENTS = (
    "Enterprise", "Large", "Large Mid-Mkt", "Mid-Mkt",
    "Global Account", "Enterprise Large",
)

SIGNAL_LABELS = {
    "all_mqa":             "MQA (all)",
    "mqa_new":             "New MQA",
    "hvp_all":             "HVP (all)",
    "hvp":                 "High-Value Prospect",
    "intent_agentic":      "Agentic Intent",
    "intent_compete":      "Compete Intent",
    "intent_international":"International Intent",
    "intent_marketing":    "Marketing Intent",
    "intent_b2b":          "B2B Intent",
    "g2_intent":           "G2 Intent",
    "activity":            "Activity",
}


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def _run_bq(sql: str) -> list[dict]:
    result = run(
        ["bq", "query", f"--project_id={_BILLING_PROJECT}",
         "--use_legacy_sql=false", "--format=json", "--max_rows=50000", sql],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"BigQuery error: {err[:600]}")
    output = result.stdout.strip()
    if not output or output == "[]":
        return []
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"BQ returned unexpected output: {exc}\nRaw: {output[:500]}"
        ) from exc


def _sq(s: str) -> str:
    """Escape backslash and single-quote for BigQuery standard SQL."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _seg_list() -> str:
    return ", ".join(f"'{_sq(s)}'" for s in BOB_SEGMENTS)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_log(since_days: int = 180) -> list[dict]:
    """Load featured accounts log rows within the last N days."""
    cutoff = (date.today() - timedelta(days=since_days)).isoformat()
    rows = []
    with open(_LOG_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["week"] >= cutoff:
                rows.append(row)
    return rows


def first_featured_per_account(log_rows: list[dict]) -> dict[str, dict]:
    """
    For each unique account, return:
      - first_week: earliest week it appeared
      - signal_types: all signal types ever seen for this account
      - had_opp_at_feature: True if it had an open opp when first featured
    """
    first: dict[str, dict] = {}
    for row in sorted(log_rows, key=lambda r: r["week"]):
        name = row["account"].strip()
        sigs = [s.strip() for s in row["signal_types"].split(",") if s.strip()]
        if name not in first:
            first[name] = {
                "first_week": row["week"],
                "signal_types": set(sigs),
                "had_opp_at_feature": row.get("has_open_opp", "no") == "yes",
            }
        else:
            first[name]["signal_types"].update(sigs)
    return first


# ---------------------------------------------------------------------------
# BigQuery query
# ---------------------------------------------------------------------------

def query_opp_dates(account_names_lower: list[str]) -> dict[str, str]:
    """
    For each lowercase account name, return the earliest SFDC opp creation date
    (any opp status — open or closed).

    Returns: dict of lowercase_name -> "YYYY-MM-DD"
    """
    result: dict[str, str] = {}
    CHUNK = 200
    seg_filter = _seg_list()

    for i in range(0, len(account_names_lower), CHUNK):
        chunk = account_names_lower[i:i + CHUNK]
        names_sql = ", ".join(f"'{_sq(n)}'" for n in chunk)
        sql = f"""
SELECT
  LOWER(TRIM(account_name))      AS account_key,
  CAST(MIN(created_date) AS STRING) AS first_opp_date
FROM `shopify-dw.mart_revenue_data.sales_opportunity_reporting_metrics`
WHERE LOWER(TRIM(account_name)) IN ({names_sql})
  AND owner_segment IN ({seg_filter})
GROUP BY 1
"""
        try:
            rows = _run_bq(sql)
        except RuntimeError as exc:
            print(f"  [warn] BQ chunk {i//CHUNK + 1} failed: {exc}")
            continue

        for row in rows:
            key = (row.get("account_key") or "").strip()
            date_str = (row.get("first_opp_date") or "")[:10]
            if key and date_str and len(date_str) == 10:
                result[key] = date_str

    return result


# ---------------------------------------------------------------------------
# Stats computation
# ---------------------------------------------------------------------------

def _rate(n: int, d: int) -> float:
    return round(n / d * 100, 1) if d > 0 else 0.0


def compute_stats(log_rows: list[dict], first: dict[str, dict],
                  opp_dates: dict[str, str]) -> dict:
    """
    Compute signal → pipeline conversion rates.

    A "conversion" = the account had NO open opp when first featured,
    AND an SFDC opp was created after its first_week in the hub.

    Rates reported:
      - overall
      - by signal type
      - by week cohort
    """
    # Deduplicate: one entry per unique account name (earliest appearance)
    seen: set[str] = set()
    deduped: list[dict] = []
    for row in sorted(log_rows, key=lambda r: r["week"]):
        name = row["account"].strip()
        if name not in seen:
            seen.add(name)
            deduped.append(row)

    # Per signal type accumulators
    by_signal: dict[str, dict] = {}
    # Per week accumulators
    by_week: dict[str, dict] = {}

    total_featured   = 0
    total_converted  = 0
    total_excluded   = 0  # had opp when featured — excluded from conversion denominator

    for row in deduped:
        name = row["account"].strip()
        meta = first.get(name, {})
        first_week     = meta.get("first_week") or row["week"]
        had_opp        = meta.get("had_opp_at_feature", False)
        sigs           = [s.strip() for s in row["signal_types"].split(",") if s.strip()]

        # Exclude accounts that already had pipeline when featured
        if had_opp:
            total_excluded += 1
            continue

        total_featured += 1

        # Check conversion
        first_opp_date = opp_dates.get(name.lower())
        converted = bool(first_opp_date and first_opp_date > first_week)

        if converted:
            total_converted += 1

        # By signal type
        for sig in sigs:
            if sig not in by_signal:
                by_signal[sig] = {"featured": 0, "converted": 0}
            by_signal[sig]["featured"] += 1
            if converted:
                by_signal[sig]["converted"] += 1

        # By week cohort
        if first_week not in by_week:
            by_week[first_week] = {"featured": 0, "converted": 0}
        by_week[first_week]["featured"] += 1
        if converted:
            by_week[first_week]["converted"] += 1

    # Build final output — sort signal types by conversion count desc
    by_signal_final = {}
    for sig, d in sorted(by_signal.items(), key=lambda x: -x[1]["converted"]):
        if d["featured"] == 0:
            continue
        by_signal_final[sig] = {
            "label":     SIGNAL_LABELS.get(sig, sig),
            "featured":  d["featured"],
            "converted": d["converted"],
            "rate":      _rate(d["converted"], d["featured"]),
        }

    by_week_final = {}
    for wk in sorted(by_week.keys()):
        d = by_week[wk]
        if d["featured"] == 0:
            continue
        by_week_final[wk] = {
            "featured":  d["featured"],
            "converted": d["converted"],
            "rate":      _rate(d["converted"], d["featured"]),
        }

    return {
        "as_of":      date.today().isoformat(),
        "excluded_had_opp": total_excluded,
        "overall": {
            "featured_accounts": total_featured,
            "converted":         total_converted,
            "rate":              _rate(total_converted, total_featured),
        },
        "by_signal_type": by_signal_final,
        "by_week":        by_week_final,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_tracker(window_days: int = 180, dry_run: bool = False) -> dict:
    """
    Public entry point so other modules (e.g. friday_pipeline.py) can call this.
    Returns the stats dict.
    """
    print("Signal Pipeline Tracker")
    print(f"  Log:    {_LOG_PATH}")
    print(f"  Window: {window_days} days")
    print()

    if not _LOG_PATH.exists():
        raise FileNotFoundError(f"Featured accounts log not found: {_LOG_PATH}")

    log_rows = load_log(since_days=window_days)
    if not log_rows:
        raise ValueError(f"No log rows within the last {window_days} days.")

    print(f"  {len(log_rows)} log rows loaded")

    first = first_featured_per_account(log_rows)
    account_names = list(first.keys())
    print(f"  {len(account_names)} unique featured accounts")

    print("\nQuerying BigQuery for opp data...")
    opp_dates = query_opp_dates([n.lower() for n in account_names])
    print(f"  {len(opp_dates)} accounts matched to SFDC opps")

    print("\nComputing conversion stats...")
    stats = compute_stats(log_rows, first, opp_dates)

    ov = stats["overall"]
    print(f"\nResults:")
    print(f"  {ov['converted']} / {ov['featured_accounts']} accounts converted ({ov['rate']}%)")
    print(f"  ({stats['excluded_had_opp']} excluded — had opp when first featured)\n")
    print("  By signal type:")
    for sig, d in stats["by_signal_type"].items():
        print(f"    {d['label']:<28}  {d['converted']}/{d['featured']}  ({d['rate']}%)")

    if not dry_run:
        _OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
        print(f"\nWritten: {_OUTPUT_PATH}")
    else:
        print("\n[dry-run] Output not written.")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Sales Insights Hub — Signal Pipeline Tracker"
    )
    parser.add_argument("--window", type=int, default=180,
                        help="Days of history to include (default: 180)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute stats but do not write output file")
    args = parser.parse_args()
    run_tracker(window_days=args.window, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
