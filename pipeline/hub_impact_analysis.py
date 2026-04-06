#!/usr/bin/env python3
"""
Sales Insights Hub — Impact Analysis

Runs two separate comparisons to measure hub impact:

  Analysis 1 — Pipeline Progression
    Featured accounts that already HAD an open opp vs.
    all BoB accounts with opps that weren't featured.
    Question: do deals at featured accounts progress faster?

  Analysis 2 — New Deal Creation
    All featured accounts vs. all BoB prospect accounts not featured.
    Question: does appearing in the hub increase the chance of a new deal?

Outputs:
  data/hub_impact_summary.csv       — headline rates for both analyses
  data/hub_impact_progression.csv   — account-level for Analysis 1
  data/hub_impact_newdeal.csv       — account-level for Analysis 2

Usage:
    python -m pipeline.hub_impact_analysis
    python -m pipeline.hub_impact_analysis --window 60
    python -m pipeline.hub_impact_analysis --since 2026-03-02
"""

import argparse
import csv
import json
from pathlib import Path
from subprocess import run

_BILLING_PROJECT = "sdp-for-analysts-platform"
_PROJECT_ROOT    = Path(__file__).parent.parent
_LOG_PATH        = _PROJECT_ROOT / "data" / "featured_accounts_log.csv"
_SUMMARY_PATH    = _PROJECT_ROOT / "data" / "hub_impact_summary.csv"
_PROGRESSION_PATH = _PROJECT_ROOT / "data" / "hub_impact_progression.csv"
_NEWDEAL_PATH    = _PROJECT_ROOT / "data" / "hub_impact_newdeal.csv"

# Segments that constitute the enterprise/commercial BoB
BOB_SEGMENTS = (
    "Enterprise", "Large", "Large Mid-Mkt", "Mid-Mkt",
    "Global Account", "Enterprise Large",
)


# -------------------------------------------------------------------------
# BigQuery helper
# -------------------------------------------------------------------------

def _run_bq(sql: str) -> list[dict]:
    result = run(
        ["bq", "query", f"--project_id={_BILLING_PROJECT}",
         "--use_legacy_sql=false", "--format=json", "--max_rows=100000", sql],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        err = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"BigQuery error: {err[:600]}")
    output = result.stdout.strip()
    if not output or output == "[]":
        return []
    return json.loads(output)


def _sq(s: str) -> str:
    """Escape backslashes and single quotes for BigQuery standard SQL."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _seg_list(segs=BOB_SEGMENTS) -> str:
    return ", ".join(f"'{_sq(s)}'" for s in segs)


# -------------------------------------------------------------------------
# Load + process featured accounts log
# -------------------------------------------------------------------------

def load_log(log_path: Path, since: str | None = None) -> list[dict]:
    rows = []
    with open(log_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if since and row["week"] < since:
                continue
            rows.append(row)
    return rows


def first_featured_per_account(log_rows: list[dict]) -> dict[str, dict]:
    """Earliest feature week per account name."""
    first: dict[str, dict] = {}
    for row in sorted(log_rows, key=lambda r: r["week"]):
        name = row["account"].strip()
        if name not in first:
            first[name] = {
                "account":      name,
                "website":      row["website"],
                "first_week":   row["week"],
                "segment":      row["segment"],
                "region":       row["region"],
                "signal_types": row["signal_types"],
            }
    return first


# -------------------------------------------------------------------------
# BigQuery queries
# -------------------------------------------------------------------------

def _featured_union(chunk: list[str], first: dict) -> str:
    """Build UNION ALL SELECT block for a list of featured account names."""
    def _val(n):
        fw = first[n]["first_week"]
        return f"SELECT '{_sq(n)}' AS account_name, DATE '{fw}' AS first_week"
    return "\n  UNION ALL ".join(_val(n) for n in chunk if n in first)


def query_featured_opp_data(chunk: list[str], first: dict, window: int) -> list[dict]:
    """
    For featured accounts: return opp + progression data.
    One row per (account, opp). Accounts with no opp return one NULL-opp row.
    """
    union_rows = _featured_union(chunk, first)
    if not union_rows:
        return []
    sql = f"""
WITH featured AS (
  {union_rows}
),
opps AS (
  SELECT o.opportunity_id, o.account_name, o.created_date, o.owner_segment, o.owner_region
  FROM `shopify-dw.mart_revenue_data.sales_opportunity_reporting_metrics` o
  WHERE LOWER(TRIM(o.account_name)) IN (SELECT LOWER(TRIM(account_name)) FROM featured)
    AND o.owner_segment IN ({_seg_list()})
    AND o.is_closed = FALSE  -- open opps only
),
progressions AS (
  SELECT p.opportunity_id, MIN(p.date) AS first_progression_date
  FROM `shopify-dw.mart_revenue_data.sales_opportunity_progression_daily` p
  WHERE p.has_stage_progressed_opportunity_id IS NOT NULL
  GROUP BY 1
)
SELECT
  f.account_name,
  f.first_week,
  o.opportunity_id,
  o.created_date        AS opp_created_date,
  o.owner_segment,
  o.owner_region,
  pr.first_progression_date,
  CASE WHEN o.created_date > f.first_week
        AND o.created_date <= DATE_ADD(f.first_week, INTERVAL {window} DAY)
       THEN TRUE ELSE FALSE END AS new_opp_in_window,
  CASE WHEN pr.first_progression_date >= f.first_week
        AND pr.first_progression_date <= DATE_ADD(f.first_week, INTERVAL {window} DAY)
       THEN TRUE ELSE FALSE END AS stage_progressed_in_window
FROM featured f
LEFT JOIN opps o ON LOWER(TRIM(o.account_name)) = LOWER(TRIM(f.account_name))
LEFT JOIN progressions pr ON pr.opportunity_id = o.opportunity_id
"""
    return _run_bq(sql)


def query_control_with_opps(excluded_lower: set[str], since_date: str, window: int) -> list[dict]:
    """
    Control group for Analysis 1 (pipeline progression).
    All BoB-segment accounts WITH an existing opp, not in the featured list.
    """
    sql = f"""
WITH opps AS (
  SELECT o.opportunity_id, o.account_name, o.created_date, o.owner_segment, o.owner_region
  FROM `shopify-dw.mart_revenue_data.sales_opportunity_reporting_metrics` o
  WHERE o.owner_segment IN ({_seg_list()})
    AND o.account_name IS NOT NULL
    AND o.is_closed = FALSE  -- open opps only
),
progressions AS (
  SELECT p.opportunity_id, MIN(p.date) AS first_progression_date
  FROM `shopify-dw.mart_revenue_data.sales_opportunity_progression_daily` p
  WHERE p.has_stage_progressed_opportunity_id IS NOT NULL
    AND p.date >= '{since_date}'
    AND p.date <= DATE_ADD(DATE '{since_date}', INTERVAL {window} DAY)
  GROUP BY 1
)
SELECT
  o.account_name,
  CAST(NULL AS DATE)  AS first_week,
  o.opportunity_id,
  o.created_date      AS opp_created_date,
  o.owner_segment,
  o.owner_region,
  pr.first_progression_date,
  CASE WHEN o.created_date >= '{since_date}'
        AND o.created_date <= DATE_ADD(DATE '{since_date}', INTERVAL {window} DAY)
       THEN TRUE ELSE FALSE END AS new_opp_in_window,
  CASE WHEN pr.first_progression_date IS NOT NULL
       THEN TRUE ELSE FALSE END AS stage_progressed_in_window
FROM opps o
LEFT JOIN progressions pr ON pr.opportunity_id = o.opportunity_id
"""
    raw = _run_bq(sql)
    return [r for r in raw if (r.get("account_name") or "").lower() not in excluded_lower]


def query_control_prospects(excluded_lower: set[str], since_date: str, window: int) -> list[dict]:
    """
    Control group for Analysis 2 (new deal creation).
    All BoB-segment prospect accounts in SFDC NOT in the featured list.
    Uses revenue_enriched_account_attributes for the universe,
    then checks sales_opportunity_reporting_metrics for new opp creation.
    """
    sql = f"""
WITH bob_accounts AS (
  -- All SFDC accounts in BoB segments that are Prospects (no active deal yet)
  -- Proxy: accounts in enriched attributes that have SF data
  SELECT DISTINCT a.account_name
  FROM `shopify-dw.mart_revenue_data.revenue_enriched_account_attributes` a
  WHERE a.has_sf_data = TRUE
    AND a.account_name IS NOT NULL
    AND a.lifecycle_status IN ('Prospect', 'Customer')
),
new_opps AS (
  SELECT o.account_name,
    CASE WHEN o.created_date >= '{since_date}'
          AND o.created_date <= DATE_ADD(DATE '{since_date}', INTERVAL {window} DAY)
         THEN TRUE ELSE FALSE END AS new_opp_in_window,
    o.owner_segment
  FROM `shopify-dw.mart_revenue_data.sales_opportunity_reporting_metrics` o
  WHERE o.owner_segment IN ({_seg_list()})
    AND o.created_date >= '{since_date}'
    AND o.account_name IS NOT NULL
)
SELECT
  b.account_name,
  CAST(NULL AS DATE)   AS first_week,
  CAST(NULL AS STRING) AS opportunity_id,
  CAST(NULL AS DATE)   AS opp_created_date,
  CAST(NULL AS STRING) AS owner_segment,
  CAST(NULL AS STRING) AS owner_region,
  CAST(NULL AS DATE)   AS first_progression_date,
  CASE WHEN n.account_name IS NOT NULL THEN TRUE ELSE FALSE END AS new_opp_in_window,
  FALSE AS stage_progressed_in_window
FROM bob_accounts b
LEFT JOIN new_opps n ON LOWER(TRIM(n.account_name)) = LOWER(TRIM(b.account_name))
"""
    raw = _run_bq(sql)
    return [r for r in raw if (r.get("account_name") or "").lower() not in excluded_lower]


# -------------------------------------------------------------------------
# Analysis helpers
# -------------------------------------------------------------------------

def compute_rates(rows: list[dict], label: str = "") -> dict:
    """Aggregate opp rows to account-level, compute group rates."""
    by_account: dict[str, dict] = {}
    for row in rows:
        name = (row.get("account_name") or "").strip()
        if not name:
            continue
        e = by_account.setdefault(name, {
            "account": name, "has_opp": False,
            "new_opp_in_window": False, "stage_progressed": False,
        })
        if row.get("opportunity_id"):
            e["has_opp"] = True
        if row.get("new_opp_in_window") in (True, "true", "True"):
            e["new_opp_in_window"] = True
        if row.get("stage_progressed_in_window") in (True, "true", "True"):
            e["stage_progressed"] = True

    accounts = list(by_account.values())
    total = len(accounts)
    if total == 0:
        return {"group": label, "total_accounts": 0, "new_opp_count": 0,
                "new_opp_rate": 0.0, "progressed_count": 0, "stage_progression_rate": 0.0}

    new_opp    = sum(1 for a in accounts if a["new_opp_in_window"])
    progressed = sum(1 for a in accounts if a["stage_progressed"])
    return {
        "group":                  label,
        "total_accounts":         total,
        "new_opp_count":          new_opp,
        "new_opp_rate":           round(new_opp    / total * 100, 1),
        "progressed_count":       progressed,
        "stage_progression_rate": round(progressed / total * 100, 1),
    }


def write_csv(rows: list[dict], path: Path, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sales Insights Hub — pipeline impact analysis")
    parser.add_argument("--window", type=int, default=90,
                        help="Days after first feature to measure (default: 90)")
    parser.add_argument("--since", default=None,
                        help="Only include log rows on/after this date (YYYY-MM-DD)")
    args = parser.parse_args()

    print("Sales Insights Hub — Impact Analysis")
    print(f"  Log:    {_LOG_PATH}")
    print(f"  Window: {args.window} days")
    if args.since:
        print(f"  Since:  {args.since}")
    print()

    # Load log
    log_rows = load_log(_LOG_PATH, since=args.since)
    if not log_rows:
        print("ERROR: No rows in featured accounts log.")
        return
    print(f"  {len(log_rows)} log rows loaded")

    first = first_featured_per_account(log_rows)
    featured_names      = set(first.keys())
    excluded_lower      = {n.lower() for n in featured_names}
    since_date = args.since or min(r["week"] for r in log_rows)

    print(f"  {len(featured_names)} unique featured accounts")
    print(f"  Measurement window starts: {since_date}")
    print()

    # ------------------------------------------------------------------
    # Fetch opp data for all featured accounts (chunked)
    # ------------------------------------------------------------------
    print("Querying BigQuery: featured accounts (opp + progression data)...")
    account_list  = list(first.keys())
    CHUNK         = 300
    featured_rows = []
    for i in range(0, len(account_list), CHUNK):
        chunk = account_list[i : i + CHUNK]
        batch = query_featured_opp_data(chunk, first, args.window)
        featured_rows.extend(batch)
        print(f"  Chunk {i // CHUNK + 1}/{(len(account_list) - 1) // CHUNK + 1}: "
              f"{len(batch)} rows")
    print(f"  Total: {len(featured_rows)} rows\n")

    # Split featured into: has_opp (Analysis 1) vs no_opp (Analysis 2)
    featured_with_opp = [r for r in featured_rows if r.get("opportunity_id")]
    featured_no_opp   = [r for r in featured_rows if not r.get("opportunity_id")]
    featured_opp_accounts  = {(r.get("account_name") or "").lower()
                               for r in featured_with_opp}

    print(f"  Featured WITH existing opp:    "
          f"{len({r['account_name'] for r in featured_with_opp})} accounts")
    print(f"  Featured WITHOUT existing opp: "
          f"{len({r['account_name'] for r in featured_no_opp})} accounts")
    print()

    # ------------------------------------------------------------------
    # Analysis 1 — Pipeline Progression (featured w/opp vs control w/opp)
    # ------------------------------------------------------------------
    print("=" * 60)
    print("ANALYSIS 1: Pipeline Progression")
    print("  Do deals at featured accounts progress faster?")
    print("=" * 60)

    a1_featured_rates = compute_rates(featured_with_opp, "featured_with_opp")
    print(f"\nFeatured (with opp): {a1_featured_rates['total_accounts']} accounts")
    print(f"  Stage progressed: {a1_featured_rates['progressed_count']} "
          f"({a1_featured_rates['stage_progression_rate']}%)")
    print(f"  New opp created:  {a1_featured_rates['new_opp_count']} "
          f"({a1_featured_rates['new_opp_rate']}%)")

    print("\nQuerying BigQuery: control group (BoB accounts with opps, not featured)...")
    ctrl_opp_rows = query_control_with_opps(excluded_lower, since_date, args.window)
    print(f"  {len(ctrl_opp_rows)} rows")

    a1_control_rates = compute_rates(ctrl_opp_rows, "control_with_opp")
    print(f"\nControl (with opp): {a1_control_rates['total_accounts']} accounts")
    print(f"  Stage progressed: {a1_control_rates['progressed_count']} "
          f"({a1_control_rates['stage_progression_rate']}%)")
    print(f"  New opp created:  {a1_control_rates['new_opp_count']} "
          f"({a1_control_rates['new_opp_rate']}%)")

    a1_progress_lift = round(a1_featured_rates["stage_progression_rate"]
                             - a1_control_rates["stage_progression_rate"], 1)
    a1_newopp_lift   = round(a1_featured_rates["new_opp_rate"]
                             - a1_control_rates["new_opp_rate"], 1)
    print(f"\nLift: progression {a1_progress_lift:+.1f}pp | new opp {a1_newopp_lift:+.1f}pp\n")

    # ------------------------------------------------------------------
    # Analysis 2 — New Deal Creation (all featured vs BoB prospects)
    # ------------------------------------------------------------------
    print("=" * 60)
    print("ANALYSIS 2: New Deal Creation")
    print("  Does hub exposure increase chance of a new deal?")
    print("=" * 60)

    a2_featured_rates = compute_rates(featured_rows, "featured_all")
    print(f"\nFeatured (all): {a2_featured_rates['total_accounts']} accounts")
    print(f"  New opp created: {a2_featured_rates['new_opp_count']} "
          f"({a2_featured_rates['new_opp_rate']}%)")

    print("\nQuerying BigQuery: control group (BoB prospect accounts, not featured)...")
    ctrl_prospect_rows = query_control_prospects(excluded_lower, since_date, args.window)
    print(f"  {len(ctrl_prospect_rows)} rows")

    a2_control_rates = compute_rates(ctrl_prospect_rows, "control_prospects")
    print(f"\nControl (BoB prospects): {a2_control_rates['total_accounts']} accounts")
    print(f"  New opp created: {a2_control_rates['new_opp_count']} "
          f"({a2_control_rates['new_opp_rate']}%)")

    a2_lift = round(a2_featured_rates["new_opp_rate"] - a2_control_rates["new_opp_rate"], 1)
    print(f"\nLift: new deal creation {a2_lift:+.1f}pp\n")

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    OPP_FIELDS = ["group", "account_name", "first_week", "opportunity_id",
                  "opp_created_date", "owner_segment", "owner_region",
                  "first_progression_date", "new_opp_in_window",
                  "stage_progressed_in_window"]
    SUMMARY_FIELDS = ["analysis", "group", "total_accounts",
                      "new_opp_count", "new_opp_rate",
                      "progressed_count", "stage_progression_rate"]

    # Analysis 1 results
    a1_rows = []
    for r in featured_with_opp:
        a1_rows.append({"group": "featured", **r})
    for r in ctrl_opp_rows:
        a1_rows.append({"group": "control",  **r})
    write_csv(a1_rows, _PROGRESSION_PATH, OPP_FIELDS)

    # Analysis 2 results
    a2_rows = []
    for r in featured_rows:
        a2_rows.append({"group": "featured", **r})
    for r in ctrl_prospect_rows:
        a2_rows.append({"group": "control",  **r})
    write_csv(a2_rows, _NEWDEAL_PATH, OPP_FIELDS)

    # Summary
    summary_rows = [
        {"analysis": "1_progression", **a1_featured_rates},
        {"analysis": "1_progression", **a1_control_rates},
        {"analysis": "2_new_deal",    **a2_featured_rates},
        {"analysis": "2_new_deal",    **a2_control_rates},
    ]
    write_csv(summary_rows, _SUMMARY_PATH, SUMMARY_FIELDS)

    print("Written:")
    print(f"  {_SUMMARY_PATH}")
    print(f"  {_PROGRESSION_PATH}")
    print(f"  {_NEWDEAL_PATH}")
    print("\nDone.")


if __name__ == "__main__":
    main()
