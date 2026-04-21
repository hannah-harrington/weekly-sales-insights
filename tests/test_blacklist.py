"""
Test 2: Blacklist filter.

Blacklisted accounts must never appear in any rep's signals or raw_signals.
This is a safety guarantee — if it breaks, reps see noise that wastes their time.
"""
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.sources.demandbase import load


# Real Demandbase NoSalesTouches column headers
_MQA_ROW_TEMPLATE = {
    "Account Name": "",
    "Website": "",
    "Owner Name": "",
    "Account Grade": "A",
    "Journey Stage": "MQA",
    "Engagement Points (3 mo.)": "1200",
    "High Intent Keywords": "",
    "Top Account Categories": "",
    "Priority Summary": "",
    "Territory Name": "",
    "Id": "123",
}


def _write_csv(path: Path, rows: list[dict]):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_blacklist(pipeline_dir: Path, accounts: list[str]) -> Path:
    bl_path = pipeline_dir / "blacklist.json"
    bl_path.write_text(json.dumps({
        "_comment": "test blacklist",
        "_total": len(accounts),
        "accounts": accounts,
    }))
    return bl_path


def test_blacklisted_account_never_appears(tmp_path, monkeypatch):
    """An account on the blacklist must not appear in any output signal."""
    # Patch the blacklist path to point at our temp one
    import pipeline.sources.demandbase as db_module

    fake_bl_path = tmp_path / "pipeline" / "blacklist.json"
    fake_bl_path.parent.mkdir(parents=True)
    fake_bl_path.write_text(json.dumps({
        "_total": 1,
        "accounts": ["nike"],  # lowercase
    }))

    # Monkeypatch _load_blacklist to return our test set
    monkeypatch.setattr(db_module, "_load_blacklist", lambda: {"nike"})

    rows = [
        {**_MQA_ROW_TEMPLATE, "Account Name": "Nike", "Website": "nike.com", "Owner Name": "Colin Behenna"},
        {**_MQA_ROW_TEMPLATE, "Account Name": "Acme Commerce Co", "Website": "acme.com", "Owner Name": "Colin Behenna"},
    ]
    _write_csv(tmp_path / "NewAccountsMovedToMqaInLastWeekWNoSalesTouches.csv", rows)

    result = load(tmp_path)

    all_accounts = [
        row.get("account", "")
        for seller_signals in result["signals_by_seller"].values()
        for signal_rows in seller_signals.values()
        for row in signal_rows
    ]

    assert "Nike" not in all_accounts, "Nike is blacklisted — must not appear in signals"
    assert "Acme Commerce Co" in all_accounts, "Non-blacklisted account should still appear"


def test_blacklist_is_case_insensitive(tmp_path, monkeypatch):
    """Blacklist matching is case-insensitive: 'NIKE', 'nike', 'Nike' all blocked."""
    import pipeline.sources.demandbase as db_module
    monkeypatch.setattr(db_module, "_load_blacklist", lambda: {"big brand corp"})

    rows = [
        {**_MQA_ROW_TEMPLATE, "Account Name": "BIG BRAND CORP", "Website": "bigbrand.com", "Owner Name": "Erin Choi"},
    ]
    _write_csv(tmp_path / "NewAccountsMovedToMqaInLastWeekWNoSalesTouches.csv", rows)

    result = load(tmp_path)

    all_accounts = [
        row.get("account", "")
        for seller_signals in result["signals_by_seller"].values()
        for signal_rows in seller_signals.values()
        for row in signal_rows
    ]

    assert "BIG BRAND CORP" not in all_accounts, (
        "Blacklist should match case-insensitively — 'BIG BRAND CORP' should be blocked"
    )
