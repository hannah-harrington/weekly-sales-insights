"""
Test 1: Signal routing.

Synthetic Demandbase CSV → confirm a known account ends up in the correct
rep's signals. This is the core of what the pipeline does. If this breaks,
nothing else matters.
"""
import csv
import io
import sys
from pathlib import Path

# Make sure the pipeline package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.sources.demandbase import load, CSV_TYPES


def _make_csv_dir(tmp_path: Path, filename: str, rows: list[dict]) -> Path:
    """Write a minimal CSV file into tmp_path and return the directory."""
    path = tmp_path / filename
    if not rows:
        return tmp_path
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return tmp_path


# Minimal row matching the real Demandbase NoSalesTouches CSV column headers
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


def test_mqa_account_routes_to_correct_rep(tmp_path):
    """A New MQA account owned by a known rep appears in that rep's signals."""
    rows = [
        {**_MQA_ROW_TEMPLATE, "Account Name": "Acme Commerce Co", "Website": "acme.com", "Owner Name": "Colin Behenna"},
    ]
    _make_csv_dir(tmp_path, "NewAccountsMovedToMqaInLastWeekWNoSalesTouches.csv", rows)

    result = load(tmp_path)

    # The account should appear somewhere in signals_by_seller
    all_accounts = [
        row.get("account", "")
        for seller_signals in result["signals_by_seller"].values()
        for signal_rows in seller_signals.values()
        for row in signal_rows
    ]
    assert "Acme Commerce Co" in all_accounts, (
        f"Expected 'Acme Commerce Co' in signals but got: {all_accounts[:5]}"
    )

    # Specifically it should be under Colin Behenna
    colin_signals = result["signals_by_seller"].get("Colin Behenna", {})
    colin_accounts = [
        row.get("account", "")
        for rows in colin_signals.values()
        for row in rows
    ]
    assert "Acme Commerce Co" in colin_accounts, (
        f"Expected 'Acme Commerce Co' in Colin Behenna's signals, got: {colin_accounts}"
    )


def test_unknown_rep_still_gets_signals(tmp_path):
    """An account owned by a rep not in ALL_KNOWN_REPS still appears in signals_by_seller."""
    rows = [
        {**_MQA_ROW_TEMPLATE, "Account Name": "Mystery Brand Inc", "Website": "mystery.com", "Owner Name": "Some New Rep"},
    ]
    _make_csv_dir(tmp_path, "NewAccountsMovedToMqaInLastWeekWNoSalesTouches.csv", rows)

    result = load(tmp_path)

    all_accounts = [
        row.get("account", "")
        for seller_signals in result["signals_by_seller"].values()
        for signal_rows in seller_signals.values()
        for row in signal_rows
    ]
    assert "Mystery Brand Inc" in all_accounts, (
        "Account with unknown rep owner should still appear in signals_by_seller"
    )
