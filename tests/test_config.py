"""
Test 3: Config integrity.

Every rep in ALL_KNOWN_REPS must have a valid derived email.
Every rep in TEAM_TO_REPS must also be in ALL_KNOWN_REPS.
These tests catch the most common config mistakes: adding a rep to the team
list but forgetting ALL_KNOWN_REPS, or a name with unusual spacing that breaks
email derivation.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.config import ALL_KNOWN_REPS, TEAM_TO_REPS, derive_email, EMAIL_OVERRIDES


def test_all_reps_have_valid_email():
    """Every rep in ALL_KNOWN_REPS should produce a non-empty, valid-looking email."""
    failures = []
    for name in ALL_KNOWN_REPS:
        if name == "Dev Admin":
            continue  # explicitly excluded from email derivation
        email = derive_email(name)
        if not email:
            failures.append(f"  {name!r} → empty email")
        elif "@shopify.com" not in email:
            # Allow email overrides that use different domains
            if name not in EMAIL_OVERRIDES:
                failures.append(f"  {name!r} → {email!r} (missing @shopify.com)")

    assert not failures, "Reps with invalid emails:\n" + "\n".join(failures)


def test_team_reps_are_in_known_reps():
    """Every rep listed in TEAM_TO_REPS must also appear in ALL_KNOWN_REPS."""
    known = set(ALL_KNOWN_REPS)
    missing = []
    for team, reps in TEAM_TO_REPS.items():
        for rep in reps:
            if rep not in known:
                missing.append(f"  {rep!r} (team: {team}) not in ALL_KNOWN_REPS")

    assert not missing, (
        "Reps in TEAM_TO_REPS but missing from ALL_KNOWN_REPS:\n" + "\n".join(missing)
    )


def test_no_duplicate_reps_across_teams():
    """No rep should appear in more than one team."""
    seen = {}
    duplicates = []
    for team, reps in TEAM_TO_REPS.items():
        for rep in reps:
            if rep in seen:
                duplicates.append(f"  {rep!r} appears in both {seen[rep]!r} and {team!r}")
            else:
                seen[rep] = team

    assert not duplicates, "Reps assigned to multiple teams:\n" + "\n".join(duplicates)


def test_email_overrides_are_valid():
    """All EMAIL_OVERRIDES values must look like valid email addresses."""
    failures = []
    for name, email in EMAIL_OVERRIDES.items():
        if "@" not in email or "." not in email.split("@")[-1]:
            failures.append(f"  {name!r} → {email!r} (invalid format)")

    assert not failures, "Invalid email overrides:\n" + "\n".join(failures)
