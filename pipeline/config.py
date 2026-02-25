"""
Seller directory, team mapping, identity roles, and pipeline settings.

This is the single source of truth for who the sellers are,
which teams they belong to, and where data flows.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_DIR = PROJECT_ROOT / "pipeline"
SITE_DIR = PROJECT_ROOT / "site"
DATA_DIR = SITE_DIR / "data"
ARCHIVE_DIR = PROJECT_ROOT / "archive"

# Where CSVs are dropped (the existing workflow folder)
CSV_INPUT_DIR = PROJECT_ROOT.parent / "Demandbase weeklys"

# Quick deployment
DEPLOY_SITE_NAME = "weekly-sales-insights"

# ---------------------------------------------------------------------------
# Identity roles
#
# Three identity types, resolved via GCP IAP email:
#   ADMIN  → always sees master dashboard
#   COACH  → sees coach view (Phase 5); master dashboard until then
#   SELLER → sees their own personal dashboard
# ---------------------------------------------------------------------------

ROLE_ADMIN = "admin"
ROLE_COACH = "coach"
ROLE_SELLER = "seller"

ADMINS: list[str] = [
    "hannah.harrington@shopify.com",
]

# Coaches: email -> dict with name and list of seller IDs they manage.
# The coach view (Phase 5) will show a rollup of these reps' signals.
COACHES: dict[str, dict] = {
    # "julie.tsai@shopify.com": {
    #     "name": "Julie Tsai",
    #     "reps": ["amanda_avedschmidt", "julien_baunay"],
    # },
}

# Explicit email overrides for reps whose email doesn't follow
# the firstname.lastname@shopify.com convention.
EMAIL_OVERRIDES: dict[str, str] = {
    # "Dev Admin": "dev.admin@shopify.com",
}

# ---------------------------------------------------------------------------
# Team mapping
#
# Move reps from ALL_KNOWN_REPS into the correct team lists below.
# Any rep not listed here will appear under "Unassigned" in reports.
# ---------------------------------------------------------------------------

TEAM_TO_REPS: dict[str, list[str]] = {
    # "Team Sam": [
    #     "Amanda Avedschmidt",
    #     "Julien Baunay",
    # ],
}

TEAM_ORDER = list(TEAM_TO_REPS.keys())

# ---------------------------------------------------------------------------
# Seller directory
# ---------------------------------------------------------------------------

ALL_KNOWN_REPS = [
    "Aaron Holl",
    "Alden Morse",
    "Alexandra Seigenberg",
    "Alexandre Saroian",
    "Anakaren Olivo",
    "Anastasia Sfregola",
    "Anthony Anastasi",
    "Capucine Delval",
    "Cassie Steinberg",
    "Chris Andreoli",
    "Colin Behenna",
    "Colton Powell",
    "Daisy Wright",
    "Danielle Salvatore",
    "Dev Admin",
    "Erin Choi",
    "Fiona Taurel",
    "Gavin Spencer",
    "Ivka Shepard",
    "John Beringer",
    "Kristin Sutton",
    "Kristy Shimkus",
    "Madeline Michelson",
    "Melanie Wollnitza",
    "Morgan Moran de Sanchez",
    "Nick Essling",
    "Nick Herrera",
    "Nicolas Berg",
    "Nicole Smelzer",
    "Rebecca Pallister",
    "Ryan Kernus",
    "Samantha Schultz",
    "Scott Cohen",
    "Simon Bennett",
    "Tanner Andresen",
]


def _normalize_id(name: str) -> str:
    """Convert a display name to a stable seller ID."""
    return name.lower().replace(" ", "_").replace("/", "_")


def derive_email(name: str) -> str:
    """Derive a Shopify email from a display name.

    Uses firstname.lastname@shopify.com convention.
    Returns empty string for names that can't be derived (e.g. "Dev Admin").
    """
    if name in EMAIL_OVERRIDES:
        return EMAIL_OVERRIDES[name]
    parts = name.strip().split()
    if len(parts) < 2:
        return ""
    first = parts[0].lower()
    last = parts[-1].lower()
    return f"{first}.{last}@shopify.com"


# Build reverse lookups
REP_TO_TEAM: dict[str, str] = {}
for _team, _reps in TEAM_TO_REPS.items():
    for _rep in _reps:
        REP_TO_TEAM[_rep] = _team

# name -> seller_id lookup (case-insensitive)
_NAME_TO_ID: dict[str, str] = {}
for _name in ALL_KNOWN_REPS:
    _NAME_TO_ID[_name.lower()] = _normalize_id(_name)


def seller_id_for(name: str) -> str:
    """Get or create a seller ID from a display name."""
    key = name.strip().lower()
    if key in _NAME_TO_ID:
        return _NAME_TO_ID[key]
    new_id = _normalize_id(name.strip())
    _NAME_TO_ID[key] = new_id
    return new_id


def team_for(name: str) -> str | None:
    """Get the team name for a rep, or None if unassigned."""
    return REP_TO_TEAM.get(name.strip())


def seller_record(name: str) -> dict:
    """Build a seller record dict for a given rep name."""
    name = name.strip()
    return {
        "name": name,
        "email": derive_email(name),
        "team": team_for(name),
        "segment": "Enterprise",
    }


def build_identity_map() -> dict:
    """Build the identity section for the JSON data file.

    Returns a dict with admins, coaches, and seller_emails
    that the SPA uses to resolve IAP email -> role + view.
    """
    seller_emails: dict[str, str] = {}
    for name in ALL_KNOWN_REPS:
        email = derive_email(name)
        if email:
            seller_emails[email] = _normalize_id(name)

    return {
        "admins": list(ADMINS),
        "coaches": {
            email: {
                "name": info["name"],
                "reps": list(info["reps"]),
            }
            for email, info in COACHES.items()
        },
        "seller_emails": seller_emails,
    }
