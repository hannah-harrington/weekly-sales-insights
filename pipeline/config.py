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

# Sales Navigator leads (Consumer team only, static enrichment)
SALES_NAV_LEADS_FILE = PROJECT_ROOT.parent / "CG_Sales_Nav_Leads_CLEAN.xlsx"

# Quick deployment
DEPLOY_SITE_NAME = "sales-insights-hub"
SITE_URL = f"https://{DEPLOY_SITE_NAME}.quick.shopify.io"

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

# Coaches: email -> dict with name, team(s), and list of seller IDs they manage.
# The coach view (Phase 5) will show a rollup of these reps' signals.
COACHES: dict[str, dict] = {
    "ryan.quarles@shopify.com": {
        "name": "Ryan Quarles",
        "teams": ["Consumer"],
        "reps": [
            "colin_behenna", "ivka_shepard", "morgan_moran_de_sanchez",
            "daisy_wright", "ryan_kernus", "erin_choi",
        ],
    },
    "dave.greenberger@shopify.com": {
        "name": "Dave Greenberger",
        "teams": ["Emerging"],
        "reps": [
            "rebecca_pallister", "alden_morse", "nick_essling",
            "kristy_shimkus", "tanner_andresen", "anthony_anastasi",
        ],
    },
    "todd.mallett@shopify.com": {
        "name": "Todd Mallett",
        "teams": ["Lifestyle 1"],
        "reps": [
            "alexandra_seigenberg", "zachary_alton", "kristin_sutton",
            "gregg_belbeck", "kelsey_bates", "scott_cohen",
        ],
    },
    "kal.stephen@shopify.com": {
        "name": "Kal Stephen",
        "teams": ["Lifestyle 2"],
        "reps": [
            "aaron_holl", "cassie_steinberg", "sheeva_sairafi",
            "vicki_bodwell", "nick_herrera", "samantha_schultz",
        ],
    },
    "brandon.gracey@shopify.com": {
        "name": "Brandon Gracey",
        "teams": ["Global Accounts", "EMEA"],
        "reps": [
            "vanessa_buttinger", "christopher_joannou", "nicole_smelzer",
            "john_beringer", "nicolas_berg", "melanie_wollnitza",
            "simon_bennett", "danielle_salvatore", "anastasia_sfregola",
            "capucine_delval", "nathan_frost", "fiona_taurel",
        ],
    },
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
    "Consumer": [
        "Colin Behenna",
        "Erin Choi",
        "Ivka Shepard",
        "Morgan Moran de Sanchez",
        "Daisy Wright",
        "Ryan Kernus",
    ],
    "Emerging": [
        "Rebecca Pallister",
        "Alden Morse",
        "Nick Essling",
        "Kristy Shimkus",
        "Tanner Andresen",
        "Anthony Anastasi",
    ],
    "Lifestyle 1": [
        "Alexandra Seigenberg",
        "Zachary Alton",
        "Kristin Sutton",
        "Gregg Belbeck",
        "Kelsey Bates",
        "Scott Cohen",
    ],
    "Lifestyle 2": [
        "Aaron Holl",
        "Cassie Steinberg",
        "Sheeva Sairafi",
        "Vicki Bodwell",
        "Nick Herrera",
        "Samantha Schultz",
    ],
    "Global Accounts": [
        "Vanessa Buttinger",
        "Christopher Joannou",
        "Nicole Smelzer",
        "John Beringer",
    ],
    "EMEA": [
        "Nicolas Berg",
        "Melanie Wollnitza",
        "Simon Bennett",
        "Danielle Salvatore",
        "Anastasia Sfregola",
        "Capucine Delval",
        "Nathan Frost",
        "Fiona Taurel",
    ],
}

TEAM_LEADS: dict[str, str] = {
    "Consumer": "Ryan Quarles",
    "Emerging": "Dave Greenberger",
    "Lifestyle 1": "Todd Mallett",
    "Lifestyle 2": "Kal Stephen",
    "Global Accounts": "Brandon Gracey",
    "EMEA": "Brandon Gracey",
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
    "Christopher Joannou",
    "Colin Behenna",
    "Colton Powell",
    "Daisy Wright",
    "Danielle Salvatore",
    "Dev Admin",
    "Erin Choi",
    "Fiona Taurel",
    "Gavin Spencer",
    "Gregg Belbeck",
    "Ivka Shepard",
    "John Beringer",
    "Kelsey Bates",
    "Kristin Sutton",
    "Kristy Shimkus",
    "Madeline Michelson",
    "Melanie Wollnitza",
    "Morgan Moran de Sanchez",
    "Nathan Frost",
    "Nick Essling",
    "Nick Herrera",
    "Nicolas Berg",
    "Nicole Smelzer",
    "Rebecca Pallister",
    "Ryan Kernus",
    "Samantha Schultz",
    "Scott Cohen",
    "Sheeva Sairafi",
    "Simon Bennett",
    "Tanner Andresen",
    "Vanessa Buttinger",
    "Vicki Bodwell",
    "Zachary Alton",
]


# ---------------------------------------------------------------------------
# Territory codes (from Enterprise Reps spreadsheet, for future use)
# ---------------------------------------------------------------------------

TERRITORY_MAP: dict[str, list[str]] = {
    "Colin Behenna": ["AMER_Enterprise_All_Consumer_A_All_01", "AMER_Enterprise_All_Consumer_X_All_01"],
    "Ivka Shepard": ["AMER_Enterprise_All_Consumer_A_All_02", "AMER_Enterprise_All_Consumer_X_All_02"],
    "Morgan Moran de Sanchez": ["AMER_Enterprise_All_Consumer_A_All_04", "AMER_Enterprise_All_Consumer_X_All_04"],
    "Daisy Wright": ["AMER_Enterprise_All_Consumer_A_All_05", "AMER_Enterprise_All_Consumer_X_All_05"],
    "Ryan Kernus": ["AMER_Enterprise_All_Consumer_A_All_06", "AMER_Enterprise_All_Consumer_X_All_06"],
    "Erin Choi": ["AMER_Enterprise_All_Consumer_A_All_03"],
    "Rebecca Pallister": ["AMER_Enterprise_All_Emerging_A_All_01", "AMER_Enterprise_All_Emerging_X_All_01"],
    "Alden Morse": ["AMER_Enterprise_All_Emerging_A_All_02", "AMER_Enterprise_All_Emerging_X_All_02"],
    "Nick Essling": ["AMER_Enterprise_All_Emerging_A_All_03", "AMER_Enterprise_All_Emerging_X_All_03"],
    "Kristy Shimkus": ["AMER_Enterprise_All_Emerging_A_All_05", "AMER_Enterprise_All_Emerging_X_All_05"],
    "Tanner Andresen": ["AMER_Enterprise_All_Emerging_A_All_06", "AMER_Enterprise_All_Emerging_X_All_06"],
    "Alexandra Seigenberg": ["AMER_Enterprise_All_Lifestyle_A_All_06", "AMER_Enterprise_All_Lifestyle_X_All_06"],
    "Zachary Alton": ["AMER_Enterprise_All_Lifestyle_A_All_08", "AMER_Enterprise_All_Lifestyle_X_All_08"],
    "Kristin Sutton": ["AMER_Enterprise_All_Lifestyle_A_All_09"],
    "Gregg Belbeck": ["AMER_Enterprise_All_Lifestyle_A_All_10", "AMER_Enterprise_All_Lifestyle_X_All_10"],
    "Kelsey Bates": ["AMER_Enterprise_All_Lifestyle_A_All_11", "AMER_Enterprise_All_Lifestyle_X_All_11"],
    "Scott Cohen": ["AMER_Enterprise_All_Lifestyle_A_All_12", "AMER_Enterprise_All_Lifestyle_X_All_12"],
    "Aaron Holl": ["AMER_Enterprise_All_Lifestyle_A_All_01", "AMER_Enterprise_All_Lifestyle_X_All_01"],
    "Cassie Steinberg": ["AMER_Enterprise_All_Lifestyle_A_All_02", "AMER_Enterprise_All_Lifestyle_X_All_02"],
    "Sheeva Sairafi": ["AMER_Enterprise_All_Lifestyle_A_All_03", "AMER_Enterprise_All_Lifestyle_X_All_03"],
    "Vicki Bodwell": ["AMER_Enterprise_All_Lifestyle_A_All_04", "AMER_Enterprise_All_Lifestyle_X_All_04"],
    "Nick Herrera": ["AMER_Enterprise_All_Lifestyle_A_All_05", "AMER_Enterprise_All_Lifestyle_X_All_05"],
    "Samantha Schultz": ["AMER_Enterprise_All_Lifestyle_A_All_07", "AMER_Enterprise_All_Lifestyle_X_All_07"],
    "Vanessa Buttinger": ["AMER_GA_All_All_A_All_01", "AMER_GA_All_All_X_All_01"],
    "Christopher Joannou": ["AMER_GA_All_All_A_All_02", "AMER_GA_All_All_X_All_02"],
    "Nicole Smelzer": ["AMER_GA_All_All_A_All_04", "AMER_GA_All_All_X_All_04"],
    "John Beringer": ["AMER_GA_All_All_A_All_05", "AMER_GA_All_All_X_All_05"],
    "Nicolas Berg": ["EMEA_Enterprise_DACH_All_A_All_01", "EMEA_Enterprise_DACH_All_X_All_01"],
    "Melanie Wollnitza": ["EMEA_Enterprise_DACH_All_A_All_02", "EMEA_Enterprise_DACH_All_X_All_02"],
    "Simon Bennett": ["EMEA_Enterprise_North_All_A_All_02", "EMEA_Enterprise_North_All_X_All_02"],
    "Danielle Salvatore": ["EMEA_Enterprise_North_All_A_All_03", "EMEA_Enterprise_North_All_X_All_03"],
    "Anastasia Sfregola": ["EMEA_Enterprise_South_All_A_All_01", "EMEA_Enterprise_South_All_X_All_01"],
    "Capucine Delval": ["EMEA_GA_All_All_A_All_02", "EMEA_GA_All_All_X_All_02"],
    "Nathan Frost": ["EMEA_GA_All_All_A_All_04", "EMEA_GA_All_All_X_All_04"],
    "Fiona Taurel": ["EMEA_GA_All_All_A_All_05", "EMEA_GA_All_All_X_All_05"],
}


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
                "teams": list(info["teams"]),
                "reps": list(info["reps"]),
            }
            for email, info in COACHES.items()
        },
        "seller_emails": seller_emails,
    }
