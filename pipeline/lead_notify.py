"""
Slack DM notifications for team leads — Weekly Sales Insights.

Sends each team lead a personalised summary of their team's signals for the week.
Brandon Gracey gets a rolled-up all-teams summary.

Reuses the same token resolution and API call logic as slack_notify.py.
"""

import json
import pathlib
import urllib.error
import urllib.request

from pipeline.config import TEAM_TO_REPS, TEAM_LEADS

SITE_URL = "https://sales-insights-hub.quick.shopify.io"

# Lead Slack IDs — static map (supplement to slack_user_map.json)
_LEAD_SLACK_IDS: dict[str, str] = {
    "ryan.quarles@shopify.com":    "U07SM7SNY22",
    "dave.greenberger@shopify.com": "U08S278T0UR",
    "todd.mallett@shopify.com":    "U04RAAETJG7",
    "kal.stephen@shopify.com":     "U06AY6D14SK",
    "thom.armstrong@shopify.com":  "U0A9XKK5GD7",
    "daniel.glock@shopify.com":    "U02D2E6V7EK",
    "brandon.gracey@shopify.com":  "U04GVTMHENB",
    "james.johnson@shopify.com":   "U00000000",   # ANZ — skipped by default
}

# How each team is referred to in lead messages
_TEAM_DISPLAY_NAMES: dict[str, str] = {
    "Consumer":       "Consumer",
    "Emerging":       "Specialized",   # Dave calls his team Specialized
    "Lifestyle 1":    "your pod",
    "Lifestyle 2":    "your pod",
    "Global Accounts": "Global Accounts",
    "EMEA":           "EMEA",
    "ANZ":            "ANZ",
}

# Lead emails per team
_TEAM_LEAD_EMAILS: dict[str, str] = {
    "Consumer":       "ryan.quarles@shopify.com",
    "Emerging":       "dave.greenberger@shopify.com",
    "Lifestyle 1":    "todd.mallett@shopify.com",
    "Lifestyle 2":    "kal.stephen@shopify.com",
    "Global Accounts": "thom.armstrong@shopify.com",
    "EMEA":           "daniel.glock@shopify.com",
}

_BRANDON_EMAIL = "brandon.gracey@shopify.com"


def _get_cookie() -> str:
    """Read the xoxd cookie from callm credentials — needed for personal tokens."""
    creds_path = pathlib.Path.home() / ".config" / "callm" / "credentials.json"
    if creds_path.exists():
        try:
            creds = json.loads(creds_path.read_text())
            return creds.get("cookies", "")
        except (json.JSONDecodeError, KeyError):
            pass
    return ""


def _api_call(method: str, token: str, params: dict | None = None) -> dict:
    url = f"https://slack.com/api/{method}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    # Personal tokens (xoxc-) require the session cookie alongside the token
    if token.startswith("xoxc-"):
        cookie = _get_cookie()
        if cookie:
            headers["Cookie"] = cookie
    body = json.dumps(params or {}).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": str(e)}


def _open_dm(user_id: str, token: str) -> str | None:
    """Open or retrieve a DM channel with a user. Returns channel ID."""
    result = _api_call("conversations.open", token, {"users": user_id})
    if result.get("ok"):
        return result["channel"]["id"]
    return None


def _send_dm(user_id: str, message: str, token: str) -> bool:
    channel = _open_dm(user_id, token)
    if not channel:
        return False
    result = _api_call("chat.postMessage", token, {"channel": channel, "text": message})
    return result.get("ok", False)


def _team_stats(team: str, data: dict) -> dict:
    """Compute signal counts for a team from the JSON data model."""
    reps = TEAM_TO_REPS.get(team, [])
    stats = {
        "mqa_new": [],      # list of (rep_first_name, account, platform)
        "hvp": 0,
        "hvp_all": 0,
        "activity_accounts": set(),
        "agentic": set(),
        "compete": set(),
        "international": set(),
        "marketing": set(),
        "b2b": set(),
        "reps_with_signals": 0,
    }

    sellers = data.get("sellers", {})
    for rep_name in reps:
        rep_id = rep_name.lower().replace(" ", "_").replace("/", "_")
        seller = sellers.get(rep_id)
        if not seller:
            continue
        sigs = seller.get("signals", {})
        first = rep_name.split()[0]
        has_any = False

        for row in sigs.get("mqa_new", []):
            stats["mqa_new"].append((first, row["account"], row.get("platform", "")))
            has_any = True
        stats["hvp"] += len(sigs.get("hvp", []))
        stats["hvp_all"] += len(sigs.get("hvp_all", []))
        for row in sigs.get("activity", []):
            stats["activity_accounts"].add(row["account"])
        for row in sigs.get("intent_agentic", []):
            stats["agentic"].add(row["account"])
        for row in sigs.get("intent_compete", []):
            stats["compete"].add(row["account"])
        for row in sigs.get("intent_international", []):
            stats["international"].add(row["account"])
        for row in sigs.get("intent_marketing", []):
            stats["marketing"].add(row["account"])
        for row in sigs.get("intent_b2b", []):
            stats["b2b"].add(row["account"])

        if seller.get("summary", {}).get("total", 0) > 0:
            stats["reps_with_signals"] += 1

    return stats


def _format_team_message(team: str, stats: dict, week_of: str, first_dm_week: bool = False) -> str:
    """Build the Slack message for a team lead."""
    display = _TEAM_DISPLAY_NAMES.get(team, team)
    lead_first = {
        "Consumer": "Ryan", "Emerging": "Dave", "Lifestyle 1": "Todd",
        "Lifestyle 2": "Kal", "Global Accounts": "Thom", "EMEA": "Daniel",
    }.get(team, "there")

    intro = (
        f"Hey {lead_first} 👋\n\n"
        f"Your reps' Sales Insights reports are live for the week of {week_of}. "
        f"Each seller got a DM from me this morning with a direct link to their signals.\n\n"
    )

    lines = []

    # New MQA — always highlight if any
    if stats["mqa_new"]:
        count = len(stats["mqa_new"])
        accounts = ", ".join(
            f"{r[0]}: {r[1]}{' (' + r[2] + ')' if r[2] and r[2] not in ('', 'Other') else ''}"
            for r in stats["mqa_new"]
        )
        lines.append(f"🔥 *{count} new MQA account{'s' if count > 1 else ''}* — {accounts}")

    # HVP (lost opp re-engagement)
    if stats["hvp"] > 0:
        lines.append(f"*{stats['hvp']} account{'s' if stats['hvp'] > 1 else ''} back on high-value pages* — lost opps revisiting Shopify.com")

    # HVP all (visiting)
    if stats["hvp_all"] > 0:
        lines.append(f"*{stats['hvp_all']} accounts* visiting Shopify.com across {display}")

    # Intent signals
    if stats["agentic"]:
        lines.append(f"*{len(stats['agentic'])} accounts showing Agentic Commerce intent* (AI, LLMs, chatbots)")
    if stats["compete"]:
        lines.append(f"*{len(stats['compete'])} accounts showing Compete intent* — actively evaluating competitors")
    if stats["international"]:
        lines.append(f"*{len(stats['international'])} accounts showing International Commerce intent*")
    if stats["b2b"]:
        lines.append(f"*{len(stats['b2b'])} accounts showing B2B intent*")

    if not lines:
        signals_block = f"Quieter week for {display} — no new MQAs, but intent signals are worth your reps reviewing."
    else:
        signals_block = "\n".join(f"— {l}" for l in lines)

    outro = f"\n\nFull reports: {SITE_URL} — each rep's view is filtered to just their book.\n\n_Sent via Hannah's Pi_ 🤖"

    return intro + signals_block + outro


def _format_brandon_message(all_stats: dict[str, dict], week_of: str) -> str:
    """Build Brandon's rolled-up all-teams summary."""
    total_mqa = sum(len(s["mqa_new"]) for s in all_stats.values())
    total_hvp = sum(s["hvp"] for s in all_stats.values())
    total_agentic = len(set().union(*[s["agentic"] for s in all_stats.values()]))
    total_compete = len(set().union(*[s["compete"] for s in all_stats.values()]))

    # Find team with highest agentic concentration
    top_agentic_team = max(all_stats, key=lambda t: len(all_stats[t]["agentic"]), default=None)

    # MQA callouts per team
    mqa_lines = []
    for team, stats in all_stats.items():
        if stats["mqa_new"]:
            display = _TEAM_DISPLAY_NAMES.get(team, team)
            accounts = ", ".join(f"{r[0]}: {r[1]}" for r in stats["mqa_new"])
            mqa_lines.append(f"  • {display}: {accounts}")

    mqa_block = "\n".join(mqa_lines) if mqa_lines else "  • No new MQAs this week"

    agentic_note = ""
    if top_agentic_team:
        display = _TEAM_DISPLAY_NAMES.get(top_agentic_team, top_agentic_team)
        agentic_note = f" — {display} has the highest concentration"

    msg = f"""Hey Brandon 👋

Here's your weekly roll-up for the week of {week_of}. Each rep got a personalised DM this morning with a link to their signals.

Demandbase tracks real-time buying signals across our enterprise book — things like an account suddenly researching pricing pages, comparing us to competitors, or crossing an engagement threshold that tells us they're actively in-market. That timing matters. An account that's hot this week may not be next week. The goal: reps shouldn't have to dig through Demandbase data in a spreadsheet to find those signals. The signals come to them, by rep, every Monday — so they know exactly which accounts to prioritise and why.

*This week across all teams:*
— 🔥 *{total_mqa} new MQA account{'s' if total_mqa != 1 else ''}*
{mqa_block}
— *{total_hvp} accounts back on high-value pages* — lost opps revisiting Shopify.com
— *{total_agentic} accounts showing Agentic Commerce intent* (AI, LLMs, agentic commerce){agentic_note}
— *{total_compete} accounts showing Compete intent* — actively evaluating competitors

Reports are live at {SITE_URL} — you can browse by team or rep. Each seller's view is locked to their book.

_Sent via Hannah's Pi_ 🤖"""

    return msg


def notify_leads(data: dict, token: str, week_of: str) -> dict:
    """
    Send weekly summary DMs to all team leads and Brandon.

    Returns stats dict with sent/failed counts.
    """
    stats = {"sent": 0, "failed": 0, "skipped": 0, "details": []}

    all_team_stats = {}

    for team in TEAM_TO_REPS:
        if team == "ANZ":
            continue  # ANZ skipped — separate workflow
        team_stats = _team_stats(team, data)
        all_team_stats[team] = team_stats

        email = _TEAM_LEAD_EMAILS.get(team)
        if not email:
            continue

        user_id = _LEAD_SLACK_IDS.get(email)
        if not user_id or user_id == "U00000000":
            print(f"  [Leads] No Slack ID for {team} lead ({email}) — skipping")
            stats["skipped"] += 1
            stats["details"].append({"team": team, "status": "skipped", "reason": "no Slack ID"})
            continue

        message = _format_team_message(team, team_stats, week_of)
        ok = _send_dm(user_id, message, token)

        if ok:
            lead_name = TEAM_LEADS.get(team, team)
            print(f"  [Leads] ✓ {lead_name} ({team})")
            stats["sent"] += 1
            stats["details"].append({"team": team, "lead": lead_name, "status": "sent"})
        else:
            lead_name = TEAM_LEADS.get(team, team)
            print(f"  [Leads] ✗ {lead_name} ({team}) — send failed")
            stats["failed"] += 1
            stats["details"].append({"team": team, "lead": lead_name, "status": "failed"})

    # Brandon — all-teams rolled up
    brandon_id = _LEAD_SLACK_IDS.get(_BRANDON_EMAIL)
    if brandon_id and brandon_id != "U00000000":
        message = _format_brandon_message(all_team_stats, week_of)
        ok = _send_dm(brandon_id, message, token)
        if ok:
            print(f"  [Leads] ✓ Brandon Gracey (all teams)")
            stats["sent"] += 1
        else:
            print(f"  [Leads] ✗ Brandon Gracey — send failed")
            stats["failed"] += 1

    return stats
