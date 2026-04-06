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
        "mqa_new": [],          # list of (rep_first_name, account, platform)
        "hvp": 0,
        "hvp_all": 0,
        "activity_accounts": set(),
        "agentic": set(),
        "compete": set(),
        "international": set(),
        "marketing": set(),
        "b2b": set(),
        "g2_intent": [],        # list of (rep_first_name, account)
        "reps_with_signals": 0,
        "rep_breakdown": [],    # list of (first_name, total, mqa_count, hvp_count, compete_count)
        "top_compete": [],      # list of (rep_first, account, keywords, engagement) — top 3 by eng
    }

    sellers = data.get("sellers", {})
    all_compete_rows = []

    for rep_name in reps:
        rep_id = rep_name.lower().replace(" ", "_").replace("/", "_")
        seller = sellers.get(rep_id)
        if not seller:
            continue
        sigs = seller.get("signals", {})
        summary = seller.get("summary", {})
        first = rep_name.split()[0]

        mqa_count = len(sigs.get("mqa_new", []))
        hvp_count = len(sigs.get("hvp", []))
        compete_count = summary.get("intent_compete", 0)
        total = summary.get("total", 0)

        for row in sigs.get("mqa_new", []):
            stats["mqa_new"].append((first, row["account"], row.get("platform", "")))
        stats["hvp"] += hvp_count
        stats["hvp_all"] += len(sigs.get("hvp_all", []))
        for row in sigs.get("activity", []):
            stats["activity_accounts"].add(row["account"])
        for row in sigs.get("intent_agentic", []):
            stats["agentic"].add(row["account"])
        for row in sigs.get("intent_compete", []):
            stats["compete"].add(row["account"])
            all_compete_rows.append((first, row["account"], row.get("matched_keywords", ""), float(row.get("engagement_3mo") or 0)))
        for row in sigs.get("intent_international", []):
            stats["international"].add(row["account"])
        for row in sigs.get("intent_marketing", []):
            stats["marketing"].add(row["account"])
        for row in sigs.get("intent_b2b", []):
            stats["b2b"].add(row["account"])
        for row in sigs.get("g2_intent", []):
            stats["g2_intent"].append((first, row["account"]))

        if total > 0:
            stats["reps_with_signals"] += 1
            stats["rep_breakdown"].append((first, total, mqa_count, hvp_count, compete_count))

    # Sort rep breakdown by total signals descending
    stats["rep_breakdown"].sort(key=lambda r: -r[1])

    # Top 3 compete accounts by engagement across team
    all_compete_rows.sort(key=lambda r: -r[3])
    stats["top_compete"] = all_compete_rows[:3]

    return stats


def _name_to_slug(name: str) -> str:
    """Convert a coach name to a URL slug. e.g. 'Ryan Quarles' → 'ryan_quarles'"""
    import re
    return re.sub(r"[^a-z0-9_]", "", name.lower().replace(" ", "_"))


def _format_team_message(team: str, stats: dict, week_of: str, first_dm_week: bool = False) -> str:
    """Build the Slack message for a team lead."""
    display = _TEAM_DISPLAY_NAMES.get(team, team)
    lead_first = {
        "Consumer": "Ryan", "Emerging": "Dave", "Lifestyle 1": "Todd",
        "Lifestyle 2": "Kal", "Global Accounts": "Thom", "EMEA": "Daniel",
    }.get(team, "there")
    lead_full = {
        "Consumer": "Ryan Quarles", "Emerging": "Dave Greenberger",
        "Lifestyle 1": "Todd Mallett", "Lifestyle 2": "Kal Stephen",
        "Global Accounts": "Thom Armstrong", "EMEA": "Daniel Glock",
    }.get(team, "")
    coach_slug = _name_to_slug(lead_full) if lead_full else ""
    coach_url = f"{SITE_URL}?coach={coach_slug}" if coach_slug else SITE_URL

    try:
        from datetime import datetime
        week_fmt = datetime.strptime(week_of, "%Y-%m-%d").strftime("%B %-d")
    except Exception:
        week_fmt = week_of

    lines = [f"Hey {lead_first} — {display} signals for {week_fmt}.\n"]
    lines.append(f"→ Your team view: {coach_url}\n")

    # New MQA
    if stats["mqa_new"]:
        lines.append("*New MQA (accounts likely entering a buying cycle):*")
        for r in stats["mqa_new"]:
            plat = f" ({r[2]})" if r[2] and r[2] not in ("", "Other") else ""
            lines.append(f"— {r[1]} ({r[0]}){plat}")
        lines.append("")

    # Key signals
    signal_lines = []
    if stats["hvp"] > 0:
        signal_lines.append(f"{stats['hvp']} previously CL account{'s' if stats['hvp'] > 1 else ''} back on high-value pages")
    if stats["compete"]:
        top_compete = stats.get("top_compete", [])
        top_names = ", ".join(r[1] for r in top_compete[:2]) if top_compete else ""
        signal_lines.append(f"{len(stats['compete'])} accounts with compete intent" + (f" — {top_names}" if top_names else ""))
    activity_count = len(stats["activity_accounts"])
    if activity_count > 0:
        signal_lines.append(f"{activity_count} accounts with new contact engagement")
    if stats["agentic"]:
        signal_lines.append(f"{len(stats['agentic'])} accounts with agentic commerce intent")
    if stats["g2_intent"]:
        g2_names = ", ".join(f"{r[1]} ({r[0]})" for r in stats["g2_intent"][:3])
        signal_lines.append(f"{len(stats['g2_intent'])} G2 intent account{'s' if len(stats['g2_intent']) > 1 else ''} — {g2_names}")
    if signal_lines:
        lines.append("*This week:*")
        lines.extend(f"— {l}" for l in signal_lines)
        lines.append("")

    # Rep breakdown
    rep_rows = stats.get("rep_breakdown", [])
    if rep_rows:
        lines.append("*Your reps:*")
        for first, total, mqa, hvp, compete in rep_rows:
            badges = []
            if mqa:     badges.append(f"{mqa} MQA")
            if hvp:     badges.append(f"{hvp} prev. CL")
            if compete: badges.append(f"{compete} compete")
            badge_str = f" — {', '.join(badges)}" if badges else ""
            lines.append(f"— {first}: {total} signals{badge_str}")
        lines.append("")

    lines.append("Each rep got a personalised DM with their signals.")
    lines.append(f"→ Full hub: {SITE_URL}\n")
    lines.append("_Sent via Hannah's Pi_ 🤖")

    return "\n".join(lines)


def _format_brandon_message(all_stats: dict[str, dict], week_of: str) -> str:
    """Build Brandon's rolled-up all-teams summary."""
    from datetime import datetime

    try:
        week_fmt = datetime.strptime(week_of, "%Y-%m-%d").strftime("%B %-d")
    except Exception:
        week_fmt = week_of

    total_mqa = sum(len(s["mqa_new"]) for s in all_stats.values())
    total_hvp = sum(s["hvp"] for s in all_stats.values())
    total_hvp_all = sum(s["hvp_all"] for s in all_stats.values())
    total_compete = len(set().union(*[s["compete"] for s in all_stats.values()]))
    total_agentic = len(set().union(*[s["agentic"] for s in all_stats.values()]))
    total_reps = sum(s["reps_with_signals"] for s in all_stats.values())
    total_g2 = sum(len(s["g2_intent"]) for s in all_stats.values())

    BRANDON_DISPLAY = {
        "Consumer": "Consumer", "Emerging": "Specialized", "EMEA": "EMEA",
        "Global Accounts": "Global Accounts", "Lifestyle 1": "Lifestyle 1", "Lifestyle 2": "Lifestyle 2",
    }
    team_order = ["Emerging", "Consumer", "EMEA", "Global Accounts", "Lifestyle 2", "Lifestyle 1"]

    lines = [f"Hey Brandon — enterprise signals for {week_fmt}.\n"]
    lines.append(f"*{total_reps} reps had signals. {total_mqa} new MQAs.*\n")

    # New MQA — grouped by team, accounts only (no rep prefix clutter)
    if total_mqa > 0:
        lines.append("*New MQA (accounts likely entering a buying cycle):*")
        for team in team_order:
            s = all_stats.get(team)
            if not s or not s["mqa_new"]:
                continue
            display = BRANDON_DISPLAY.get(team, team)
            accounts = ", ".join(r[1] for r in s["mqa_new"])
            rep_attr = ", ".join(f"{r[1]} ({r[0]})" for r in s["mqa_new"])
            lines.append(f"— *{display} ({len(s['mqa_new'])})* — {rep_attr}")
        lines.append("")

    # Team summary — clean table
    lines.append("*By team:*")
    for team in team_order:
        s = all_stats.get(team)
        if not s:
            continue
        display = BRANDON_DISPLAY.get(team, team)
        reps_str = f"{s['reps_with_signals']} rep{'s' if s['reps_with_signals'] != 1 else ''}"
        badges = []
        if s["mqa_new"]:    badges.append(f"{len(s['mqa_new'])} MQA")
        if s["hvp"]:        badges.append(f"{s['hvp']} prev. CL")
        if s["compete"]:    badges.append(f"{len(s['compete'])} compete")
        if s["g2_intent"]:  badges.append(f"{len(s['g2_intent'])} G2")
        badge_str = " · " + " · ".join(badges) if badges else ""
        lines.append(f"— {display}: {reps_str} with signals{badge_str}")
    lines.append("")

    # Segment-level signals — only show what has data
    seg_lines = []
    if total_hvp_all:
        seg_lines.append(f"{total_hvp_all} accounts visiting high-value pages")
    if total_hvp:
        seg_lines.append(f"{total_hvp} previously CL accounts back on site")
    if total_compete:
        seg_lines.append(f"{total_compete} accounts with compete intent")
    if total_agentic:
        seg_lines.append(f"{total_agentic} accounts with agentic commerce intent")
    if total_g2:
        all_g2 = []
        for team, s in all_stats.items():
            d = BRANDON_DISPLAY.get(team, team)
            for rep, account in s["g2_intent"]:
                if len(account.split()) >= 2:
                    all_g2.append(f"{account} ({rep}, {d})")
        g2_sample = ", ".join(all_g2[:5])
        g2_extra = f" + {len(all_g2) - 5} more" if len(all_g2) > 5 else ""
        seg_lines.append(f"{total_g2} G2 intent accounts — {g2_sample}{g2_extra}")
    if seg_lines:
        lines.append("*Across the segment:*")
        lines.extend(f"— {l}" for l in seg_lines)
        lines.append("")

    lines.append(f"→ {SITE_URL}\n")
    lines.append("_Sent via Hannah's Pi_ 🤖")

    return "\n".join(lines)


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
