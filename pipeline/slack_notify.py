"""
Slack DM notifications for Weekly Sales Insights.

Sends each rep a personalized DM with their signal count and a direct
link to their report. Uses the Slack Web API via urllib (no extra deps).

Token resolution order:
1. SLACK_BOT_TOKEN env var (bot token, xoxb-...) — preferred, requires Slack app approval
2. Personal session token from ~/.config/callm/credentials.json (xoxc-...) — workaround
   Uses a static email→user ID map (slack_user_map.json) since lookupByEmail
   doesn't work with personal tokens.
"""

import json
import os
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request

_user_cache: dict[str, str | None] = {}

# Static email→user ID map used when personal token is active (no bot token)
_USER_MAP: dict[str, str] = {}
_USER_MAP_LOADED = False

def _load_user_map() -> None:
    """Load the static slack_user_map.json if not already loaded."""
    global _USER_MAP, _USER_MAP_LOADED
    if _USER_MAP_LOADED:
        return
    map_path = pathlib.Path(__file__).parent / "slack_user_map.json"
    if map_path.exists():
        data = json.loads(map_path.read_text())
        _USER_MAP = {k: v for k, v in data.items() if not k.startswith("_")}
    _USER_MAP_LOADED = True


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
    """Make a Slack Web API call and return the parsed JSON response."""
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

    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def lookup_slack_user(email: str, token: str) -> str | None:
    """Resolve a Shopify email to a Slack user ID. Returns None on failure.

    If using a personal token (xoxc-), falls back to the static user map
    since lookupByEmail requires a bot token.
    """
    if not email:
        return None
    if email in _user_cache:
        return _user_cache[email]

    # Personal tokens can't use lookupByEmail — use static map instead
    if token.startswith("xoxc-"):
        _load_user_map()
        uid = _USER_MAP.get(email)
        if uid:
            _user_cache[email] = uid
            return uid
        print(f"  [Slack] {email} not in user map, skipping (add manually to slack_user_map.json)")
        _user_cache[email] = None
        return None

    try:
        result = _api_call("users.lookupByEmail", token, {"email": email})
        if result.get("ok") and result.get("user", {}).get("id"):
            uid = result["user"]["id"]
            _user_cache[email] = uid
            return uid
        _user_cache[email] = None
        return None
    except (urllib.error.URLError, json.JSONDecodeError, KeyError) as exc:
        print(f"  [Slack] Could not look up {email}: {exc}")
        _user_cache[email] = None
        return None


def _build_highlights(signals: dict) -> list[str]:
    """Pull 3-4 specific account/person highlights to surface in the DM."""
    lines = []

    # 1. New MQA — highest priority, name the account + why
    for row in signals.get("mqa_new", [])[:2]:
        acct = row.get("account", "")
        platform = row.get("platform", "")
        pages = row.get("pages_visited", "")
        if not acct:
            continue
        detail = ""
        if platform and "shopify" not in platform.lower():
            detail = f" — on {platform}"
        elif pages:
            first_page = pages.split(",")[0].strip().split("/")[-1].replace("-", " ")
            if first_page:
                detail = f" — researching {first_page}"
        lines.append(f"🟢 *{acct}* just moved to MQA{detail}")

    # 2. Compete intent — name the account
    for row in signals.get("intent_compete", [])[:1]:
        acct = row.get("account", "")
        if acct:
            lines.append(f"⚔️ *{acct}* is actively researching competitors")

    # 3. Named person from activity
    for row in signals.get("activity", [])[:2]:
        name = row.get("full_name", "") or row.get("name", "")
        title = row.get("title", "")
        acct = row.get("account", "")
        if name and acct:
            short_title = title.split(",")[0].strip()[:45] if title else ""
            detail = f" ({short_title})" if short_title else ""
            lines.append(f"👤 *{name}*{detail} at {acct} engaged this week")
            break

    # 4. G2 — strong buying signal
    for row in signals.get("g2_intent", [])[:1]:
        acct = row.get("account", "")
        if acct:
            lines.append(f"🔍 *{acct}* is comparing vendors on G2")

    # 4b. LinkedIn High/Very High engagement (max 1)
    for row in signals.get("li_very_high", [])[:1]:
        acct = row.get("account", "")
        if acct:
            lines.append(f"💼 *{acct}* — High or Very High LinkedIn engagement this week")

    # 5. HVP fallback if nothing else
    if len(lines) < 2:
        hvp_rows = signals.get("hvp", []) + signals.get("hvp_all", [])
        for row in hvp_rows[:2]:
            acct = row.get("account", "")
            if acct and not any(acct in l for l in lines):
                lines.append(f"📈 *{acct}* is visiting high-value pages")

    return lines[:4]


def build_dm_blocks(
    seller_name: str,
    summary: dict,
    signal_types: dict,
    week_of: str,
    personal_url: str,
    signals: dict | None = None,
) -> list[dict]:
    """Build Slack Block Kit blocks for a personalized DM."""
    first_name = seller_name.split()[0]
    total = summary.get("total", 0)

    # Count line — only signal types with data
    parts = []
    for st_key, st_meta in signal_types.items():
        count = summary.get(st_key, 0)
        if count > 0:
            parts.append(f"{count} {st_meta['short_label']}")
    breakdown = ", ".join(parts) if parts else "signals waiting"

    # Build highlights from real account data if available
    highlights = _build_highlights(signals or {})

    li_count = summary.get("li_very_high", 0)
    mqa_count = summary.get("mqa_new", 0)

    if highlights:
        highlight_text = "\n".join(f"• {h}" for h in highlights)
        body = (
            f"*Hey {first_name}* — your weekly signals are in. Here's what's hot:\n\n"
            f"{highlight_text}\n\n"
            f"_+ {total} total signals this week ({breakdown})_"
        )
    else:
        body = (
            f"*Hey {first_name}* — your weekly sales insights are ready.\n\n"
            f"You have *{total} signal{'s' if total != 1 else ''}* this week: {breakdown}"
        )

    # MQA explanation for reps
    if mqa_count:
        body += "\n\n_MQA = accounts likely entering a buying cycle._"

    # LinkedIn new feature callout
    li_note = (
        "\n\n🆕 *New this week:* We're now showing LinkedIn signals. "
        "These are accounts with High or Very High engagement with Shopify content on LinkedIn — "
        "a new signal to layer on top of your existing report."
    )

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": li_note},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": body},
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View your full report →"},
                    "url": personal_url,
                    "style": "primary",
                }
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"📊 Week of {week_of} · Sales Insights Hub",
                }
            ],
        },
    ]
    return blocks


def send_dm(user_id: str, blocks: list[dict], fallback_text: str, token: str) -> bool:
    """Send a DM to a Slack user. Returns True on success."""
    try:
        # Personal tokens (xoxc-) can't post to a user ID directly —
        # need to open the DM channel first to get a channel ID.
        channel = user_id
        if token.startswith("xoxc-"):
            open_result = _api_call("conversations.open", token, {"users": user_id})
            if open_result.get("ok"):
                channel = open_result["channel"]["id"]
        result = _api_call(
            "chat.postMessage",
            token,
            {
                "channel": channel,
                "blocks": blocks,
                "text": fallback_text,
            },
        )
        return result.get("ok", False)
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        print(f"  [Slack] Failed to DM user {user_id}: {exc}")
        return False


def validate_token(token: str) -> tuple[bool, str]:
    """Check the token is valid via auth.test. Returns (ok, error_message)."""
    try:
        result = _api_call("auth.test", token)
        if result.get("ok"):
            return True, ""
        error = result.get("error", "unknown_error")
        if error in ("invalid_auth", "token_expired", "token_revoked", "not_authed", "account_inactive"):
            return False, (
                f"Slack token is invalid or expired (error: {error}).\n"
                f"  Fix: node ~/pi-backup/refresh-callm-creds.js\n"
                f"  Then re-run --notify."
            )
        return False, f"Slack auth.test failed: {error}"
    except Exception as exc:
        return False, f"Slack auth check failed: {exc}"


def notify_all(data: dict, site_url: str, token: str) -> dict:
    """
    Send personalized DMs to all sellers with signals.

    Returns a summary dict: {"sent": N, "skipped": N, "failed": N}
    """
    sellers = data.get("sellers", {})
    signal_types = data.get("signal_types", {})
    week_of = data.get("meta", {}).get("week_of", "")
    stats = {"sent": 0, "skipped": 0, "failed": 0}

    # Validate token before attempting any sends — fail fast on auth errors
    ok, err = validate_token(token)
    if not ok:
        print(f"  [Slack] ❌ Auth failed — aborting DM send.")
        print(f"  [Slack] {err}")
        stats["failed"] = len(sellers)
        return stats

    for sid, seller in sellers.items():
        name = seller.get("name", "")
        email = seller.get("email", "")
        total = seller.get("summary", {}).get("total", 0)

        if total == 0:
            stats["skipped"] += 1
            continue

        if not email:
            print(f"  [Slack] No email for {name}, skipping")
            stats["skipped"] += 1
            continue

        user_id = lookup_slack_user(email, token)
        if not user_id:
            print(f"  [Slack] No Slack user for {email} ({name}), skipping")
            stats["skipped"] += 1
            continue

        personal_url = f"{site_url}?seller={urllib.parse.quote(sid)}"
        blocks = build_dm_blocks(name, seller["summary"], signal_types, week_of, personal_url, signals=seller.get("signals", {}))
        fallback = f"Your weekly sales insights are ready — {total} signals this week: {personal_url}"

        ok = send_dm(user_id, blocks, fallback, token)
        if ok:
            stats["sent"] += 1
        else:
            stats["failed"] += 1

        time.sleep(0.5)

    return stats


def _get_top_headline(account_name: str, account_news: dict) -> str | None:
    """Look up the top Google News headline for an account name. Returns None if not found."""
    if not account_news:
        return None
    keys = [
        account_name.lower(),
        account_name.lower().split(" - ")[-1].strip(),
        account_name.lower().split(" - ")[0].strip(),
    ]
    for k in keys:
        articles = account_news.get(k, [])
        if articles:
            title = articles[0].get("title", "")
            # Strip trailing source attribution (e.g. " - Reuters")
            if " - " in title:
                title = title.rsplit(" - ", 1)[0].strip()
            return title
    return None


def _build_start_here(signals: dict, summary: dict, account_news: dict | None = None) -> list[str]:
    """
    Pick the top 2-3 accounts to highlight in the 'Start here' section.
    Strategy: prioritise MQA, HVP, G2 intent first, then compete/agentic for variety.
    Appends a Google News headline under each account if available.
    """
    callouts = []
    used_types = set()
    news = account_news or {}

    def add(line: str, type_key: str, account_name: str = "") -> bool:
        if len(callouts) >= 3:
            return False
        headline = _get_top_headline(account_name, news) if account_name else None
        full = line + (f"\n  📰 \"{headline}\"" if headline else "")
        callouts.append(full)
        used_types.add(type_key)
        return True

    # 1. New MQA — always show all (rare and high priority)
    for row in signals.get("mqa_new", []):
        account = row.get("account", "")
        platform = row.get("platform", "")
        brief = row.get("brief", "")
        sentences = [s.strip() for s in brief.split(".") if s.strip()]
        angle = next((s for s in reversed(sentences) if not s.lower().startswith("currently on") and not s.lower().startswith("recently") and not s[0].isdigit()), "")
        plat_str = f" — on {platform}" if platform and platform not in ("", "Other") else ""
        angle_str = f" {angle}." if angle else ""
        add(f"*{account} just hit MQA*{plat_str}.{angle_str}", "mqa_new", account)
        if len(callouts) >= 3:
            return callouts

    # 2. HVP — closed-lost back on site (max 1)
    for row in signals.get("hvp", []):
        if "hvp" not in used_types:
            account = row.get("account", "")
            add(f"*{account}* — closed-lost account back on your high-value pages. Re-engagement timing is strong.", "hvp", account)
            break

    if len(callouts) >= 3:
        return callouts

    # 3. G2 intent — actively comparing vendors (max 1)
    for row in signals.get("g2_intent", []):
        if "g2_intent" not in used_types:
            account = row.get("account", "")
            add(f"*{account}* — G2 intent signal. They're actively comparing vendors right now. Good time to get in front of them.", "g2_intent", account)
            break

    if len(callouts) >= 3:
        return callouts

    # 3b. LinkedIn High/Very High (max 1)
    for row in signals.get("li_very_high", [])[:1]:
        if "li" not in used_types:
            account = row.get("account", "")
            add(f"*{account}* — High or Very High LinkedIn engagement this week. They're actively engaging with Shopify content on LinkedIn.", "li", account)
            break

    if len(callouts) >= 3:
        return callouts

    # 4. Best compete account (max 1 — pick highest engagement)
    compete = sorted(signals.get("intent_compete", []), key=lambda r: float(r.get("engagement_3mo") or 0), reverse=True)
    for row in compete:
        if "compete" not in used_types:
            account = row.get("account", "")
            kws = row.get("matched_keywords", "")
            add(f"*{account}* — compete intent ({kws}). Good time to get in front of them.", "compete", account)
            break

    if len(callouts) >= 3:
        return callouts

    # 5. Best agentic account (max 1 — pick highest engagement)
    agentic = sorted(signals.get("intent_agentic", []), key=lambda r: float(r.get("engagement_3mo") or 0), reverse=True)
    for row in agentic:
        if "agentic" not in used_types:
            account = row.get("account", "")
            kws = row.get("matched_keywords", "")
            add(f"*{account}* — AI intent ({kws}). Strong buying signals right now.", "agentic", account)
            break

    if len(callouts) >= 3:
        return callouts

    # 6. Best HVP all account (max 1)
    for row in signals.get("hvp_all", []):
        if "hvp_all" not in used_types:
            account = row.get("account", "")
            platform = row.get("platform", "")
            pages = row.get("pages_visited", "")
            plat_str = f" — on {platform}" if platform and platform not in ("", "Other") else ""
            page_str = f", visiting /{pages.split(',')[0].strip()}" if pages else ""
            add(f"*{account}*{plat_str}{page_str}.", "hvp_all", account)
            break

    return callouts


def _clean_summary_line(summary: dict) -> str:
    """Build a clean one-line signal summary — no noise, just the 4 key stats."""
    mqa    = summary.get("mqa_new", 0)
    hvp    = summary.get("hvp", 0)
    people = summary.get("activity", 0) + summary.get("new_people", 0)
    intent = sum(summary.get(t, 0) for t in [
        "intent_agentic", "intent_compete", "intent_international",
        "intent_marketing", "intent_b2b",
    ])
    li     = summary.get("li_very_high", 0)
    parts = []
    if mqa:    parts.append(f"{mqa} New MQA (accounts likely entering a buying cycle)")
    if hvp:    parts.append(f"{hvp} Previously CL")
    if people: parts.append(f"{people} engaged people")
    if intent: parts.append(f"{intent} intent signals")
    if li:     parts.append(f"{li} LinkedIn High/Very High")
    return " · ".join(parts) if parts else "signals this week"


def build_personal_dm_text(
    seller_name: str,
    summary: dict,
    signals: dict,
    signal_types: dict,
    week_of: str,
    personal_url: str,
    account_news: dict | None = None,
) -> str:
    """
    Build a personalised plain-text Slack message for a rep.
    Highlights 2-3 specific accounts in a 'Start here' section.
    Includes Google News headlines and G2 intent signals where available.
    """
    first = seller_name.split()[0]

    callouts = _build_start_here(signals, summary, account_news)

    lines = [f"Hey {first} — your weekly signals are ready.\n"]

    if callouts:
        lines.append("*Start here:*")
        for c in callouts:
            lines.append(f"• {c}")
        lines.append("")

    summary_line = _clean_summary_line(summary)
    lines.append(f"_{summary_line}_\n")
    lines.append(f"→ {personal_url}\n")
    lines.append("_Sent via Hannah's Pi_ 🤖")

    return "\n".join(lines)


def notify_all_personal(data: dict, site_url: str, token: str) -> dict:
    """
    Send personalised 'Start here' DMs to all sellers with signals.
    Replaces the generic count-based message with specific account callouts.

    Returns a summary dict: {"sent": N, "skipped": N, "failed": N}
    """
    sellers = data.get("sellers", {})
    signal_types = data.get("signal_types", {})
    week_of = data.get("meta", {}).get("week_of", "")
    stats = {"sent": 0, "skipped": 0, "failed": 0}

    for sid, seller in sellers.items():
        name = seller.get("name", "")
        email = seller.get("email", "")
        total = seller.get("summary", {}).get("total", 0)

        if total == 0:
            stats["skipped"] += 1
            continue

        if not email:
            stats["skipped"] += 1
            continue

        user_id = lookup_slack_user(email, token)
        if not user_id:
            stats["skipped"] += 1
            continue

        personal_url = f"{site_url}?seller={urllib.parse.quote(sid)}"
        account_news = data.get("account_news", {})
        text = build_personal_dm_text(
            name, seller["summary"], seller["signals"], signal_types, week_of, personal_url, account_news
        )

        try:
            result = _api_call("conversations.open", token, {"users": user_id})
            channel = result.get("channel", {}).get("id") if result.get("ok") else None
            if not channel:
                stats["failed"] += 1
                continue
            result = _api_call("chat.postMessage", token, {"channel": channel, "text": text})
            if result.get("ok"):
                stats["sent"] += 1
            else:
                stats["failed"] += 1
        except (urllib.error.URLError, json.JSONDecodeError) as exc:
            print(f"  [Slack] Failed to DM {name}: {exc}")
            stats["failed"] += 1

        time.sleep(0.5)

    return stats


# Hannah's Slack user ID — always notify her on pipeline runs
_HANNAH_SLACK_ID = "U02SQJTUQDT"


def notify_hannah(success: bool, week_date: str, details: str = "", token: str | None = None) -> None:
    """Send Hannah a Slack DM with the Friday pipeline result.

    Called at the end of friday_pipeline.py whether the run succeeds or fails.
    If no token is available, prints a warning to the log instead.
    """
    tok = token or get_token()
    if not tok:
        print("[Slack] Cannot notify Hannah — no Slack token available.")
        print("[Slack] Run: node ~/pi-backup/refresh-callm-creds.js")
        return

    if success:
        text = (
            f":white_check_mark: *Friday pipeline complete* — week of {week_date}\n"
            f"Site is live and ready for Monday Slacks.\n"
            f"salesinsights-hub.quick.shopify.io\n"
            + (f"\n{details}" if details else "")
        )
    else:
        text = (
            f":x: *Friday pipeline FAILED* — week of {week_date}\n"
            f"The site may be stale. Check the log:\n"
            f"`~/Desktop/Cursor Brain/sales-insights/logs/friday-pipeline.log`\n"
            + (f"\n*Error:* {details}" if details else "")
        )

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": text},
        }
    ]
    sent = send_dm(_HANNAH_SLACK_ID, blocks, text, tok)
    if not sent:
        print("[Slack] Warning: Hannah notification DM failed to send.")


def get_token() -> str | None:
    """Get the Slack token.

    Tries in order:
    1. SLACK_BOT_TOKEN env var (bot token — preferred)
    2. Personal session token from ~/.config/callm/credentials.json (workaround)
    """
    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    if bot_token:
        return bot_token

    # Fallback: personal token from callm credentials
    creds_path = pathlib.Path.home() / ".config" / "callm" / "credentials.json"
    if creds_path.exists():
        try:
            creds = json.loads(creds_path.read_text())
            token = creds.get("info", {}).get("token")
            if token:
                print("  [Slack] Using personal session token (no SLACK_BOT_TOKEN set).")
                print("  [Slack] Run `node ~/pi-backup/refresh-callm-creds.js` if DMs fail.")
                return token
        except (json.JSONDecodeError, KeyError):
            pass

    return None
