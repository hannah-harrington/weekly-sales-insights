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


def _api_call(method: str, token: str, params: dict | None = None) -> dict:
    """Make a Slack Web API call and return the parsed JSON response."""
    url = f"https://slack.com/api/{method}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

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


def build_dm_blocks(
    seller_name: str,
    summary: dict,
    signal_types: dict,
    week_of: str,
    personal_url: str,
) -> list[dict]:
    """Build Slack Block Kit blocks for a personalized DM."""
    first_name = seller_name.split()[0]
    total = summary.get("total", 0)

    parts = []
    for st_key, st_meta in signal_types.items():
        count = summary.get(st_key, 0)
        if count > 0:
            parts.append(f"{count} {st_meta['short_label']}")
    breakdown = ", ".join(parts) if parts else "check your report for details"

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Hey {first_name}* — your weekly sales insights are ready.\n\n"
                    f"You have *{total} signal{'s' if total != 1 else ''}* this week: {breakdown}"
                ),
            },
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View your report"},
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
                    "text": f"📊 Week of {week_of} · Powered by Weekly Sales Insights",
                }
            ],
        },
    ]
    return blocks


def send_dm(user_id: str, blocks: list[dict], fallback_text: str, token: str) -> bool:
    """Send a DM to a Slack user. Returns True on success."""
    try:
        result = _api_call(
            "chat.postMessage",
            token,
            {
                "channel": user_id,
                "blocks": blocks,
                "text": fallback_text,
            },
        )
        return result.get("ok", False)
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        print(f"  [Slack] Failed to DM user {user_id}: {exc}")
        return False


def notify_all(data: dict, site_url: str, token: str) -> dict:
    """
    Send personalized DMs to all sellers with signals.

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
            print(f"  [Slack] No email for {name}, skipping")
            stats["skipped"] += 1
            continue

        user_id = lookup_slack_user(email, token)
        if not user_id:
            print(f"  [Slack] No Slack user for {email} ({name}), skipping")
            stats["skipped"] += 1
            continue

        personal_url = f"{site_url}?seller={urllib.parse.quote(sid)}"
        blocks = build_dm_blocks(name, seller["summary"], signal_types, week_of, personal_url)
        fallback = f"Your weekly sales insights are ready — {total} signals this week: {personal_url}"

        ok = send_dm(user_id, blocks, fallback, token)
        if ok:
            stats["sent"] += 1
        else:
            stats["failed"] += 1

        time.sleep(0.5)

    return stats


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
