"""
Google News RSS fetcher for top accounts.

Fetches up to 3 recent headlines per account from Google News (free, no API key).
Results are cached to news_cache.json keyed by (week_date, account_name) so
reruns within the same week are instant.

Usage:
    from pipeline.sources import news
    account_news = news.fetch_account_news(account_names, week_date="2026-03-31")
"""

import json
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from email.utils import parsedate
from pathlib import Path

CACHE_FILE = Path(__file__).parent.parent / "news_cache.json"
MAX_PER_ACCOUNT = 3
SLEEP_BETWEEN = 0.4   # seconds between requests (be polite)
REQUEST_TIMEOUT = 8   # seconds


def fetch_account_news(
    account_names: list[str],
    week_date: str,
    max_per_account: int = MAX_PER_ACCOUNT,
) -> dict:
    """
    Fetch Google News RSS for a list of account names.

    Returns a dict keyed by lowercase account name:
        {
            "acme corp": [
                {"title": "...", "url": "...", "source": "...", "date": "YYYY-MM-DD"},
                ...
            ],
            ...
        }

    Results are cached per (week_date, account) in news_cache.json.
    On a re-run for the same week, cached results are returned immediately.
    """
    cache = _load_cache()
    result: dict[str, list] = {}
    to_fetch: list[str] = []

    for name in account_names:
        key = _cache_key(week_date, name)
        if key in cache:
            result[name.lower()] = cache[key]
        else:
            to_fetch.append(name)

    if to_fetch:
        print(f"  Fetching Google News for {len(to_fetch)} accounts (cached: {len(account_names) - len(to_fetch)})...")

    for i, name in enumerate(to_fetch):
        articles = _fetch_rss(name, max_per_account)
        result[name.lower()] = articles
        cache[_cache_key(week_date, name)] = articles
        if i < len(to_fetch) - 1:
            time.sleep(SLEEP_BETWEEN)

    if to_fetch:
        _save_cache(cache)

    return result


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _cache_key(week_date: str, account_name: str) -> str:
    return f"{week_date}::{account_name.lower()}"


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


# ── RSS fetch ──────────────────────────────────────────────────────────────────

def _fetch_rss(company_name: str, max_results: int = MAX_PER_ACCOUNT) -> list[dict]:
    """Fetch and parse Google News RSS for a company name."""
    query = urllib.parse.quote(f'"{company_name}"')
    url = (
        f"https://news.google.com/rss/search"
        f"?q={query}&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SalesInsightsBot/1.0)"},
        )
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = resp.read()

        root = ET.fromstring(data)
        channel = root.find("channel")
        if channel is None:
            return []

        articles = []
        for item in channel.findall("item")[:max_results]:
            title = item.findtext("title", "").strip()
            link  = item.findtext("link", "").strip()
            pub   = item.findtext("pubDate", "").strip()

            source_el = item.find("source")
            source = (source_el.text or "").strip() if source_el is not None else ""

            # Skip generic/low-signal headlines
            if not title or not link:
                continue

            articles.append({
                "title":  title,
                "url":    link,
                "source": source,
                "date":   _parse_rfc2822(pub),
            })

        return articles

    except Exception:
        return []


def _parse_rfc2822(date_str: str) -> str:
    """Convert RFC2822 date string to YYYY-MM-DD."""
    if not date_str:
        return ""
    try:
        t = parsedate(date_str)
        if t:
            return f"{t[0]}-{t[1]:02d}-{t[2]:02d}"
    except Exception:
        pass
    # Fallback: just return first 16 chars if something is there
    return date_str[:10] if len(date_str) >= 10 else date_str
