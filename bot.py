import os
import json
import time
import hashlib
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import feedparser
import requests


FEEDS_PATH = os.getenv("FEEDS_PATH", "feeds.json")
EXCLUDE_WORDS_PATH = os.getenv("EXCLUDE_WORDS_PATH", "exclude_words.json")
STATE_PATH = os.getenv("STATE_PATH", "posted.json")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
WARM_START = os.getenv("WARM_START", "false").lower() == "true"
MAX_POSTS_PER_RUN = int(os.getenv("MAX_POSTS_PER_RUN", "1000"))
PRUNE_DAYS = int(os.getenv("PRUNE_DAYS", "90"))


def normalize_url(url: str) -> str:
    u = urlparse(url)
    q = [
        (k, v)
        for k, v in parse_qsl(u.query, keep_blank_values=True)
        if not (k.lower().startswith("utm_") or k.lower() in {"fbclid", "gclid"})
    ]
    new_query = urlencode(q)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def stable_key(title: str, url: str) -> str:
    base = (normalize_url(url).strip() + "\n" + (title or "").strip()).encode("utf-8")
    return hashlib.sha256(base).hexdigest()


def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def is_excluded(title: str, exclude_words) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    tl = t.lower()
    for w in exclude_words:
        if not w:
            continue
        if w in t:
            return True
        if w.lower() in tl:
            return True
    return False


def post_to_slack(title: str, url: str):
    text = f"{title}\n{url}"
    r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=20)
    if r.status_code == 429:
        retry = int(r.headers.get("Retry-After", "2"))
        time.sleep(max(1, retry))
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=20)
    r.raise_for_status()


def prune_state(state: dict, now_ts: int, days: int):
    cutoff = now_ts - days * 24 * 60 * 60
    old_keys = [k for k, v in state.items() if isinstance(v, int) and v < cutoff]
    for k in old_keys:
        state.pop(k, None)


def main():
    if not SLACK_WEBHOOK_URL and not WARM_START:
        raise RuntimeError("SLACK_WEBHOOK_URL が未設定（GitHub Secretsに入れて渡す）")

    feeds = load_json(FEEDS_PATH, [])
    exclude_words = load_json(EXCLUDE_WORDS_PATH, [])
    state = load_json(STATE_PATH, {})

    now = int(time.time())
    prune_state(state, now, PRUNE_DAYS)

    posted_count = 0
    new_count = 0

    for feed_url in feeds:
        d = feedparser.parse(feed_url)
        entries = getattr(d, "entries", []) or []

        for e in entries:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue

            if is_excluded(title, exclude_words):
                continue

            key = stable_key(title, link)
            if key in state:
                continue

            state[key] = now
            new_count += 1

            if WARM_START:
                continue

            post_to_slack(title, link)
            posted_count += 1

            time.sleep(0.25)
            if posted_count >= MAX_POSTS_PER_RUN:
                break

        if posted_count >= MAX_POSTS_PER_RUN:
            break

    save_json(STATE_PATH, state)
    print(f"new={new_count} posted={posted_count} warm_start={WARM_START}")


if __name__ == "__main__":
    main()
