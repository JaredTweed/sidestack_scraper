#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

def read_json(path: Path) -> Any:
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)

def to_str(v: Optional[Any]) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    return str(v)

def pick_language(details: Dict[str, Any]) -> str:
    for key in ("in_language", "language_ui"):
        lang = details.get(key)
        if isinstance(lang, str) and lang.strip():
            return lang.strip()
    return ""

def pick_feed_url(item: Dict[str, Any]) -> str:
    s = item.get("substack", {}) if isinstance(item.get("substack"), dict) else {}
    return (
        s.get("feed_verified_url")
        or s.get("feed_url")
        or s.get("alternate_feed_url")
        or ""
    )

def compute_total_subscribers(details: Dict[str, Any]) -> Optional[int]:
    free_cnt = details.get("free_subscribers_count")
    paid_cnt = details.get("paid_subscribers_count")
    if free_cnt is None and paid_cnt is None:
        return None
    total = 0
    for v in (free_cnt, paid_cnt):
        if isinstance(v, (int, float)):
            total += int(v)
    return total

def map_item(item: Dict[str, Any]) -> Dict[str, str]:
    sub = item.get("substack", {}) if isinstance(item.get("substack"), dict) else {}
    details = sub.get("details", {}) if isinstance(sub.get("details"), dict) else {}
    headline = to_str(details.get("headline"))
    author_name = to_str(details.get("author_name"))
    language = to_str(pick_language(details))
    feed_url = to_str(pick_feed_url(item))
    category = to_str(item.get("category"))
    total_subs_val = compute_total_subscribers(details)
    total_subscribers = "" if total_subs_val is None else str(total_subs_val)
    return {
        "headline": headline,
        "author_name": author_name,
        "language": language,
        "feed_url": feed_url,
        "category": category,
        "total_subscribers": total_subscribers,
    }, total_subs_val or 0

def format_total_subscribers(value: Optional[Any]) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(int(value))
    value_str = str(value).strip()
    if not value_str:
        return ""
    try:
        return str(int(float(value_str)))
    except ValueError:
        return value_str


def main(argv: List[str]) -> int:
    in_path = Path("feeds.json")
    out_path = Path("feeds_mapped.json")
    min_subscribers = 0
    merge_other_feeds = False

    raw_args = argv[1:]
    positional: List[str] = []
    idx = 0
    while idx < len(raw_args):
        arg = raw_args[idx]
        if arg == "--min-subscribers":
            try:
                min_subscribers = int(raw_args[idx + 1])
            except (IndexError, ValueError):
                raise SystemExit("Error: --min-subscribers flag requires an integer value.")
            idx += 2
        elif arg == "--merge-other-feeds":
            merge_other_feeds = True
            idx += 1
        else:
            positional.append(arg)
            idx += 1

    if len(positional) >= 1:
        in_path = Path(positional[0])
    if len(positional) >= 2:
        out_path = Path(positional[1])

    data = read_json(in_path)
    if not isinstance(data, list):
        raise SystemExit("Input JSON must be a list of items.")

    out = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "OK":
            continue
        mapped, total = map_item(item)
        if total < min_subscribers:
            continue
        if not mapped["feed_url"]:
            continue
        out.append(mapped)

    seen_urls = {entry["feed_url"] for entry in out if entry.get("feed_url")}

    def append_extra_feed(feed: Dict[str, Any]) -> None:
        url = to_str(feed.get("feed_url")).strip()
        if not url or url in seen_urls:
            return
        entry = {
            "headline": to_str(feed.get("headline") or feed.get("title")),
            "author_name": to_str(feed.get("author_name")),
            "language": to_str(feed.get("language")),
            "feed_url": url,
            "category": to_str(feed.get("category")),
            "total_subscribers": format_total_subscribers(feed.get("total_subscribers") or feed.get("subscribers")),
        }
        out.append(entry)
        seen_urls.add(url)

    if merge_other_feeds:
        extra_path = Path("other_feeds.json")
        extra_data = read_json(extra_path)
        if not isinstance(extra_data, list):
            raise SystemExit(f"Error: {extra_path} must contain a list of feeds.")
        for item in extra_data:
            if not isinstance(item, dict):
                continue
            append_extra_feed(item)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(out)} items to {out_path} (min_subscribers={min_subscribers})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
