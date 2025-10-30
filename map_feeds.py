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

def main(argv: List[str]) -> int:
    in_path = Path("feeds.json")
    out_path = Path("feeds_mapped.json")
    min_subscribers = 0

    args = argv[1:]
    # Simple arg parsing
    if "--min-subscribers" in args:
        idx = args.index("--min-subscribers")
        try:
            min_subscribers = int(args[idx + 1])
        except (IndexError, ValueError):
            raise SystemExit("Error: --min-subscribers flag requires an integer value.")
        del args[idx:idx + 2]

    if len(args) >= 1:
        in_path = Path(args[0])
    if len(args) >= 2:
        out_path = Path(args[1])

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
    
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(out)} items to {out_path} (min_subscribers={min_subscribers})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
