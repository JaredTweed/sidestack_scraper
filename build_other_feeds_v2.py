#!/usr/bin/env python3
"""Build ``other_feeds_v2.json`` from candidate feeds and validator cache.

The builder is intentionally separate from the validator so that imports can be
audited, re-run, and de-duplicated without touching the network. Candidates are
accepted as JSON arrays or JSON Lines. Only feeds marked ``valid`` in the
validation cache are written to the final directory.

Example::

    python build_other_feeds_v2.py \
        --candidate-file candidates/curated.json \
        --candidate-file candidates/smallweb.json \
        --cache .feed_validation_cache.json \
        --output other_feeds_v2.json \
        --stats feed_directory_stats.json
"""

from __future__ import annotations

import argparse
import html
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Optional

from feed_utils import (
    clean_string,
    dump_json,
    host_of,
    is_substack_url,
    normalize_language,
    normalize_url,
)

CATEGORY_ALIASES = {
    "Environment": "Climate & Energy",
    "Community": "Society",
    "Video": "Media",
    "Tech": "Technology",
    "News": "News & Politics",
    "Economics": "Business & Economics",
    "World Politics": "International Affairs",
    "Urbanism": "Urbanism & Transport",
    "Transit": "Urbanism & Transport",
    "Energy": "Climate & Energy",
    "Investigations": "News & Politics",
    "Humor": "Arts & Culture",
    "Ideas": "Arts & Culture",
}

LINUX_KEYWORDS = (
    "linux",
    "gnome",
    "kde",
    "ubuntu",
    "debian",
    "fedora",
    "arch linux",
    "archlinux",
    "wayland",
    "flatpak",
    "open source",
    "opensource",
    "free software",
)

CATEGORY_KEYWORDS: list[tuple[str, tuple[str, ...]]] = [
    ("Artificial Intelligence", ("artificial intelligence", "machine learning", " ai ", "llm", "openai")),
    ("Linux & Open Source", ("linux", "gnome", "kde", "ubuntu", "debian", "fedora", "arch linux", "open source", "flatpak", "wayland")),
    ("Programming", ("programming", "developer", "software engineering", "rust", "python", "javascript", "database", "devops", "github")),
    ("Security", ("security", "infosec", "cybersecurity", "privacy", "malware")),
    ("Climate & Energy", ("climate", "energy", "solar", "renewable", "battery", "electric vehicle", "heat pump", "electrification")),
    ("Urbanism & Transport", ("urbanism", "transit", "transport", "cycling", "bicycle", "railway", "rail ", "infrastructure")),
    ("Canadian News", ("canada", "canadian", "toronto", "vancouver", "ontario", "quebec", "alberta")),
    ("Business & Economics", ("business", "econom", "finance", "market", "startup", "investing", "money")),
    ("Health & Medicine", ("health", "medicine", "medical", "hospital", "doctor", "vaccine")),
    ("Science", ("science", "physics", "astronomy", "space", "biology", "chemistry", "mathematics", "research")),
    ("Arts & Culture", ("art", "music", "film", "movie", "book", "culture", "photography", "design", "theatre", "literature")),
    ("Food & Drink", ("food", "recipe", "cooking", "wine", "beer", "restaurant", "coffee")),
    ("Travel", ("travel", "tourism", "hotel", "flight")),
    ("Sports", ("sport", "football", "soccer", "baseball", "basketball", "hockey", "cricket", "tennis")),
    ("Technology", ("tech", "gadget", "computer", "mobile", "apple", "google", "microsoft", "internet")),
]


def infer_category(headline: str, url: str, fallback: str = "General") -> str:
    text = f" {headline} {url} ".lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = f" {text} "
    for category, needles in CATEGORY_KEYWORDS:
        if any(needle in text for needle in needles):
            return category
    return fallback


def normalize_category(value: str, headline: str, url: str) -> str:
    value = clean_string(value)
    if not value or value == "General":
        value = infer_category(headline, url)
    value = CATEGORY_ALIASES.get(value, value)
    text = f" {headline} {url} ".lower()
    if value in {"Technology", "Programming", "Personal Blogs", "General"} and any(
        keyword in text for keyword in LINUX_KEYWORDS
    ):
        return "Linux & Open Source"
    return value


HTML_FORMATTING_TAG = re.compile(
    r"</?(?:a|b|br|code|del|div|em|i|mark|p|s|small|span|strike|strong|sub|sup|u)(?:\s[^>]*)?>",
    re.I,
)


def clean_headline(value: str) -> str:
    value = html.unescape(clean_string(value))
    value = HTML_FORMATTING_TAG.sub("", value)
    value = re.sub(r"\s*::\s*(blog|feed|rss|home|posts?)\s*$", "", value, flags=re.I)
    value = re.sub(r"\s+", " ", value).strip()
    return value[:300]


def is_generic_title(value: str) -> bool:
    value = clean_headline(value).casefold()
    return value in {
        "blog",
        "blog posts",
        "posts",
        "home",
        "homepage",
        "feed",
        "rss",
        "articles",
        "news",
        "latest posts",
        "website",
    }


def looks_like_url_or_domain(value: str) -> bool:
    value = value.strip()
    if re.match(r"^https?://", value, re.I):
        return True
    if re.match(r"^www\.", value, re.I):
        return True
    return bool(re.fullmatch(r"[a-z0-9-]+(\.[a-z0-9-]+)+(/.*)?", value, re.I))


def choose_headline(candidate: dict[str, Any], result: dict[str, Any]) -> str:
    candidate_title = clean_headline(candidate.get("headline", ""))
    feed_title = clean_headline(result.get("feed_title", ""))
    host = host_of(candidate.get("feed_url", ""))
    if candidate_title and not is_generic_title(candidate_title):
        if looks_like_url_or_domain(candidate_title):
            if feed_title and not is_generic_title(feed_title) and not looks_like_url_or_domain(feed_title):
                return feed_title
            if candidate_title.lower() == host and feed_title and not is_generic_title(feed_title):
                return feed_title
            candidate_title = re.sub(r"^https?://", "", candidate_title, flags=re.I)
            candidate_title = re.sub(r"^www\.", "", candidate_title, flags=re.I)
            candidate_title = candidate_title.rstrip("/")
        return candidate_title
    if feed_title and not is_generic_title(feed_title):
        return feed_title
    return candidate_title or feed_title or host or candidate["feed_url"]


def load_candidates(paths: Iterable[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise SystemExit(f"candidate file not found: {path}")
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            continue
        if text.startswith("["):
            data = json.loads(text)
            if not isinstance(data, list):
                raise SystemExit(f"{path} must contain a JSON array")
            result.extend(item for item in data if isinstance(item, dict))
        else:
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                item = json.loads(line)
                if isinstance(item, dict):
                    result.append(item)
    return result


def dedupe_and_filter(
    candidates: list[dict[str, Any]], cache: dict[str, dict[str, Any]]
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    prepared = []
    seen_candidate_urls: set[str] = set()
    failures: Counter[str] = Counter()
    failure_examples: dict[str, list[dict[str, str]]] = {}

    for candidate in candidates:
        url = clean_string(candidate.get("feed_url") or candidate.get("url") or "")
        key = normalize_url(url)
        if not url or not key:
            continue
        if key in seen_candidate_urls:
            failures["duplicate_candidate_url"] += 1
            continue
        seen_candidate_urls.add(key)

        result = cache.get(key)
        if not result:
            failures["not_validated"] += 1
            continue
        classification = result.get("classification")
        if classification != "valid":
            label = f"{classification}:{result.get('error') or 'unknown'}"
            failures[label] += 1
            if len(failure_examples.setdefault(label, [])) < 3:
                failure_examples[label].append({"url": url, "headline": clean_string(candidate.get("headline", ""))})
            continue

        final_url = clean_string(result.get("final_url", ""))
        if is_substack_url(url) or is_substack_url(final_url):
            failures["substack_duplicate"] += 1
            continue

        headline = choose_headline(candidate, result)
        if not headline:
            failures["missing_headline"] += 1
            continue
        category = normalize_category(candidate.get("category", ""), headline, url)
        language = normalize_language(candidate.get("language", "")) or normalize_language(result.get("language", ""))
        prepared.append(
            {
                "headline": headline,
                "author_name": clean_string(candidate.get("author_name", "")),
                "language": language,
                "feed_url": url,
                "category": category,
                "total_subscribers": "",
                "_priority": int(candidate.get("source_priority", 0)),
                "_source": clean_string(candidate.get("source", "")),
                "_final_url": final_url,
                "_entry_links": result.get("entry_links") or [],
                "_feed_title": clean_string(result.get("feed_title", "")),
            }
        )

    # Prefer curated entries over bulk personal-blog entries.
    prepared.sort(key=lambda item: (-item["_priority"], item["headline"].casefold(), item["feed_url"]))

    kept: list[dict[str, str]] = []
    by_url: dict[str, dict[str, str]] = {}
    by_title: dict[str, list[tuple[dict[str, str], set[str]]]] = {}
    duplicates = Counter()

    for item in prepared:
        url_key = normalize_url(item["feed_url"])
        final_key = normalize_url(item["_final_url"]) if item["_final_url"] else ""
        if url_key in by_url:
            duplicates["duplicate_url"] += 1
            continue
        if final_key and final_key in by_url:
            duplicates["duplicate_final_url"] += 1
            continue

        title_key = clean_headline(item["_feed_title"]).casefold()
        entry_keys = {normalize_url(link) for link in item["_entry_links"] if link}
        duplicate = False
        if title_key and entry_keys:
            for existing, existing_links in by_title.get(title_key, []):
                overlap = entry_keys & existing_links
                # Require two shared recent articles, or an exact one-article
                # match, before treating feeds as the same syndicated source.
                if len(overlap) >= 2 or (len(entry_keys) == 1 and entry_keys == existing_links):
                    duplicate = True
                    break
        if duplicate:
            duplicates["duplicate_content"] += 1
            continue

        by_url[url_key] = item
        if final_key:
            by_url[final_key] = item
        if title_key and entry_keys:
            by_title.setdefault(title_key, []).append((item, entry_keys))
        kept.append(item)

    final_entries = []
    for item in kept:
        final_entries.append(
            {
                "headline": item["headline"],
                "author_name": item["author_name"],
                "language": item["language"],
                "feed_url": item["feed_url"],
                "category": item["category"],
                "total_subscribers": "",
            }
        )

    final_entries.sort(key=lambda item: (item["headline"].casefold(), item["feed_url"].casefold(), item["feed_url"]))
    stats = {
        "candidates_received": len(candidates),
        "validated_candidates": len(prepared),
        "sources": dict(Counter(item["_source"] for item in prepared).most_common()),
        "duplicates_removed": dict(duplicates),
        "failed_candidates": dict(failures.most_common()),
        "failure_examples": failure_examples,
    }
    return final_entries, stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--candidate-file", action="append", required=True, help="JSON/JSONL candidate file (repeatable)")
    parser.add_argument("--cache", default=".feed_validation_cache.json", help="validator cache")
    parser.add_argument("--output", default="other_feeds_v2.json", help="final six-field JSON")
    parser.add_argument("--stats", default="feed_directory_stats.json", help="build statistics JSON")
    parser.add_argument("--target", type=int, default=5148, help="minimum acceptable feed count")
    args = parser.parse_args()

    cache = json.loads(Path(args.cache).read_text(encoding="utf-8"))
    candidates = load_candidates(args.candidate_file)
    entries, stats = dedupe_and_filter(candidates, cache)
    dump_json(Path(args.output), entries)

    stats.update(
        {
            "final_feed_count": len(entries),
            "target": args.target,
            "target_reached": len(entries) >= args.target,
            "categories": dict(Counter(item["category"] for item in entries).most_common()),
            "languages": dict(Counter(item["language"] or "(unknown)" for item in entries).most_common()),
        }
    )
    dump_json(Path(args.stats), stats)

    print(f"wrote {len(entries)} feeds to {args.output}")
    print(f"target {args.target}: {'reached' if len(entries) >= args.target else 'NOT reached'}")
    print(json.dumps(stats["duplicates_removed"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
