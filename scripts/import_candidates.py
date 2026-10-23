#!/usr/bin/env python3
"""Extract candidate feed URLs from the downloaded source collections.

Expected layout (created by ``scripts/fetch_candidate_sources.py``)::

    candidate_sources/
      awesome-rss-feeds/
      tidings-rss/
      smallweb/
      fountain-of-rss/
      kagi_opml.xml
      ooh_opml/*.opml

The output is JSON Lines with the metadata used by
``build_other_feeds_v2.py``. Candidates are not verified here; run
``validate_other_feeds.py`` next.
"""

from __future__ import annotations

import argparse
import csv
import json
import pathlib
import re
import sys
from collections import Counter

from lxml import etree

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from feed_utils import clean_string, host_of, is_substack_url, normalize_language, normalize_url  # noqa: E402

CAT_MAP = {
    "arts": "Arts & Culture",
    "technology": "Technology",
    "countries": "International News",
    "economics": "Business & Economics",
    "education": "Education",
    "politics": "News & Politics",
    "humanities": "Humanities",
    "personal": "Personal Blogs",
    "recreation": "Lifestyle",
    "science": "Science",
    "society": "Society",
    "uncategorizable": "General",
}

RECOMMENDED_MAP = {
    "Tech": "Technology",
    "Programming": "Programming",
    "Science": "Science",
    "Space": "Science",
    "News": "News & Politics",
    "Startups": "Business & Economics",
    "Personal finance": "Business & Economics",
    "Business & Economy": "Business & Economics",
    "Health": "Health & Medicine",
    "Environment": "Environment",
    "Design": "Design",
    "Photography": "Arts & Culture",
    "Music": "Arts & Culture",
    "Movies": "Arts & Culture",
    "Television": "Arts & Culture",
    "Books": "Arts & Culture",
    "History": "History",
    "Travel": "Travel",
    "Food": "Food & Drink",
    "Sports": "Sports",
    "Gaming": "Gaming",
    "Android Development": "Technology",
    "iOS Development": "Technology",
    "Web Development": "Programming",
    "UI - UX": "Design",
    "Linux": "Linux & Open Source",
    "Education": "Education",
    "Politics": "News & Politics",
    "World News": "International News",
    "Canada": "Canadian News",
}

TIDINGS_MAP = {
    "Engineering & Technology": "Technology",
    "Videos": "Video",
    "Podcasts": "Podcasts",
    "Personal Blogs": "Personal Blogs",
    "Artificial Intelligence": "Artificial Intelligence",
    "News": "News & Politics",
    "Research & Science": "Science",
    "Communities": "Community",
    "Technology Media": "Technology",
    "Culture & Ideas": "Arts & Culture",
    "Tech Newsletters & Weeklies": "Technology",
    "Security": "Security",
    "Product & Design": "Design",
    "Business & Startups": "Business & Economics",
}

FOUNTAIN_BLACKLIST = (
    "porn", "xxx", "nsfw", "onlyfans", "camgirl", "escort", "hentai",
    "xvideos", "xhamster", "pornhub", "sex", "nude", "fetish", "dating",
)


def fountain_category(tags: list[str], title: str, url: str) -> str:
    text = f" {' '.join(tags)} {title} {url} ".lower()
    rules = [
        ("Artificial Intelligence", (" ai ", "artificial intelligence", "machine learning", "llm", "openai", "deep learning")),
        ("Linux & Open Source", ("linux", "gnome", "kde", "ubuntu", "debian", "fedora", "arch linux", "open source", "opensource", "floss")),
        ("Programming", ("programming", "developer", "software", "coding", "python", "rust", "javascript", "database", "devops", "github")),
        ("Security", ("security", "infosec", "cybersecurity", "privacy", "malware")),
        ("Science", ("science", "physics", "astronomy", "space", "biology", "chemistry", "research", "mathematics")),
        ("Health & Medicine", ("health", "medicine", "medical", "covid", "vaccine", "doctor", "hospital")),
        ("Climate & Energy", ("climate", "energy", "solar", "renewable", "battery", "electric vehicle", "heat pump", "environment")),
        ("Business & Economics", ("business", "economy", "economics", "finance", "market", "startup", "investing")),
        ("Urbanism & Transport", ("urbanism", "transit", "transport", "cycling", "bicycle", "railway", "rail", "city", "infrastructure")),
        ("Canadian News", ("canada", "canadian", "toronto", "vancouver", "ontario", "quebec", "alberta")),
        ("News & Politics", ("news", "politics", "world", "international", "government", "election")),
        ("Arts & Culture", ("art", "music", "film", "movie", "book", "culture", "photography", "design", "theatre", "literature")),
        ("Food & Drink", ("food", "recipe", "cooking", "wine", "beer", "restaurant", "coffee")),
        ("Travel", ("travel", "tourism", "hotel", "flight")),
        ("Sports", ("sport", "football", "soccer", "baseball", "basketball", "hockey", "cricket", "tennis")),
        ("Technology", ("tech", "gadget", "computer", "mobile", "apple", "google", "microsoft", "internet")),
    ]
    for category, needles in rules:
        if any(needle in text for needle in needles):
            return category
    return "General"


def safe_parse(path: pathlib.Path):
    try:
        return etree.parse(str(path))
    except etree.XMLSyntaxError:
        parser = etree.XMLParser(recover=True, resolve_entities=False)
        return etree.parse(str(path), parser)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", default="candidate_sources")
    parser.add_argument("--repo-dir", default=".")
    parser.add_argument("--output", default="candidates/all.jsonl")
    parser.add_argument("--no-substack-filter", action="store_true")
    args = parser.parse_args()

    source_dir = pathlib.Path(args.source_dir)
    repo_dir = pathlib.Path(args.repo_dir)
    candidates: dict[str, dict] = {}
    order: list[str] = []

    def add(url, headline="", author="", language="", category="", source="", priority=0,
            html="", tags=None, last_item="", entry_count=0, source_category=""):
        url = clean_string(url)
        if not url or not re.match(r"^https?://", url, re.I):
            return
        key = normalize_url(url)
        if not key or is_substack_url(url):
            return
        if key in candidates:
            item = candidates[key]
            if priority > item["source_priority"]:
                item["source_priority"] = priority
                item["source"] = source
            for field, value in (
                ("headline", headline), ("author_name", author), ("language", language),
                ("category", category), ("html_url", html), ("last_item", last_item),
            ):
                if value and not item.get(field):
                    item[field] = value
            if tags:
                item["tags"] = sorted(set(item.get("tags", [])) | set(tags))
            if entry_count and not item.get("entry_count"):
                item["entry_count"] = entry_count
            if source_category and not item.get("source_category"):
                item["source_category"] = source_category
            return
        candidates[key] = {
            "feed_url": url,
            "headline": clean_string(headline),
            "author_name": clean_string(author),
            "language": normalize_language(language),
            "category": clean_string(category),
            "total_subscribers": "",
            "source": source,
            "source_priority": priority,
            "html_url": clean_string(html),
            "tags": sorted(set(tags or [])),
            "last_item": last_item,
            "entry_count": int(entry_count or 0),
            "source_category": clean_string(source_category),
        }
        order.append(key)

    for filename, priority, source_name in (
        ("other_feeds_v2.json", 100, "existing-v2"),
        ("other_feeds.json", 95, "existing-v1"),
    ):
        path = repo_dir / filename
        if not path.exists():
            continue
        for item in json.loads(path.read_text(encoding="utf-8")):
            add(item.get("feed_url"), item.get("headline"), item.get("author_name"),
                item.get("language"), item.get("category"), source_name, priority)

    ooh_dir = source_dir / "ooh_opml"
    for path in sorted(ooh_dir.glob("*.opml")) if ooh_dir.exists() else []:
        match = re.match(r"https:__ooh\.directory_blogs_(.*)", path.stem)
        cat_path = match.group(1).replace("_", "/") if match else ""
        top = cat_path.split("/", 1)[0] if cat_path else ""
        category = CAT_MAP.get(top, "General")
        country = cat_path.split("/")[1] if top == "countries" and "/" in cat_path else ""
        if country == "canada":
            category = "Canadian News"
        elif country == "usa":
            category = "US News"
        elif country == "uk":
            category = "UK News"
        try:
            root = safe_parse(path)
        except Exception as exc:
            print(f"warning: cannot parse {path}: {exc}")
            continue
        for outline in root.iter("outline"):
            url = outline.get("xmlUrl") or outline.get("xmlurl")
            if url:
                add(url, outline.get("title") or outline.get("text") or "", "", "en",
                    category, "ooh.directory", 80, outline.get("htmlUrl") or "",
                    source_category=cat_path)

    awesome_dir = source_dir / "awesome-rss-feeds"
    if awesome_dir.exists():
        for path in awesome_dir.rglob("*.opml"):
            if "/without_category/" in str(path):
                continue
            rel = str(path)
            country = ""
            category = "General"
            match = re.search(r"/countries/with_category/(.+)\.opml$", rel)
            if match:
                country = match.group(1)
                category = "Canadian News" if country == "Canada" else (
                    "US News" if country == "United States" else "International News"
                )
            else:
                match = re.search(r"/recommended/with_category/(.+)\.opml$", rel)
                if match:
                    category = RECOMMENDED_MAP.get(match.group(1), "General")
            try:
                root = safe_parse(path)
            except Exception as exc:
                print(f"warning: cannot parse {path}: {exc}")
                continue
            for outline in root.iter("outline"):
                url = outline.get("xmlUrl") or outline.get("xmlurl")
                if url:
                    add(url, outline.get("title") or outline.get("text") or "", "", "",
                        category, "awesome-rss-feeds", 75, outline.get("htmlUrl") or "",
                        source_category=country or category)

    tidings = source_dir / "tidings-rss" / "data" / "feeds.json"
    if tidings.exists():
        data = json.loads(tidings.read_text(encoding="utf-8"))
        for item in data.get("feeds", []):
            add(item.get("feed_url"), item.get("title"), "", item.get("language"),
                TIDINGS_MAP.get(item.get("category"), "General"), "tidings-rss", 85,
                item.get("site_url") or "", tags=item.get("packs") or [],
                last_item=item.get("latest_item_at") or "",
                source_category=item.get("category") or "")

    kagi = source_dir / "kagi_opml.xml"
    if kagi.exists():
        for outline in safe_parse(kagi).iter("outline"):
            url = outline.get("xmlUrl") or outline.get("xmlurl")
            if url:
                add(url, outline.get("title") or outline.get("text") or "", "", "en",
                    "Personal Blogs", "kagi-smallweb", 60,
                    outline.get("htmlUrl") or "", source_category="personal")

    fountain = source_dir / "fountain-of-rss" / "feeds.tsv"
    if fountain.exists():
        with fountain.open(encoding="utf-8", errors="replace") as handle:
            for row in csv.reader(handle, delimiter="\t"):
                if len(row) < 15:
                    continue
                (url, language, title, subtitle, site, tags_json, _upd, _updf,
                 _updated_s, _updated_ts, _latest_s, latest_ts, entry_count,
                 status, _fetched) = row[:15]
                if status != "200":
                    continue
                try:
                    count = int(entry_count or 0)
                except ValueError:
                    count = 0
                if count < 1 or not title.strip():
                    continue
                lower = f"{url} {title} {subtitle}".lower()
                if any(word in lower for word in FOUNTAIN_BLACKLIST):
                    continue
                if re.search(r"/(comments?|feed/comments)(/|$)|[?&]feed=comments", url, re.I):
                    continue
                try:
                    tags = json.loads(tags_json) if tags_json else []
                except json.JSONDecodeError:
                    tags = []
                if not isinstance(tags, list):
                    tags = []
                category = fountain_category([str(tag) for tag in tags], title, url)
                add(url, title, "", language, category, "fountain-of-rss", 50, site,
                    tags=tags, last_item=latest_ts, entry_count=count,
                    source_category=category)

    for url, title, language, category in (
        ("https://thisweek.gnome.org/index.xml", "This Week in GNOME", "en", "Linux & Open Source"),
        ("https://blogs.gnome.org/feed/", "GNOME Blogs", "en", "Linux & Open Source"),
        ("https://planet.gnome.org/atom.xml", "Planet GNOME", "en", "Linux & Open Source"),
        ("https://blog.gtk.org/feed/", "GTK Development Blog", "en", "Linux & Open Source"),
        ("https://foundation.gnome.org/feed/", "GNOME Foundation", "en", "Linux & Open Source"),
    ):
        add(url, title, "", language, category, "manual-gnome", 90)

    if not args.no_substack_filter:
        sub_urls: set[str] = set()
        sub_hosts: set[str] = set()
        sub_titles: set[str] = set()
        feeds_path = repo_dir / "feeds.json"
        if feeds_path.exists():
            for item in json.loads(feeds_path.read_text(encoding="utf-8")):
                substack = item.get("substack", {})
                if not isinstance(substack, dict):
                    continue
                details = substack.get("details", {})
                for key in ("feed_verified_url", "feed_url", "alternate_feed_url"):
                    url = substack.get(key)
                    if url:
                        sub_urls.add(normalize_url(url))
                        sub_hosts.add(host_of(url))
                headline = details.get("headline") if isinstance(details, dict) else None
                if isinstance(headline, str) and headline.strip():
                    sub_titles.add(" ".join(headline.lower().split()))

        filtered = []
        for key in order:
            item = candidates[key]
            if key in sub_urls or host_of(item["feed_url"]) in sub_hosts:
                continue
            headline = " ".join(item["headline"].lower().split())
            if headline and headline in sub_titles and item["source"] not in ("existing-v1", "existing-v2"):
                continue
            filtered.append(item)
    else:
        filtered = [candidates[key] for key in order]

    output = pathlib.Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for item in filtered:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(f"wrote {len(filtered)} candidates to {output}")
    print("by source:", dict(Counter(item["source"] for item in filtered).most_common()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
