"""Tests for feed validation, normalisation, and the final directory."""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

from build_other_feeds_v2 import dedupe_and_filter
from feed_utils import normalize_language, normalize_url
from validate_other_feeds import HostRateLimiter, parse_feed_bytes, validate_one

ROOT = Path(__file__).resolve().parents[1]

RSS_DOCUMENT = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Example RSS</title>
  <link>http://example.com/</link>
  <description>An example feed</description>
  <language>en-US</language>
  <item><title>First post</title><link>http://example.com/1</link></item>
  <item><title>Second post</title><link>http://example.com/2</link></item>
</channel></rss>"""

ATOM_DOCUMENT = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Example Atom</title>
  <link href="http://example.com/"/>
  <updated>2026-10-20T12:00:00Z</updated>
  <entry><title>Atom post</title><link href="http://example.com/atom-1"/><updated>2026-10-20T12:00:00Z</updated></entry>
</feed>"""

EMPTY_RSS = b"""<?xml version="1.0"?>
<rss version="2.0"><channel><title>Empty</title><link>http://example.com/</link><description>Empty</description></channel></rss>"""

HTML_DOCUMENT = b"""<!DOCTYPE html><html><head><title>Not a feed</title></head><body>Hello</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802 - required by BaseHTTPRequestHandler
        routes = {
            "/rss.xml": (200, "application/rss+xml", RSS_DOCUMENT),
            "/atom.xml": (200, "application/atom+xml", ATOM_DOCUMENT),
            "/empty.xml": (200, "application/rss+xml", EMPTY_RSS),
            "/html": (200, "text/html", HTML_DOCUMENT),
            "/missing": (404, "text/html", b"not found"),
        }
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/rss.xml")
            self.end_headers()
            return
        if self.path == "/loop":
            self.send_response(302)
            self.send_header("Location", "/loop")
            self.end_headers()
            return
        status, content_type, body = routes.get(self.path, (404, "text/plain", b"unknown"))
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # keep test output quiet
        pass


@pytest.fixture(scope="module")
def feed_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    yield base
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _validate(base_url: str, path: str):
    async def run():
        async with httpx.AsyncClient(follow_redirects=True, max_redirects=5) as client:
            return await validate_one(
                client, HostRateLimiter(0), base_url + path, timeout=5.0, retries=0
            )

    return asyncio.run(run())


def test_parse_valid_rss():
    parsed = parse_feed_bytes(RSS_DOCUMENT, "application/rss+xml")
    assert parsed["classification"] == "valid"
    assert parsed["feed_title"] == "Example RSS"
    assert parsed["entry_count"] == 2
    assert parsed["language"] == "en"


def test_parse_valid_atom():
    parsed = parse_feed_bytes(ATOM_DOCUMENT, "application/atom+xml")
    assert parsed["classification"] == "valid"
    assert parsed["feed_title"] == "Example Atom"
    assert parsed["entry_count"] == 1


def test_parse_html_is_rejected():
    parsed = parse_feed_bytes(HTML_DOCUMENT, "text/html")
    assert parsed["classification"] == "invalid"
    assert parsed["error"] == "html_not_feed"


def test_parse_empty_feed_is_rejected():
    parsed = parse_feed_bytes(EMPTY_RSS, "application/rss+xml")
    assert parsed["classification"] == "invalid"
    assert parsed["error"] == "no_usable_entries"


def test_parse_garbage_is_rejected():
    parsed = parse_feed_bytes(b'{"error": "not a feed"}', "application/json")
    assert parsed["classification"] == "invalid"


def test_http_rss_is_valid(feed_server):
    result = _validate(feed_server, "/rss.xml")
    assert result.classification == "valid"
    assert result.http_status == 200
    assert result.feed_title == "Example RSS"


def test_redirect_to_rss_is_valid(feed_server):
    result = _validate(feed_server, "/redirect")
    assert result.classification == "valid"
    assert result.redirects == 1
    assert result.final_url.endswith("/rss.xml")


def test_html_response_is_invalid(feed_server):
    result = _validate(feed_server, "/html")
    assert result.classification == "invalid"
    assert result.error == "html_not_feed"


def test_missing_feed_is_invalid(feed_server):
    result = _validate(feed_server, "/missing")
    assert result.classification == "invalid"
    assert result.error == "http_404"


def test_redirect_loop_is_inconclusive(feed_server):
    result = _validate(feed_server, "/loop")
    assert result.classification == "inconclusive"


def test_url_normalisation():
    assert normalize_url("HTTP://Example.COM:80/feed/?utm_source=x") == "http://example.com/feed"
    assert normalize_url("https://example.com/feed/") == normalize_url("https://example.com/feed")
    assert normalize_url("https://example.com/Foo/Bar/") == "https://example.com/Foo/Bar"
    assert normalize_url("https://example.com/feed?b=2&a=1") == "https://example.com/feed?a=1&b=2"
    assert normalize_url("https://example.com/feed#fragment") == "https://example.com/feed"


def test_language_normalisation():
    assert normalize_language("en_US") == "en"
    assert normalize_language("zh-CN") == "zh"
    assert normalize_language("utf-8") == ""
    assert normalize_language("") == ""


def test_builder_deduplicates_syndicated_copies():
    url_a = "https://a.example/feed"
    url_b = "https://b.example/feed"
    cache = {
        normalize_url(url_a): {
            "classification": "valid",
            "feed_title": "Same Publication",
            "entry_links": ["https://example.com/one", "https://example.com/two"],
            "final_url": url_a,
        },
        normalize_url(url_b): {
            "classification": "valid",
            "feed_title": "Same Publication",
            "entry_links": ["https://example.com/one", "https://example.com/two"],
            "final_url": url_b,
        },
    }
    candidates = [
        {"feed_url": url_a, "headline": "Same Publication", "source_priority": 90},
        {"feed_url": url_b, "headline": "Same Publication", "source_priority": 10},
    ]
    entries, stats = dedupe_and_filter(candidates, cache)
    assert len(entries) == 1
    assert stats["duplicates_removed"].get("duplicate_content") == 1


def test_builder_keeps_distinct_category_feeds():
    cache = {}
    candidates = []
    for index, (url, title, link) in enumerate(
        [
            ("https://site.example/news/feed", "Site News", "https://site.example/news/1"),
            ("https://site.example/tech/feed", "Site Tech", "https://site.example/tech/1"),
        ]
    ):
        cache[normalize_url(url)] = {
            "classification": "valid",
            "feed_title": title,
            "entry_links": [link],
            "final_url": url,
        }
        candidates.append({"feed_url": url, "headline": title, "source_priority": 50})
    entries, _ = dedupe_and_filter(candidates, cache)
    assert len(entries) == 2


def test_final_directory_schema_and_counts():
    data = json.loads((ROOT / "other_feeds_v2.json").read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) >= 5148

    expected_keys = {
        "headline",
        "author_name",
        "language",
        "feed_url",
        "category",
        "total_subscribers",
    }
    seen = set()
    for item in data:
        assert set(item) == expected_keys
        assert all(isinstance(value, str) for value in item.values())
        assert item["headline"].strip()
        assert item["category"].strip()
        assert item["feed_url"].startswith(("http://", "https://"))
        assert item["total_subscribers"] == ""
        key = normalize_url(item["feed_url"])
        assert key not in seen, f"duplicate normalised URL: {item['feed_url']}"
        seen.add(key)


def test_this_week_in_gnome_is_present():
    data = json.loads((ROOT / "other_feeds_v2.json").read_text(encoding="utf-8"))
    matches = [item for item in data if item["feed_url"] == "https://thisweek.gnome.org/index.xml"]
    assert len(matches) == 1
    assert matches[0]["headline"] == "This Week in GNOME"


def test_original_file_is_unchanged():
    original = (ROOT / "other_feeds.json").read_bytes()
    assert len(json.loads(original)) == 107
    assert hashlib.sha256(original).hexdigest() == (
        "e8cfea1f3dbbf2e6b910e0701a63c8299c87258bce88617026e040002c1fc275"
    )
