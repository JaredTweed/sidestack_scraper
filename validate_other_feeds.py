#!/usr/bin/env python3
"""Validate RSS/Atom feeds used by ``other_feeds_v2.json``.

The validator performs a real HTTP request for every feed, follows redirects,
parses the response with feedparser, and records whether the endpoint actually
returned a usable feed. It never rewrites the input directory; results go to a
separate JSON report (and optionally an incremental cache).

Examples
--------
Validate the whole directory::

    python validate_other_feeds.py --input other_feeds_v2.json

Validate only feeds that are new compared with the original v1 file::

    python validate_other_feeds.py --input other_feeds_v2.json \
        --baseline other_feeds.json --only-new

Re-check everything, ignoring the cache::

    python validate_other_feeds.py --force --concurrency 96

Exit codes: 0 = no invalid feeds, 1 = at least one invalid feed,
2 = only inconclusive results (blocked/timeout/rate-limited).
"""

from __future__ import annotations

import argparse
import asyncio
import calendar
import json
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import feedparser
import httpx

from feed_utils import clean_string, dump_json, host_of, load_json, normalize_language, normalize_url

try:  # pragma: no cover - exercised implicitly when the package is present
    from defusedxml import ElementTree as DefusedElementTree
except ImportError:  # pragma: no cover
    DefusedElementTree = None


USER_AGENT = (
    "sidestack-feed-validator/1.0 "
    "(+https://github.com/JaredTweed/sidestack_scraper; feed validation)"
)
MAX_BODY_BYTES = 12 * 1024 * 1024
FEED_TAGS = (b"<rss", b"<feed", b"<rdf:rdf", b"<rdf", b"<channel")


@dataclass
class ValidationResult:
    url: str
    final_url: str = ""
    classification: str = "inconclusive"  # valid | invalid | inconclusive
    http_status: int = 0
    content_type: str = ""
    feed_title: str = ""
    feed_link: str = ""
    language: str = ""
    entry_count: int = 0
    latest_entry: str = ""
    strict_xml: bool = False
    redirects: int = 0
    elapsed_ms: int = 0
    checked_at: str = ""
    error: str = ""
    entry_links: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.classification == "valid"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _entry_timestamp(entry: Any) -> Optional[int]:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        value = entry.get(key)
        if value:
            try:
                return calendar.timegm(value)
            except (TypeError, ValueError, OverflowError):
                continue
    return None


def _is_html_document(content: bytes) -> bool:
    sniff = content[:2048].lstrip().lower()
    return sniff.startswith(b"<!doctype html") or sniff.startswith(b"<html")


def parse_feed_bytes(content: bytes, content_type: str = "") -> dict[str, Any]:
    """Parse a feed response without doing any network I/O.

    Returns a dictionary with ``classification``, ``error`` and metadata. This
    is intentionally usable by tests and by future import tooling.
    """

    result: dict[str, Any] = {
        "classification": "invalid",
        "error": "",
        "feed_title": "",
        "feed_link": "",
        "language": "",
        "entry_count": 0,
        "latest_entry": "",
        "strict_xml": False,
        "entry_links": [],
    }

    if not content or len(content) < 32:
        result["error"] = "empty_or_tiny_response"
        return result
    if len(content) > MAX_BODY_BYTES:
        result["error"] = "response_too_large"
        return result
    if _is_html_document(content):
        result["error"] = "html_not_feed"
        return result

    if DefusedElementTree is not None:
        try:
            DefusedElementTree.fromstring(content)
            result["strict_xml"] = True
        except Exception:
            # Many real-world feeds are slightly malformed. feedparser is
            # tolerant, so a strict parse failure is not by itself fatal.
            result["strict_xml"] = False

    try:
        parsed = feedparser.parse(content)
    except Exception as exc:  # feedparser is usually forgiving, but be safe
        result["error"] = f"parser_error:{type(exc).__name__}"
        return result

    version = clean_string(parsed.get("version", ""))
    header = content[:16384].lower()
    has_feed_tag = any(tag in header for tag in FEED_TAGS)
    if not version and not has_feed_tag:
        result["error"] = "not_rss_or_atom"
        return result
    if version and not (
        version.startswith("rss")
        or version.startswith("atom")
        or version.startswith("rdf")
    ):
        result["error"] = f"unsupported_format:{version}"
        return result

    entries = list(parsed.entries or [])
    usable_entries = []
    entry_links: list[str] = []
    latest: Optional[int] = None
    for entry in entries:
        title = clean_string(entry.get("title", ""))
        link = clean_string(entry.get("link", ""))
        if not title and not link:
            continue
        usable_entries.append(entry)
        if link:
            entry_links.append(link)
        timestamp = _entry_timestamp(entry)
        if timestamp and (latest is None or timestamp > latest):
            latest = timestamp

    if not usable_entries:
        result["error"] = "no_usable_entries"
        return result

    feed = parsed.get("feed", {})
    result.update(
        classification="valid",
        feed_title=clean_string(feed.get("title", "")),
        feed_link=clean_string(feed.get("link", "")),
        language=normalize_language(clean_string(feed.get("language", ""))),
        entry_count=len(entries),
        latest_entry=(
            datetime.fromtimestamp(latest, tz=timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
            if latest
            else ""
        ),
        entry_links=entry_links[:3],
    )
    if not result["feed_title"]:
        result["feed_title"] = clean_string(feed.get("subtitle", ""))
    return result


class HostRateLimiter:
    """Serialize request starts per host while allowing global concurrency."""

    def __init__(self, min_interval: float) -> None:
        self.min_interval = max(0.0, min_interval)
        self._locks: dict[str, asyncio.Lock] = {}
        self._last: dict[str, float] = {}

    async def wait(self, host: str) -> None:
        if self.min_interval <= 0 or not host:
            return
        lock = self._locks.setdefault(host, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            last = self._last.get(host, 0.0)
            delay = last + self.min_interval - now
            if delay > 0:
                await asyncio.sleep(delay)
            self._last[host] = time.monotonic()


def _retryable(result: ValidationResult) -> bool:
    if result.classification != "inconclusive":
        return False
    return result.error.startswith(
        (
            "timeout",
            "network_error",
            "http_429",
            "http_5",
            "too_many_redirects",
            "tls_error",
            "read_error",
        )
    )


def _retry_after_seconds(response: httpx.Response) -> float:
    value = response.headers.get("retry-after", "").strip()
    if not value:
        return 0.0
    try:
        return min(30.0, max(0.0, float(value)))
    except ValueError:
        return 0.0


async def validate_one(
    client: httpx.AsyncClient,
    limiter: HostRateLimiter,
    url: str,
    timeout: float,
    retries: int,
) -> ValidationResult:
    started = time.monotonic()
    result = ValidationResult(url=url, checked_at=iso_now())
    attempts = max(1, retries + 1)

    for attempt in range(attempts):
        await limiter.wait(host_of(url))
        retry_after = 0.0
        try:
            response = await client.get(url)
            result.http_status = response.status_code
            result.final_url = str(response.url)
            result.content_type = response.headers.get("content-type", "")
            result.redirects = len(response.history)
            body = response.content

            if 200 <= response.status_code < 300:
                parsed = parse_feed_bytes(body, result.content_type)
                result.classification = parsed["classification"]
                result.error = parsed["error"]
                result.feed_title = parsed["feed_title"]
                result.feed_link = parsed["feed_link"]
                result.language = parsed["language"] or result.language
                result.entry_count = parsed["entry_count"]
                result.latest_entry = parsed["latest_entry"]
                result.strict_xml = parsed["strict_xml"]
                result.entry_links = parsed["entry_links"]
                if result.classification == "valid":
                    break
                # A 200 response that is not a feed is a permanent problem.
                break

            if response.status_code in (401, 403, 406, 429):
                result.classification = "inconclusive"
                result.error = f"http_{response.status_code}"
                retry_after = _retry_after_seconds(response)
            elif response.status_code in (404, 410):
                result.classification = "invalid"
                result.error = f"http_{response.status_code}"
                break
            elif 400 <= response.status_code < 500:
                result.classification = "invalid"
                result.error = f"http_{response.status_code}"
                break
            else:
                result.classification = "inconclusive"
                result.error = f"http_{response.status_code}"
                retry_after = _retry_after_seconds(response)
        except httpx.TooManyRedirects:
            result.classification = "inconclusive"
            result.error = "too_many_redirects"
        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout):
            result.classification = "inconclusive"
            result.error = "timeout"
        except httpx.ConnectError as exc:
            message = str(exc).lower()
            if any(
                marker in message
                for marker in (
                    "name or service not known",
                    "nodename nor servname",
                    "no address associated with hostname",
                    "name does not resolve",
                )
            ):
                result.classification = "invalid"
                result.error = "dns_error"
                break
            result.classification = "inconclusive"
            result.error = "network_error"
        except httpx.ReadError as exc:
            result.classification = "inconclusive"
            result.error = f"read_error:{type(exc).__name__}"
        except httpx.HTTPError as exc:
            result.classification = "inconclusive"
            result.error = f"http_error:{type(exc).__name__}"
        except Exception as exc:  # defensive: never lose a whole batch
            result.classification = "inconclusive"
            result.error = f"unexpected:{type(exc).__name__}"

        if attempt + 1 < attempts and _retryable(result):
            backoff = retry_after or min(20.0, 1.5 * (2**attempt))
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            continue
        break

    result.elapsed_ms = int((time.monotonic() - started) * 1000)
    return result


def cache_is_fresh(result: dict[str, Any], max_age_hours: float) -> bool:
    checked = parse_iso(str(result.get("checked_at", "")))
    if checked is None:
        return False
    age = utc_now() - checked
    classification = result.get("classification")
    # Inconclusive results are cheap to retry and often temporary.
    limit = timedelta(hours=min(max_age_hours, 6) if classification == "inconclusive" else max_age_hours)
    return age <= limit


def _load_input(path: Path) -> list[dict[str, Any]]:
    data = load_json(path)
    if isinstance(data, dict) and isinstance(data.get("feeds"), list):
        data = data["feeds"]
    if not isinstance(data, list):
        raise SystemExit(f"{path} must contain a JSON array of feed objects")
    entries = []
    for item in data:
        if not isinstance(item, dict):
            continue
        url = clean_string(item.get("feed_url") or item.get("url") or "")
        if not url:
            continue
        entry = dict(item)
        entry["feed_url"] = url
        entries.append(entry)
    return entries


def _urls(path: Optional[Path]) -> set[str]:
    if not path or not path.exists():
        return set()
    return {normalize_url(item["feed_url"]) for item in _load_input(path)}


async def run(args: argparse.Namespace) -> int:
    input_path = Path(args.input)
    entries = _load_input(input_path)
    if args.only_new:
        baseline = _urls(Path(args.baseline) if args.baseline else None)
        if not baseline:
            raise SystemExit("--only-new requires --baseline with existing feed URLs")
        entries = [e for e in entries if normalize_url(e["feed_url"]) not in baseline]

    # Deduplicate by normalised URL while preserving input order.
    seen: set[str] = set()
    unique_entries = []
    for entry in entries:
        key = normalize_url(entry["feed_url"])
        if not key or key in seen:
            continue
        seen.add(key)
        unique_entries.append(entry)

    if args.limit:
        unique_entries = unique_entries[: args.limit]

    cache_path = Path(args.cache) if args.cache else None
    cache: dict[str, dict[str, Any]] = {}
    if cache_path and cache_path.exists():
        try:
            loaded = load_json(cache_path)
            if isinstance(loaded, dict):
                cache = loaded
        except (json.JSONDecodeError, OSError):
            cache = {}

    results: dict[str, ValidationResult] = {}
    pending: list[dict[str, Any]] = []
    fresh = 0
    for entry in unique_entries:
        key = normalize_url(entry["feed_url"])
        cached = cache.get(key)
        if cached and not args.force and cache_is_fresh(cached, args.max_age_hours):
            try:
                results[key] = ValidationResult(**cached)
                fresh += 1
                continue
            except TypeError:
                pass
        pending.append(entry)

    if not args.quiet:
        print(
            f"{len(unique_entries)} unique feeds; {fresh} fresh cached; "
            f"{len(pending)} to check",
            file=sys.stderr,
        )

    limiter = HostRateLimiter(args.min_interval)
    semaphore = asyncio.Semaphore(max(1, args.concurrency))
    timeout = httpx.Timeout(args.timeout, connect=min(10.0, args.timeout))
    limits = httpx.Limits(
        max_connections=max(10, args.concurrency * 2),
        max_keepalive_connections=max(10, args.concurrency),
    )

    async with httpx.AsyncClient(
        follow_redirects=True,
        max_redirects=10,
        timeout=timeout,
        limits=limits,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*;q=0.5",
            "Accept-Language": "en,en-US;q=0.8,*;q=0.5",
        },
    ) as client:
        async def worker(entry: dict[str, Any]) -> ValidationResult:
            key = normalize_url(entry["feed_url"])
            async with semaphore:
                result = await validate_one(
                    client, limiter, entry["feed_url"], timeout=args.timeout, retries=args.retries
                )
            return result

        batch_size = max(1, args.batch_size)
        for start in range(0, len(pending), batch_size):
            batch = pending[start : start + batch_size]
            batch_results = await asyncio.gather(*(worker(entry) for entry in batch))
            for result in batch_results:
                key = normalize_url(result.url)
                results[key] = result
                cache[key] = asdict(result)
            if cache_path:
                dump_json(cache_path, cache)
            if not args.quiet:
                done = min(start + len(batch), len(pending))
                print(f"checked {done}/{len(pending)}", file=sys.stderr)

    ordered = []
    for entry in unique_entries:
        key = normalize_url(entry["feed_url"])
        result = results.get(key)
        if result is None:
            continue
        record = asdict(result)
        # Entry links are useful in the incremental cache (for duplicate
        # detection) but not in the human-facing validation report.
        record.pop("entry_links", None)
        record["input_url"] = entry["feed_url"]
        record["headline"] = clean_string(entry.get("headline", ""))
        ordered.append(record)

    valid = sum(1 for r in ordered if r["classification"] == "valid")
    invalid = sum(1 for r in ordered if r["classification"] == "invalid")
    inconclusive = sum(1 for r in ordered if r["classification"] == "inconclusive")
    report = {
        "generated_at": iso_now(),
        "input": str(input_path),
        "settings": {
            "concurrency": args.concurrency,
            "timeout_seconds": args.timeout,
            "retries": args.retries,
            "min_interval_seconds": args.min_interval,
            "max_age_hours": args.max_age_hours,
            "only_new": args.only_new,
            "baseline": args.baseline,
        },
        "summary": {
            "total": len(ordered),
            "valid": valid,
            "invalid": invalid,
            "inconclusive": inconclusive,
            "fresh_cache_hits": fresh,
        },
        "results": sorted(ordered, key=lambda r: (r["classification"], r["input_url"].lower())),
    }
    dump_json(Path(args.output), report)

    if not args.quiet:
        print(
            f"valid={valid} invalid={invalid} inconclusive={inconclusive} "
            f"report={args.output}",
            file=sys.stderr,
        )

    if invalid:
        return 1
    if inconclusive:
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default="other_feeds_v2.json", help="feed directory JSON to validate")
    parser.add_argument("--output", default="validation_report.json", help="machine-readable report path")
    parser.add_argument("--cache", default=".feed_validation_cache.json", help="incremental cache path (empty to disable)")
    parser.add_argument("--baseline", default="other_feeds.json", help="baseline used by --only-new")
    parser.add_argument("--only-new", action="store_true", help="validate only URLs absent from --baseline")
    parser.add_argument("--concurrency", type=int, default=48, help="maximum simultaneous HTTP requests")
    parser.add_argument("--timeout", type=float, default=20.0, help="per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=2, help="retries for temporary failures")
    parser.add_argument("--min-interval", type=float, default=0.35, help="minimum seconds between requests to one host")
    parser.add_argument("--max-age-hours", type=float, default=168.0, help="reuse cached results younger than this")
    parser.add_argument("--batch-size", type=int, default=100, help="cache write / progress batch size")
    parser.add_argument("--limit", type=int, default=0, help="validate at most N unique feeds (debugging)")
    parser.add_argument("--force", action="store_true", help="ignore fresh cached results")
    parser.add_argument("--quiet", action="store_true", help="suppress progress output")
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
