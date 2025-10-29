#!/usr/bin/env python3
"""
Sidestack Substack directory scraper
------------------------------------

Scrapes https://sidestack.io/directory/all/<LETTER> pages to collect Substack
detail pages, then resolves each to the canonical Substack site URL and feed URL,
and extracts Sidestack-side "Substack details" key/value pairs (e.g., Author,
First Posted, Free Subscribers, Paid Subscribers, Country, Language).

Only the following CLI flags are supported (as requested):
  --sidestack-directory-base  Base directory URL (default: https://sidestack.io/directory/all)
  --max-workers               Max threads for crawling (default: 32)
  --verbose                   Chatty progress logging
  --output                    Output JSON file (default: feeds.json)
  --sample-first N            Process only the first N detail pages, then write JSON and exit

Design goals:
- Robust extraction of substack site from detail pages:
    1) Prefer any <a href="https://*.substack.com/..."> found on page (excluding substackcdn / my.substack).
    2) If none, derive from slug in detail URL: https://{slug}.substack.com
- Feed URL resolution:
    - Try "/feed", then "/feed.xml" (some stacks use explicit .xml)
    - Use GET (not HEAD), allow redirects, set a browsery UA
    - If 403/429/timeouts, keep the candidate feed but mark status="UNVERIFIED"
- Rate limiting & retries:
    - Global requests.Session with urllib3 Retry
    - Simple per-host pacing (100ms between requests to the same netloc)
- Threaded crawling using concurrent.futures (no extra deps)
- Sidestack details & metadata:
    - Parse <dt>/<dd> pairs in the "Substack details" section (generic, order-agnostic).
    - Parse <title>, <meta name="description">, <meta property="og:image">.
    - Parse breadcrumb/topic chips like /directory/category/<slug> as categories.

Output schema (list of dicts):
{
  "slug": "a16znews",
  "detail_url": "https://sidestack.io/directory/substack/a16znews",
  "sidestack": {
    "page_title": "...",
    "meta_description": "...",
    "og_image": "...",
    "categories": ["Economics", "Markets"]
  },
  "substack": {
    "website": "https://a16znews.substack.com",
    "feed_url": "https://a16znews.substack.com/feed",
    "alternate_feed_url": "https://a16znews.substack.com/feed.xml",
    "feed_verified_url": "https://a16znews.substack.com/feed" | null,
    "details": {
      "headline": "...",
      "description": "...",
      "author_name": "...",
      "author_image": "...",
      "date_published": "YYYY-MM-DD",
      "in_language": "en",
      "first_posted": "Month YYYY",
      "free_subscribers_count": 23000,
      "paid_subscribers_count": 100,
      "country": "US",
      "language_ui": "English"
    }
  },
  "status": "OK" | "UNVERIFIED" | "FAIL",
  "reason": "optional text (for FAIL/UNVERIFIED)"
}
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import html
import json
import re
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_BASE = "https://sidestack.io/directory/all"
BUCKETS = ["%230-9"] + [chr(c) for c in range(ord("A"), ord("Z")+1)]

# Match "https://<something>.substack.com[/optional-path]" but exclude substackcdn/my.substack
SUBSTACK_SITE_RE = re.compile(
    r'https?://(?!(?:substackcdn\.com|my\.substack\.com)\b)([a-z0-9-]+(?:\.[a-z0-9-]+)*)\.substack\.com(?:/[^\s"\'<>)]*)?',
    re.IGNORECASE,
)

# Relative links to detail pages
DETAIL_HREF_RE = re.compile(r'href="(/directory/substack/[^"]+)"', re.IGNORECASE)

# <dt>..<dd> pairs in the "Substack details" card
DT_DD_RE = re.compile(
    r"<dt[^>]*>(.*?)</dt>\s*(?:<hr[^>]*>\s*)?<dd[^>]*>(.*?)</dd>",
    re.IGNORECASE | re.DOTALL,
)

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_DESC_RE = re.compile(
    r'<meta\s+(?:name=["\']description["\']|property=["\']og:description["\'])\s+content=["\'](.*?)["\']',
    re.IGNORECASE,
)
OG_IMAGE_RE = re.compile(
    r'<meta\s+property=["\']og:image["\']\s+content=["\'](.*?)["\']',
    re.IGNORECASE,
)
# category chips/links like: <a href="/directory/category/economics">Economics</a>
CATEGORY_RE = re.compile(
    r'href="/directory/category/[^"]+"\s*[^>]*>([^<]+)</a>',
    re.IGNORECASE,
)

# JSON-LD block (often present)
JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)

# --- Requests session with retries ------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,*/*"})
    return s

SESSION = make_session()

# simple per-host pacing (100ms between calls per host) to reduce 403/timeout rates
_host_lock = defaultdict(threading.Lock)
_host_last = defaultdict(float)
_PACE_SECONDS = 0.10

def _paced_get(url: str, **kwargs) -> requests.Response:
    parsed = urlparse(url)
    host = parsed.netloc
    with _host_lock[host]:
        now = time.time()
        wait = _PACE_SECONDS - (now - _host_last[host])
        if wait > 0:
            time.sleep(wait)
        resp = SESSION.get(url, timeout=20, allow_redirects=True, **kwargs)
        _host_last[host] = time.time()
        return resp

# --- Small utils -----------------------------------------------------------------

def log(msg: str, enabled: bool):
    if enabled:
        print(msg, flush=True)

def strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s)

def normalize_ws(s: str) -> str:
    return " ".join(s.split())

def clean_text(s: str) -> str:
    return normalize_ws(html.unescape(strip_tags(s or "")).strip())

def strip_utm(url: str) -> str:
    """Remove common tracking params but keep the rest of the querystring."""
    p = urlparse(url)
    if not p.query:
        return url
    q = [(k, v) for (k, v) in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    return urlunparse(p._replace(query=urlencode(q)))

def canonicalize_site(url: str) -> str:
    """Ensure scheme+host are lowercased, drop fragments, strip utm params, and trim trailing slash."""
    url = strip_utm(url)
    p = urlparse(url)
    scheme = p.scheme.lower() or "https"
    netloc = p.netloc.lower()
    path = p.path or ""
    new = urlunparse((scheme, netloc, path, "", "", ""))
    if new.endswith("/"):
        new = new[:-1]
    return new

def parse_count_like(s: str) -> Optional[int]:
    """
    Convert strings like '23K+', '1.2M', '100+' to integers.
    Returns None if no numeric content.
    """
    if not s:
        return None
    raw = s.strip().replace(",", "").replace(" ", "")
    m = re.match(r"^([0-9]*\.?[0-9]+)\s*([KMBkmb])?\+?$", raw)
    if not m:
        m2 = re.match(r"^([0-9]+)\+?$", raw)
        if m2:
            try:
                return int(m2.group(1))
            except Exception:
                return None
        return None
    val = float(m.group(1))
    suf = (m.group(2) or "").lower()
    mult = 1
    if suf == "k":
        mult = 1_000
    elif suf == "m":
        mult = 1_000_000
    elif suf == "b":
        mult = 1_000_000_000
    return int(round(val * mult))

def best_substack_on_page(html_text: str) -> Optional[str]:
    """Return the first plausible substack site URL found in the document, excluding substackcdn/my.substack."""
    text = html.unescape(html_text)
    for m in SUBSTACK_SITE_RE.finditer(text):
        candidate = m.group(0)
        if "substackcdn.com" in candidate or "my.substack.com" in candidate:
            continue
        return canonicalize_site(candidate)
    return None

def slug_from_detail(detail_url: str) -> str:
    return urlparse(detail_url).path.rstrip("/").split("/")[-1]

def derive_site_from_slug(detail_url: str) -> str:
    return f"https://{slug_from_detail(detail_url)}.substack.com"

def choose_feed_url(site_url: str) -> List[str]:
    # Try common patterns in order
    return [f"{site_url}/feed", f"{site_url}/feed.xml"]

def verify_feed(url: str) -> Tuple[bool, str]:
    try:
        r = _paced_get(url, headers={"Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8"})
        head = r.content[:4096]
        if r.status_code == 200 and (b"<rss" in head or b"<feed" in head or head.strip().startswith(b"<?xml")):
            return True, "200"
        if 200 <= r.status_code < 300 and (b"<rss" in r.content or b"<feed" in r.content):
            return True, str(r.status_code)
        return False, f"HTTP {r.status_code}"
    except requests.RequestException as e:
        return False, f"REQ_ERR {type(e).__name__}: {e}"

def parse_sidestack_meta(html_text: str) -> Dict[str, Optional[str]]:
    title = None
    m = TITLE_RE.search(html_text)
    if m:
        title = clean_text(m.group(1))
    desc = None
    m = META_DESC_RE.search(html_text)
    if m:
        desc = clean_text(m.group(1))
    og = None
    m = OG_IMAGE_RE.search(html_text)
    if m:
        og = m.group(1).strip()
    cats = [clean_text(x) for x in CATEGORY_RE.findall(html_text)]
    cats = [c for c in cats if c]
    return {
        "page_title": title,
        "meta_description": desc,
        "og_image": og,
        "categories": cats or None,
    }

def parse_dt_dd_pairs(html_text: str) -> Dict[str, str]:
    """
    Extract all <dt>Label</dt><dd>Value</dd> pairs (order-agnostic).
    Returns dict with normalized labels as keys (lowercase words joined by underscores).
    """
    pairs = {}
    for m in DT_DD_RE.finditer(html_text):
        raw_dt, raw_dd = m.group(1), m.group(2)
        key = clean_text(raw_dt)
        val = clean_text(raw_dd)
        if not key:
            continue
        norm = re.sub(r"[^a-z0-9]+", "_", key.lower()).strip("_")
        if norm:
            pairs[norm] = val
    return pairs

def parse_jsonld_first(html_text: str) -> Optional[dict]:
    m = JSONLD_RE.search(html_text)
    if not m:
        return None
    block = m.group(1).strip()
    try:
        return json.loads(block)
    except Exception:
        try:
            first_obj = re.search(r"\{.*\}", block, re.DOTALL)
            if first_obj:
                return json.loads(first_obj.group(0))
        except Exception:
            return None
    return None

def merge_details(dtdd: Dict[str, str], jsonld: Optional[dict]) -> Dict[str, Optional[object]]:
    out: Dict[str, Optional[object]] = {
        "headline": None,
        "description": None,
        "author_name": None,
        "author_image": None,
        "date_published": None,
        "in_language": None,
        "first_posted": None,
        "free_subscribers_count": None,
        "paid_subscribers_count": None,
        "country": None,
        "language_ui": None,
    }

    # From dt/dd
    if "author" in dtdd:
        out["author_name"] = dtdd.get("author")
    if "first_posted" in dtdd:
        out["first_posted"] = dtdd.get("first_posted")
    if "free_subscribers" in dtdd:
        out["free_subscribers_count"] = parse_count_like(dtdd.get("free_subscribers", ""))
    if "paid_subscribers" in dtdd:
        out["paid_subscribers_count"] = parse_count_like(dtdd.get("paid_subscribers", ""))
    if "country" in dtdd:
        out["country"] = dtdd.get("country")
    if "language" in dtdd:
        out["language_ui"] = dtdd.get("language")

    # From JSON-LD
    if jsonld:
        out["headline"] = jsonld.get("headline") or out["headline"]
        out["description"] = jsonld.get("description") or out["description"]
        out["in_language"] = jsonld.get("inLanguage") or out["in_language"]
        out["date_published"] = jsonld.get("datePublished") or out["date_published"]

        author = jsonld.get("author")
        if isinstance(author, dict):
            out["author_name"] = author.get("name") or out["author_name"]
            out["author_image"] = author.get("image") or out["author_image"]
        elif isinstance(author, list) and author:
            a0 = author[0]
            if isinstance(a0, dict):
                out["author_name"] = a0.get("name") or out["author_name"]
                out["author_image"] = a0.get("image") or out["author_image"]
            elif isinstance(a0, str):
                out["author_name"] = a0 or out["author_name"]
        elif isinstance(author, str):
            out["author_name"] = author or out["author_name"]

        if not out["author_image"]:
            img = jsonld.get("image")
            if isinstance(img, str):
                out["author_image"] = img
            elif isinstance(img, dict):
                out["author_image"] = img.get("url") or out["author_image"]

    for k, v in list(out.items()):
        if isinstance(v, str) and not v.strip():
            out[k] = None
    return out

# --- Crawling --------------------------------------------------------------------

@dataclass
class DetailResult:
    slug: str
    detail_url: str
    sidestack_meta: Dict[str, Optional[str]]
    substack_site: Optional[str] = None
    feed_url: Optional[str] = None
    feed_alt_url: Optional[str] = None
    feed_verified_url: Optional[str] = None
    substack_details: Optional[Dict[str, object]] = None
    status: str = "FAIL"
    reason: Optional[str] = None

def scrape_bucket(bucket_url: str, verbose: bool=False) -> List[str]:
    """Return list of absolute detail-page URLs under this letter bucket."""
    try:
        resp = _paced_get(bucket_url)
        html_text = resp.text
        rels = DETAIL_HREF_RE.findall(html_text)
        urls = [urljoin(bucket_url, r) for r in rels]
        urls = sorted(set(urls))
        log(f"[crawl {bucket_url.rsplit('/',1)[-1]}] {len(urls)} detail pages", verbose)
        return urls
    except requests.RequestException as e:
        log(f"[crawl ERR] {bucket_url} -> {e}", verbose)
        return []

def process_detail(detail_url: str, verbose: bool=False) -> DetailResult:
    slug = slug_from_detail(detail_url)
    try:
        r = _paced_get(detail_url)
        text = r.text
    except requests.RequestException as e:
        return DetailResult(
            slug=slug, detail_url=detail_url, sidestack_meta={}, status="FAIL",
            reason=f"REQ_ERR {e}"
        )

    sidemeta = parse_sidestack_meta(text)
    dtdd = parse_dt_dd_pairs(text)
    jsonld = parse_jsonld_first(text)
    details = merge_details(dtdd, jsonld)

    site = best_substack_on_page(text) or derive_site_from_slug(detail_url)
    site = canonicalize_site(site)

    candidates = choose_feed_url(site)
    verified: Optional[str] = None
    chosen: Optional[str] = None
    alt: Optional[str] = None
    if candidates:
        chosen = candidates[0]
        alt = candidates[1] if len(candidates) > 1 else None

    for cand in candidates:
        ok, _why = verify_feed(cand)
        if ok:
            verified = cand
            break

    status = "OK" if verified else "UNVERIFIED"
    reason = None if verified else "Could not confirm XML (possible rate-limit or non-standard feed)"

    if verbose:
        if verified:
            log(f"[detail OK] {detail_url} -> {site} :: {verified}", True)
        else:
            log(f"[detail UNVERIFIED] {detail_url} -> {site} :: {chosen}", True)

    return DetailResult(
        slug=slug,
        detail_url=detail_url,
        sidestack_meta=sidemeta,
        substack_site=site,
        feed_url=chosen,
        feed_alt_url=alt,
        feed_verified_url=verified,
        substack_details=details,
        status=status,
        reason=reason,
    )

def serialize_and_write(results: List[DetailResult], out_path: str, verbose: bool):
    out = [
        {
            "slug": r.slug,
            "detail_url": r.detail_url,
            "sidestack": {
                "page_title": r.sidestack_meta.get("page_title"),
                "meta_description": r.sidestack_meta.get("meta_description"),
                "og_image": r.sidestack_meta.get("og_image"),
                "categories": r.sidestack_meta.get("categories"),
            },
            "substack": {
                "website": r.substack_site,
                "feed_url": r.feed_url,
                "alternate_feed_url": r.feed_alt_url,
                "feed_verified_url": r.feed_verified_url,
                "details": r.substack_details,
            },
            "status": r.status,
            **({"reason": r.reason} if r.reason else {}),
        }
        for r in results
    ]
    Path(out_path).write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    ok = sum(1 for r in results if r.status == "OK")
    unv = sum(1 for r in results if r.status == "UNVERIFIED")
    fail = sum(1 for r in results if r.status == "FAIL")
    log(f"[summary] total: {len(results)}  OK: {ok}  UNVERIFIED: {unv}  FAIL: {fail}", True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sidestack-directory-base", default=DEFAULT_BASE)
    ap.add_argument("--max-workers", type=int, default=32)
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--output", default="feeds.json")
    ap.add_argument("--sample-first", type=int, default=None,
                    help="Process only the first N detail pages, then write JSON and exit")
    args = ap.parse_args()

    base = args.sidestack_directory_base.rstrip("/")
    buckets = [urljoin(base + "/", b) for b in BUCKETS]

    # Crawl buckets
    start = time.time()
    all_detail_urls: List[str] = []
    with cf.ThreadPoolExecutor(max_workers=min(max(1, args.max_workers//4), 32)) as ex:
        futs = {ex.submit(scrape_bucket, u, args.verbose): u for u in buckets}
        done_count = 0
        for fut in cf.as_completed(futs):
            urls = fut.result()
            all_detail_urls.extend(urls)
            done_count += 1
            rate = done_count / max(1e-6, (time.time() - start))
            remaining = len(buckets) - done_count
            eta = remaining / rate if rate > 0 else 0
            log(f"[crawl buckets] {done_count}/{len(buckets)} ({rate:.2f}/s) ETA {eta:.1f}s", args.verbose)

    all_detail_urls = sorted(set(all_detail_urls))
    log(f"[sidestack-dir] collected {len(all_detail_urls)} detail pages", True)

    # Sampling mode: limit to first N
    if args.sample_first is not None and args.sample_first > 0:
        original = len(all_detail_urls)
        all_detail_urls = all_detail_urls[:args.sample_first]
        log(f"[sample] --sample-first={args.sample_first} enabled "
            f"(processing {len(all_detail_urls)}/{original} detail pages)", True)

    # Process details
    results: List[DetailResult] = []
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(process_detail, u, args.verbose): u for u in all_detail_urls}
        processed = 0
        for fut in cf.as_completed(futs):
            res: DetailResult = fut.result()
            results.append(res)
            processed += 1
            if processed % 25 == 0 or (args.sample_first and processed == len(all_detail_urls)):
                rate = processed / max(1e-6, (time.time() - t0))
                remaining = len(all_detail_urls) - processed
                eta = remaining / rate if rate > 0 else 0
                log(f"[detail pages] {processed}/{len(all_detail_urls)} ({rate:.2f}/s) ETA {eta:.1f}s", True)

    # Serialize & done
    serialize_and_write(results, args.output, True)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
