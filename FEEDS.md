# Non-Substack feed directory (`other_feeds_v2.json`)

`other_feeds_v2.json` is a curated, validated RSS/Atom directory for sources
that are **not** already covered by the Substack dataset in `feeds.json`.
`other_feeds.json` remains the untouched legacy v1 list (107 feeds).

**Snapshot: 2026-10-22 — 17,011 distinct feeds, all checked with a real HTTP
request and parsed as RSS/RSS 1.0/Atom with at least one usable entry.**

## Schema

Every entry has exactly these six string fields:

```json
{
  "headline": "This Week in GNOME",
  "author_name": "",
  "language": "en",
  "feed_url": "https://thisweek.gnome.org/index.xml",
  "category": "Linux & Open Source",
  "total_subscribers": ""
}
```

- `headline` is the publication/feed name, not a generated slug.
- `author_name` is populated only when the source supplies a reliable name.
- `language` is a short language code when known; it is empty when unknown.
- `category` is a consistent editorial category (see the distribution below).
- `total_subscribers` is deliberately empty; subscriber counts are not invented.
- Entries are sorted by `headline`, then `feed_url`, for stable diffs.

## Where the feeds came from

| Source | Candidates checked | Feeds in final file | Notes |
|---|---:|---:|---|
| [Kagi Small Web](https://github.com/kagisearch/smallweb) | 18,000 | 13,512 | MIT-licensed personal-blog directory; no per-feed language metadata |
| [ooh.directory](https://ooh.directory/) | 2,361 | 2,248 | Category OPML exports; 2,421 unique blogs across 152 topics |
| [Tidings RSS](https://github.com/fuxiaoai/tidings-rss) | 675 | 549 | CC0 curated collections, including Chinese-language sources |
| [Awesome RSS Feeds](https://github.com/plenaryapp/awesome-rss-feeds) | 721 | 527 | CC0 recommended and country OPML files |
| Existing v2 entries | 148 | 146 | Two removed after 403/429 responses |
| Existing v1 entries not in v2 | 30 | 27 | Re-added only when independently validated |
| Manual GNOME additions | 5 | 2 | *This Week in GNOME* and GTK blog; others were empty/404 |
| **Total** | **21,940** | **17,011** | |

Other directories were explored but not imported in this pass:

- [Fountain of RSS](https://github.com/tfederman/fountain-of-rss) contains about
  32k feeds collected in April 2025. About 25k were extracted as candidates,
  but the dataset is stale, explicitly favours quantity over quality, and warns
  about NSFW/spam content. It is kept as a future source rather than padding
  this verified list.
- [blogs.hn](https://blogs.hn/) is a useful discovery list, but it points to
  blogs rather than stable feed URLs. Feed autodiscovery would have to be run
  per site; it was not used to inflate the count.

### Duplicate/Substack handling

Before validation, URLs are normalised (scheme/host case, default ports,
fragments, tracking parameters, sorting query parameters, trailing slashes).
Candidates are removed when they are already represented in the Substack
dataset by normalised feed URL, Substack hostname, or an exact headline match.
The builder additionally removes:

- 24 repeated candidate URLs,
- 26 feeds whose final redirect destination duplicates another feed,
- 272 syndicated copies identified by identical feed title plus two shared
  recent article links (or an exact one-article match).

The Kagi expansion contributed 16 URL duplicates, 6 redirect duplicates, and
224 content duplicates; the original build contributed the other 76.

This is deliberately conservative: different category feeds from the same site
are kept even when they share a publication name.

## Validation method

`validate_other_feeds.py` performs a real HTTP `GET` for every feed:

- follows up to 10 redirects and records the final destination;
- uses a descriptive user agent and per-host spacing to avoid hammering sites;
- bounded global concurrency (48–256 in the import runs, 32 in CI); the large
  expansion batch was checked at 128 concurrency with one retry;
- 20–30 second timeouts and retries with exponential backoff for temporary
  failures and HTTP 429/5xx;
- parses the response with `feedparser` and performs a strict `defusedxml`
  well-formedness check for diagnostics (feedparser remains tolerant of
  slightly malformed XML);
- requires an RSS, RSS 1.0/RDF, or Atom document and at least one entry with a
  title or link;
- rejects HTML pages, search pages, empty feeds, 404/410 responses, DNS
  failures, redirect loops, and implausibly large responses;
- classifies blocked/rate-limited/timeout/5xx endpoints as **inconclusive**,
  not verified. Inconclusive feeds never count toward the total.

### Results

**Original build**

- 7,440 unique candidates were requested.
- 6,839 passed real feed validation.
- 601 did not: 372 invalid (404/410, DNS failure, HTML instead of feed, empty
  feed, non-feed XML, oversize response), 228 inconclusive (403/429, timeouts,
  5xx, connection errors), and 1 duplicate Substack source.
- 76 duplicates were removed during assembly.
- Original final: **6,763 feeds**.

**Kagi Small Web expansion**

- 14,500 additional Kagi candidates were requested.
- 10,494 passed real feed validation.
- 4,006 did not: 233 invalid and 3,773 inconclusive. The high-concurrency
  expansion passes (up to 256 simultaneous requests, then 128) produced many
  timeout-based inconclusive results; those feeds were excluded rather than
  assumed valid.
- 246 additional duplicates were removed (224 content, 6 redirect, 16 URL).
- Net addition: **10,248 feeds**.
- New final: **17,011 feeds**.

An additional low-concurrency retry pass on the original batch rechecked
inconclusive candidates with 25-second timeouts; 1,589 subsequently validated.
A random 100-feed low-concurrency audit of the accepted Kagi additions found
99 valid, 1 transient inconclusive, and 0 invalid. Feeds that still returned
403/429 after retries (for example STAT News and VentureBeat) were excluded
rather than claimed as verified.

A later platform recheck found that all 124 YouTube RSS endpoints in the
original draft returned HTTP 404, so they were removed rather than left as
broken entries. Nine additional slow/unreachable feeds were removed in the
same final passes.

Validation is a point-in-time result. `validation_report.json` records the
`checked_at` timestamp, final URL, status, parser metadata, and any error for
every included feed.

### Coverage and known limitations

- All 17,011 counted feeds returned a parseable feed with at least one readable
  entry during validation. There is no minimum publication frequency: a valid
  but slow/archived feed is not rejected solely for being old.
- 90 entries have unknown language; the file currently reports English
  (16,424), Chinese (432), Spanish (14), French (10), Polish/Russian (7 each),
  and smaller numbers of other languages. Kagi Small Web does not publish
  per-feed language metadata, so its entries default to `en`; the English
  figure is therefore an overcount, not an audited language census.
- Categories are editorially mapped from the source directories and, for a few
  legacy entries, inferred from the publication name/URL. They are useful
  browse categories, not a formal ontology. Kagi is a personal-blog directory,
  so its entries are mostly `Personal Blogs` (13,871); other large categories
  include Arts & Culture (836) and Technology (732).
- Duplicate detection is URL/redirect/title+article based. Two genuinely
  different feeds that syndicate exactly the same recent articles may be
  collapsed; two copies with different titles and no shared recent entries may
  remain.
- Feed availability changes. Run the validator regularly; do not delete a feed
  merely because one check was inconclusive.

## Maintaining the list

### Install

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-validation.txt
```

### Validate

```bash
# Validate the whole directory; writes validation_report.json
.venv/bin/python validate_other_feeds.py --input other_feeds_v2.json

# Validate only feeds that are not in the legacy v1 file
.venv/bin/python validate_other_feeds.py \
  --input other_feeds_v2.json --baseline other_feeds.json --only-new

# Ignore the incremental cache and re-check everything
.venv/bin/python validate_other_feeds.py --input other_feeds_v2.json --force

# Faster / slower runs
.venv/bin/python validate_other_feeds.py --concurrency 32 --timeout 25 --retries 2
```

Useful options:

- `--cache PATH` — incremental results cache (default
  `.feed_validation_cache.json`, git-ignored); fresh valid/invalid results are
  reused for `--max-age-hours`.
- `--only-new --baseline FILE` — validate only URLs absent from the baseline.
- `--output PATH` — machine-readable report.
- `--limit N` — validate a sample while debugging.
- Exit codes: `0` all valid, `1` at least one invalid, `2` only inconclusive
  problems.

The report separates `valid`, `invalid`, and `inconclusive`. The workflow never
rewrites `other_feeds_v2.json`; a transient failure cannot silently remove a
source.

### Rebuild after importing candidates

```bash
.venv/bin/python build_other_feeds_v2.py \
  --candidate-file candidates/curated.json \
  --candidate-file candidates/smallweb.json \
  --cache .feed_validation_cache.json \
  --output other_feeds_v2.json \
  --stats feed_directory_stats.json
```

Candidate files may be JSON arrays or JSON Lines. Each candidate should have
`feed_url`, `headline`, `category`, `language`, `source`, and optionally
`source_priority`. Only cache entries classified as `valid` are written.

To reproduce the external source downloads and candidate extraction:

```bash
.venv/bin/python scripts/fetch_candidate_sources.py --output-dir candidate_sources
.venv/bin/python scripts/import_candidates.py \
  --source-dir candidate_sources --repo-dir . --output candidates/all.jsonl
```

### Tests

```bash
.venv/bin/python -m pytest -q
```

Tests cover URL normalisation, RSS/Atom parsing, HTML/empty/404/redirect-loop
rejection, duplicate handling, the six-field schema, URL uniqueness, the
original v1 hash, and the presence of *This Week in GNOME*.

### CI

`.github/workflows/validate-feeds.yml` runs tests on pull requests and a full
validation weekly/on demand. The validation job writes a summary, uploads the
report as an artifact, and fails only when feeds are definitively invalid (not
when they are merely inconclusive). The JSON directory is never modified by
CI.

## Attribution

- Kagi Small Web — MIT-licensed repository.
- Tidings RSS — CC0 1.0.
- Awesome RSS Feeds — CC0 1.0.
- ooh.directory — public blog directory; used with attribution.
- Individual feeds remain the property of their publishers.
