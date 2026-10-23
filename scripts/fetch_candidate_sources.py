#!/usr/bin/env python3
"""Download the public source collections used by the feed importer.

This is a convenience script for maintainers. It clones the four GitHub
collections with ``git clone --depth 1`` and fetches ooh.directory's category
OPML exports. Nothing is written outside ``--output-dir`` (by default
``candidate_sources/``).

The downloaded data is not committed to this repository. Check each source's
licence/terms before redistributing it.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import pathlib
import re
import subprocess
import time

import httpx

REPOS = {
    "awesome-rss-feeds": "https://github.com/plenaryapp/awesome-rss-feeds.git",
    "tidings-rss": "https://github.com/fuxiaoai/tidings-rss.git",
    "smallweb": "https://github.com/kagisearch/smallweb.git",
    "fountain-of-rss": "https://github.com/tfederman/fountain-of-rss.git",
}

USER_AGENT = (
    "sidestack-feed-validator/1.0 "
    "(+https://github.com/JaredTweed/sidestack_scraper; candidate import)"
)


def clone_repos(output_dir: pathlib.Path) -> None:
    for name, url in REPOS.items():
        target = output_dir / name
        if target.exists():
            print(f"skip existing {target}")
            continue
        print(f"cloning {url}")
        subprocess.run(["git", "clone", "--depth", "1", url, str(target)], check=True)


def safe_name(url: str) -> str:
    return url.replace("://", ":__").replace("/", "_")


def fetch_ooh(output_dir: pathlib.Path) -> None:
    opml_dir = output_dir / "ooh_opml"
    opml_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": USER_AGENT}

    with httpx.Client(timeout=30, follow_redirects=True, headers=headers) as client:
        sitemap = client.get("https://ooh.directory/sitemap-categories.xml")
        sitemap.raise_for_status()
        (output_dir / "ooh_categories_sitemap.xml").write_bytes(sitemap.content)
        categories = re.findall(r"<loc>([^<]+)</loc>", sitemap.text)
        print(f"ooh.directory: {len(categories)} category pages")

        def fetch_one(category_url: str) -> tuple[str, str] | None:
            try:
                page = client.get(category_url)
                page.raise_for_status()
                match = re.search(r'href="([^"]*opml[^"]*\.xml)"', page.text)
                if not match:
                    return None
                opml_url = match.group(1)
                if opml_url.startswith("/"):
                    opml_url = "https://ooh.directory" + opml_url
                response = client.get(opml_url)
                response.raise_for_status()
                if b"<opml" not in response.content:
                    return None
                destination = opml_dir / (safe_name(category_url) + ".opml")
                destination.write_bytes(response.content)
                time.sleep(0.15)
                return category_url, str(destination)
            except Exception as exc:  # keep going on individual category failures
                print(f"warning: {category_url}: {exc}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = [item for item in pool.map(fetch_one, categories) if item]
        print(f"ooh.directory: downloaded {len(results)} category OPML files")


def fetch_kagi(output_dir: pathlib.Path) -> None:
    target = output_dir / "kagi_opml.xml"
    with httpx.Client(timeout=30, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        response = client.get("https://kagi.com/smallweb/opml")
        response.raise_for_status()
    target.write_bytes(response.content)
    print(f"kagi small web: wrote {target}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="candidate_sources", help="destination directory")
    parser.add_argument("--skip-clones", action="store_true")
    parser.add_argument("--skip-ooh", action="store_true")
    parser.add_argument("--skip-kagi", action="store_true")
    args = parser.parse_args()

    output_dir = pathlib.Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    if not args.skip_clones:
        clone_repos(output_dir)
    if not args.skip_ooh:
        fetch_ooh(output_dir)
    if not args.skip_kagi:
        fetch_kagi(output_dir)
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
