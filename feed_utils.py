"""Shared helpers for the non-Substack feed directory.

The functions here deliberately avoid third-party dependencies so they can be
used by the builder, the validator, and the tests without importing HTTP or
parsing libraries.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "dclid",
    "gbraid",
    "wbraid",
    "msclkid",
    "yclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "_hsenc",
    "_hsmi",
    "mkt_tok",
    "vero_id",
    "oly_anon_id",
    "oly_enc_id",
    "spm",
    "ref_src",
}

TRACKING_PREFIXES = ("utm_", "pk_", "sc_", "elq")


def normalize_url(url: str) -> str:
    """Return a stable, conservative canonical form for feed deduplication.

    Only transformations that are safe for virtually every feed are applied:
    scheme/host case, default ports, fragments, obvious tracking parameters,
    duplicate slashes, and trailing slashes. Query strings are retained and
    sorted so that parameter order does not create duplicates.
    """

    if not isinstance(url, str):
        return ""
    url = url.strip()
    if not url:
        return ""
    if "://" not in url:
        url = "https://" + url

    try:
        parts = urlsplit(url)
    except ValueError:
        return url

    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower().rstrip(".")
    if not host:
        return url

    port = parts.port
    netloc = host
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"

    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    if path == "/":
        path = ""

    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        lower = key.lower()
        if lower in TRACKING_PARAMETERS or lower.startswith(TRACKING_PREFIXES):
            continue
        query_items.append((key, value))
    query_items.sort()
    query = urlencode(query_items, doseq=True)

    return urlunsplit((scheme, netloc, path, query, ""))


def host_of(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


def normalize_language(value: str) -> str:
    """Reduce BCP-47 / locale values to a simple language code."""

    if not isinstance(value, str):
        return ""
    value = value.strip().lower()
    if not value:
        return ""
    value = value.replace("_", "-")
    if value in {"utf", "utf8", "utf-8", "iso", "iso-8859-1", "us-ascii", "ascii"}:
        return ""
    if value in {"en-us", "en-gb", "en-ca", "en-au", "en-nz", "en-ie"}:
        return "en"
    if value.startswith("zh-") or value in {"zh", "zho", "chi"}:
        return "zh"
    if value.startswith("pt-"):
        return "pt"
    if value.startswith("fr-"):
        return "fr"
    if value.startswith("de-"):
        return "de"
    if value.startswith("es-"):
        return "es"
    code = value.split("-", 1)[0]
    if re.fullmatch(r"[a-z]{2,3}", code):
        return code
    return ""


def is_substack_url(url: str) -> bool:
    host = host_of(url)
    return host == "substack.com" or host.endswith(".substack.com")


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    tmp.replace(path)


def unique_by(items: Iterable[dict], key) -> list[dict]:
    seen = set()
    result = []
    for item in items:
        value = key(item)
        if value in seen:
            continue
        seen.add(value)
        result.append(item)
    return result


def clean_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.replace("\x00", " ").split()).strip()
    return str(value).strip()
