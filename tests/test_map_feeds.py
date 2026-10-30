"""Tests for the map_feeds CLI helpers."""

from __future__ import annotations

import pytest

from map_feeds import map_item, parse_min_subscribers


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0", 0),
        ("1000", 1000),
        ("1,000", 1000),
        ("2.5k", 2500),
        ("1m", 1_000_000),
        ("1.5M", 1_500_000),
        ("2b", 2_000_000_000),
    ],
)
def test_parse_min_subscribers_short_forms(value: str, expected: int) -> None:
    assert parse_min_subscribers(value) == expected


@pytest.mark.parametrize("value", ["", "abc", "2.5x", "-1", "1.2.3k"])
def test_parse_min_subscribers_rejects_bad_values(value: str) -> None:
    with pytest.raises(ValueError):
        parse_min_subscribers(value)


def test_map_item_preserves_schema_and_sums_subscribers() -> None:
    mapped, total = map_item(
        {
            "status": "OK",
            "category": "Technology",
            "substack": {
                "feed_verified_url": "https://example.substack.com/feed",
                "details": {
                    "headline": "Example",
                    "author_name": "Ada",
                    "in_language": "en",
                    "free_subscribers_count": 100,
                    "paid_subscribers_count": 25,
                },
            },
        }
    )
    assert mapped == {
        "headline": "Example",
        "author_name": "Ada",
        "language": "en",
        "feed_url": "https://example.substack.com/feed",
        "category": "Technology",
        "total_subscribers": "125",
    }
    assert total == 125
