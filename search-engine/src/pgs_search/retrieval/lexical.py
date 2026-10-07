"""BM25 retrieval on the OpenSearch index (indexing/mappings.py)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from opensearchpy import OpenSearch

from pgs_search.config import settings

_SEARCH_FIELDS = ["title^3", "description^2", "keywords^2", "searchable_text"]
# Never shipped back per hit: the snippet comes from the highlighter (or `summary`).
_SOURCE_EXCLUDES = ["searchable_text"]
SNIPPET_CHARS = 240


def build_query(
    variants: Sequence[str],
    size: int,
    filters: Sequence[dict[str, Any]] | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """One `should` clause per query variant (original, lemma, place name, translation):
    a document matches if any variant does, and matches on several add up."""
    bool_query: dict[str, Any] = {
        "should": [
            {
                "multi_match": {
                    "query": variant,
                    "fields": _SEARCH_FIELDS,
                    "type": "best_fields",
                    "fuzziness": "AUTO",
                    "prefix_length": 1,
                    "max_expansions": 20,
                }
            }
            for variant in variants
        ],
        "minimum_should_match": 1,
    }
    if filters:
        bool_query["filter"] = list(filters)
    body: dict[str, Any] = {
        "size": size,
        "track_total_hits": False,
        "query": {"bool": bool_query},
        "_source": {"excludes": _SOURCE_EXCLUDES},
        "highlight": {
            "fields": {
                "searchable_text": {
                    "fragment_size": SNIPPET_CHARS,
                    "number_of_fragments": 1,
                    "no_match_size": 0,
                }
            },
            "pre_tags": [""],
            "post_tags": [""],
        },
    }
    if timeout_seconds:
        # Server-side budget: OpenSearch returns what it has by then (partial results).
        body["timeout"] = f"{max(1, int(timeout_seconds * 1000))}ms"
    return body


def search_bm25(
    client: OpenSearch,
    variants: Sequence[str],
    size: int,
    filters: Sequence[dict[str, Any]] | None = None,
    timeout_seconds: float | None = None,
) -> list[dict[str, Any]]:
    """Up to `size` hits, best first. Raises on OpenSearch errors (the caller maps them)."""
    variants = [variant for variant in variants if variant.strip()]
    if not variants or size < 1:
        return []
    response = client.search(
        index=settings.opensearch_index,
        body=build_query(variants, size, filters, timeout_seconds),
        request_timeout=timeout_seconds or settings.opensearch_timeout_seconds,
    )
    return list(response["hits"]["hits"])
