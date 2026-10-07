"""The OpenSearch index the search engine queries, and the only definition of it.

Documents are written by the search indexer (indexing/indexer.py) from PostgreSQL Silver
(`pgs_db.repositories.search.SearchRepository.documents`, one document per canonical
page, `_id` = `document_id` = `pages.id`) and read by the BM25 retriever. Vectors are not
stored here: dense retrieval runs on pgvector (`page_embeddings`), so each embedding has
one home.

The index is `<alias>_v<INDEX_VERSION>` behind the alias in OPENSEARCH_INDEX
(`np_web_pages`). A change to this mapping that existing documents cannot take (a field's
type, an analyzer) needs a new INDEX_VERSION and a reindex; see index_manager.py.
`dynamic: strict` makes a document with an unmapped field fail loudly at index time
instead of silently growing the mapping.
"""

from __future__ import annotations

from typing import Any

INDEX_VERSION = 1

_KEYWORD: dict[str, Any] = {"type": "keyword"}
_TEXT: dict[str, Any] = {"type": "text", "analyzer": "pgs_text_analyzer"}
_NAME: dict[str, Any] = {
    "type": "text",
    "fields": {"raw": {"type": "keyword", "ignore_above": 256}},
}

# GeoLocationOut (pgs_db.schemas.silver): codes for filtering, names for display.
_GEO_PROPERTIES: dict[str, Any] = {
    "province_code": _KEYWORD,
    "province_name_en": _NAME,
    "province_name_ne": _NAME,
    "district_code": _KEYWORD,
    "district_name_en": _NAME,
    "district_name_ne": _NAME,
    "municipality_id": _KEYWORD,
    "municipality_type": _KEYWORD,
    "municipality_name_en": _NAME,
    "municipality_name_ne": _NAME,
    "ward_number": {"type": "integer"},
}

# Fields the indexer writes; anything else in a Silver document is dropped before
# indexing (see indexer.to_index_document).
INDEXED_FIELDS = (
    "document_id",
    "title",
    "description",
    "searchable_text",
    "summary",
    "source_url",
    "domain",
    "language",
    "content_type",
    "category",
    "published_at",
    "keywords",
    "content_length",
    "geo",
    "geo_tags",
    "file_info",
    "indexed_at",
)


def index_body(replicas: int = 0) -> dict[str, Any]:
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": replicas,
            "analysis": {
                "analyzer": {
                    # Unicode word segmentation (UAX #29) handles Devanagari and Latin;
                    # no stopwords, so Nepali particles still match exact phrases.
                    "pgs_text_analyzer": {"type": "standard", "stopwords": "_none_"}
                }
            },
        },
        "mappings": {
            "dynamic": "strict",
            "_meta": {"index_version": INDEX_VERSION, "source": "pgs_db SearchRepository"},
            "properties": {
                "document_id": _KEYWORD,
                "title": {**_TEXT, "fields": {"raw": {"type": "keyword", "ignore_above": 512}}},
                "description": _TEXT,
                "searchable_text": _TEXT,
                # The first few hundred characters, the snippet when nothing matched.
                "summary": {"type": "text", "index": False},
                "source_url": {"type": "keyword", "index": False},
                "domain": _KEYWORD,
                "language": _KEYWORD,
                "content_type": _KEYWORD,
                "category": _KEYWORD,
                "published_at": {"type": "date"},
                "keywords": _KEYWORD,
                "content_length": {"type": "integer"},
                # The page's primary location.
                "geo": {"properties": _GEO_PROPERTIES},
                # Every location the page is tagged with, each with its full code chain:
                # a filter on district D38 matches a page tagged with any municipality
                # of D38 (the same semantics as the pgvector search).
                "geo_tags": {"properties": _GEO_PROPERTIES},
                "file_info": {
                    "properties": {
                        "extension": _KEYWORD,
                        "mime_type": _KEYWORD,
                        "size_bytes": {"type": "long"},
                    }
                },
                "indexed_at": {"type": "date"},
            },
        },
    }
