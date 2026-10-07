"""Dense retrieval: the query's LaBSE vector against page_embeddings (pgvector).

`pgs_db.repositories.search.SearchRepository.vector_search` scores the canonical pages'
current embeddings by cosine similarity, with the same geo / language / content-type
filters as the BM25 query (a district filter matches pages tagged anywhere inside it).
"""

from __future__ import annotations

from typing import Any, Protocol

from pgs_search.query.embeddings import QueryEmbedder


class SearchFilters(Protocol):
    province_code: str
    district_code: str
    municipality_id: str
    ward_number: int
    content_type: str
    language: str


class DenseRetriever:
    def __init__(self, session_factory: Any, embedder: QueryEmbedder) -> None:
        self._session_factory = session_factory
        self._embedder = embedder

    def search(self, query: str, filters: SearchFilters, limit: int) -> list[dict[str, Any]]:
        from pgs_db.repositories.search import SearchRepository

        vector = self._embedder.embed(query)
        with self._session_factory() as session:
            rows = SearchRepository(session).vector_search(
                vector,
                self._embedder.model_name,
                province_code=filters.province_code or None,
                district_code=filters.district_code or None,
                municipality_id=filters.municipality_id or None,
                ward_number=filters.ward_number or None,
                content_type=filters.content_type or None,
                language=filters.language or None,
                limit=limit,
            )
        return [
            {
                "document_id": str(row["page_id"]),
                "vector_score": float(row["score"]),
                "title": row.get("title") or "",
                "source_url": row.get("url") or "",
                "domain": row.get("domain") or "",
                "content_type": row.get("content_type") or "",
            }
            for row in rows
        ]
