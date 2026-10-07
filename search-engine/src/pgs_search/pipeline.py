"""The search pipeline: one query in, one bounded, ranked page of results out.

    query
     -> normalize, detect language, expand            (query/normalizer.py: lemma,
        place-name equivalent, NLLB translation)
     -> BM25 on OpenSearch, with the filters         (retrieval/lexical.py)   required
     -> dense retrieval on pgvector, same filters    (retrieval/dense.py)     optional
     -> Reciprocal Rank Fusion                       (ranking/fusion.py)
     -> metadata for dense-only hits (one mget)
     -> LightGBM rerank of the fused candidates      (ranking/lightgbm_reranker.py) optional
     -> the requested page

Bounded at every step: each retriever returns at most
min(max(page * limit, CANDIDATE_POOL), MAX_RESULT_WINDOW) candidates, and page * limit
may not exceed MAX_RESULT_WINDOW. OpenSearch being unreachable fails the request
(SearchBackendUnavailable); a failure of an optional stage (dense retrieval, translation,
rerank) is logged and the response is marked `degraded`.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from opensearchpy.exceptions import ConnectionTimeout, TransportError

from pgs_search.config import Settings, settings
from pgs_search.query.normalizer import QueryExpansion, expand_query
from pgs_search.ranking.fusion import reciprocal_rank_fusion
from pgs_search.retrieval.lexical import search_bm25

logger = logging.getLogger(__name__)

ALL_SENTINELS = {"", "all", "auto"}
SNIPPET_LENGTH = 240
# Below this much time left, the optional dense stage is skipped rather than started.
MIN_SECONDS_FOR_DENSE = 0.5

Candidate = dict[str, Any]


class InvalidSearchRequest(ValueError):
    """The request is outside the engine's limits (empty/long query, deep paging)."""


class SearchBackendUnavailable(RuntimeError):
    """OpenSearch, which every search needs, did not answer."""


class SearchTimeout(RuntimeError):
    """The request's deadline passed before the required stages finished."""


@dataclass(frozen=True, slots=True)
class SearchInput:
    query: str
    province_code: str = ""
    district_code: str = ""
    municipality_id: str = ""
    ward_number: int = 0
    content_type: str = ""
    language: str = ""
    page: int = 1
    limit: int = 10


@dataclass(frozen=True, slots=True)
class GeoRef:
    province_code: str = ""
    province_name: str = ""
    district_code: str = ""
    district_name: str = ""
    municipality_id: str = ""
    municipality_name: str = ""
    ward_number: int = 0


@dataclass(frozen=True, slots=True)
class PipelineHit:
    id: str
    result_type: str
    title: str
    url: str
    domain: str
    snippet: str
    download_url: str
    file_size_bytes: int
    relevance_score: float
    language: str = ""
    published_at: str = ""
    geo: GeoRef | None = None


@dataclass(frozen=True, slots=True)
class PipelineOutput:
    total_hits: int
    results: list[PipelineHit]
    query_language: str = "unknown"
    degraded: bool = False
    # Stage -> "ok" | "skipped: ..." | "failed: ...", for logs and debugging.
    stages: dict[str, str] = field(default_factory=dict)


DenseSearch = Callable[[str, SearchInput, int], list[Candidate]]
Reranker = Callable[[list[Candidate], int], list[Candidate]]
Expander = Callable[[str], QueryExpansion]


class FinalSearchPipeline:
    """Coordinates retrieval, fusion and ranking. Thread-safe: holds no request state."""

    def __init__(
        self,
        *,
        opensearch_client: Any | None = None,
        dense_search: DenseSearch | None = None,
        reranker: Reranker | None = None,
        expander: Expander | None = None,
        config: Settings = settings,
    ) -> None:
        self._client = opensearch_client
        self._dense_search = dense_search
        self._reranker = reranker
        self._expand = expander or expand_query
        self._config = config

    # ------------------------------------------------------------------ public

    def search(self, search_input: SearchInput, deadline: float | None = None) -> PipelineOutput:
        """Run the pipeline. `deadline` is a time.monotonic() value (the gRPC deadline)."""
        window = self.validate(search_input)
        pool = min(max(window, self._config.candidate_pool), self._config.max_result_window)
        stages: dict[str, str] = {}

        expansion = self._expand(search_input.query)
        stages["translation"] = "failed" if expansion.translation_failed else "ok"
        if not expansion.normalized:
            raise InvalidSearchRequest("the query has no searchable characters")

        filters = build_filter_clauses(search_input)
        bm25_hits = self._bm25(expansion.variants, pool, filters, deadline)
        bm25 = [normalize_bm25_hit(hit) for hit in bm25_hits]
        stages["bm25"] = "ok"

        dense = self._dense(expansion.normalized, search_input, pool, deadline, stages)

        fused = reciprocal_rank_fusion([bm25, dense])[:pool]
        candidates = self._merge(fused, bm25, dense, stages)
        features = [with_lightgbm_features(c, search_input, expansion) for c in candidates]
        ranked = self._rerank(features, stages)

        start = (search_input.page - 1) * search_input.limit
        page = ranked[start : start + search_input.limit]
        degraded = any(status.startswith("failed") for status in stages.values())
        return PipelineOutput(
            total_hits=len(ranked),
            results=[candidate_to_hit(candidate) for candidate in page],
            query_language=expansion.language,
            degraded=degraded,
            stages=stages,
        )

    def validate(self, search_input: SearchInput) -> int:
        """Raise InvalidSearchRequest outside the limits; return page * limit."""
        query = search_input.query.strip()
        if not query:
            raise InvalidSearchRequest("query must not be empty")
        if len(query) > self._config.max_query_chars:
            raise InvalidSearchRequest(
                f"query is longer than {self._config.max_query_chars} characters"
            )
        if search_input.page < 1 or not 1 <= search_input.limit <= self._config.max_limit:
            raise InvalidSearchRequest(
                f"page must be >= 1 and limit between 1 and {self._config.max_limit}"
            )
        window = search_input.page * search_input.limit
        if window > self._config.max_result_window:
            raise InvalidSearchRequest(
                f"page * limit may not exceed {self._config.max_result_window}"
            )
        return window

    # ------------------------------------------------------------------ stages

    def _opensearch(self) -> Any:
        if self._client is None:
            from pgs_search.client.opensearch import get_opensearch_client

            self._client = get_opensearch_client(self._config)
        return self._client

    def _remaining(self, deadline: float | None) -> float:
        timeout = self._config.opensearch_timeout_seconds
        if deadline is None:
            return timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SearchTimeout("the request deadline passed")
        return min(timeout, remaining)

    def _bm25(
        self,
        variants: Sequence[str],
        size: int,
        filters: list[dict[str, Any]],
        deadline: float | None,
    ) -> list[dict[str, Any]]:
        timeout = self._remaining(deadline)
        try:
            return search_bm25(self._opensearch(), variants, size, filters, timeout)
        except ConnectionTimeout as exc:
            raise SearchTimeout(f"OpenSearch did not answer within {timeout:.1f} s") from exc
        except OpenSearchConnectionError as exc:
            raise SearchBackendUnavailable(f"OpenSearch is unreachable: {exc}") from exc
        except TransportError as exc:
            if isinstance(exc.status_code, int) and exc.status_code >= 500:
                raise SearchBackendUnavailable(f"OpenSearch failed: {exc}") from exc
            raise

    def _dense(
        self,
        query: str,
        search_input: SearchInput,
        size: int,
        deadline: float | None,
        stages: dict[str, str],
    ) -> list[Candidate]:
        if self._dense_search is None:
            stages["dense"] = "skipped: not configured"
            return []
        if deadline is not None and deadline - time.monotonic() < MIN_SECONDS_FOR_DENSE:
            stages["dense"] = "failed: no time left"
            return []
        try:
            results = self._dense_search(query, search_input, size)
        except Exception as exc:  # noqa: BLE001 -- optional stage: DB, model or embedding failure
            logger.warning("dense retrieval failed, continuing with BM25 only: %s", exc)
            stages["dense"] = f"failed: {type(exc).__name__}"
            return []
        stages["dense"] = "ok"
        return [{**result, "retrieval_sources": {"dense"}} for result in results]

    def _merge(
        self,
        fused: Sequence[tuple[str, float]],
        bm25: Sequence[Candidate],
        dense: Sequence[Candidate],
        stages: dict[str, str],
    ) -> list[Candidate]:
        by_id: dict[str, Candidate] = {}
        for candidate in [*bm25, *dense]:
            merge_candidate_data(by_id.setdefault(str(candidate["document_id"]), {}), candidate)
        missing = [document_id for document_id, _ in fused if not by_id[document_id].get("indexed")]
        if missing:
            try:
                hydrated = hydrate_candidates(self._opensearch(), missing, self._config)
            except (OpenSearchConnectionError, ConnectionTimeout, TransportError) as exc:
                logger.warning("could not load metadata for dense-only hits: %s", exc)
                stages["hydrate"] = f"failed: {type(exc).__name__}"
                hydrated = {}
            for document_id, metadata in hydrated.items():
                merge_candidate_data(by_id[document_id], metadata)
        merged = []
        for document_id, fusion_score in fused:
            candidate = by_id[document_id]
            # A page in pgvector but not (yet) in the index has no snippet or geo names:
            # it is left out until the indexer has caught up.
            if not candidate.get("indexed"):
                continue
            candidate["fusion_score"] = fusion_score
            merged.append(candidate)
        return merged

    def _rerank(self, candidates: list[Candidate], stages: dict[str, str]) -> list[Candidate]:
        if not candidates:
            return []
        if self._reranker is None or not self._config.rerank_enabled:
            stages["rerank"] = "skipped: disabled"
            return candidates
        try:
            ranked = self._reranker(candidates, len(candidates))
        except Exception as exc:  # noqa: BLE001 -- optional stage: keep the fusion order
            logger.warning("rerank failed, keeping the fusion order: %s", exc)
            stages["rerank"] = f"failed: {type(exc).__name__}"
            return candidates
        stages["rerank"] = "ok"
        return ranked


# ---------------------------------------------------------------------- helpers


def _active(value: str) -> str:
    value = value.strip()
    return "" if value.lower() in ALL_SENTINELS else value


def geo_constraints(search_input: SearchInput) -> dict[str, str | int]:
    constraints: dict[str, str | int] = {}
    for name in ("province_code", "district_code", "municipality_id"):
        value = getattr(search_input, name).strip()
        if value:
            constraints[name] = value
    if search_input.ward_number > 0:
        constraints["ward_number"] = search_input.ward_number
    return constraints


def build_filter_clauses(search_input: SearchInput) -> list[dict[str, Any]]:
    """Exact filters. Geo filters match any of a page's tags (`geo_tags`), each of which
    carries its full code chain, so a district matches pages tagged with any of its
    municipalities -- the same semantics as the pgvector search."""
    filters: list[dict[str, Any]] = [
        {"term": {f"geo_tags.{name}": value}}
        for name, value in geo_constraints(search_input).items()
    ]
    content_type = _active(search_input.content_type)
    if content_type:
        filters.append({"term": {"content_type": content_type}})
    language = _active(search_input.language)
    if language:
        filters.append({"term": {"language": language.lower()}})
    return filters


def _source_candidate(document_id: str, source: Mapping[str, Any]) -> Candidate:
    return {
        "document_id": str(source.get("document_id") or document_id),
        "indexed": True,
        "title": source.get("title") or "",
        "description": source.get("description") or "",
        "summary": source.get("summary") or "",
        "source_url": source.get("source_url") or "",
        "domain": source.get("domain") or "",
        "language": source.get("language") or "",
        "content_type": source.get("content_type") or "",
        "keywords": source.get("keywords") or [],
        "geo": _mapping(source.get("geo")),
        "published_at": source.get("published_at") or "",
        "content_length": source.get("content_length") or 0,
        "file_info": _mapping(source.get("file_info")),
    }


def normalize_bm25_hit(hit: Mapping[str, Any]) -> Candidate:
    """An OpenSearch hit in the internal candidate shape."""
    source = _mapping(hit.get("_source"))
    candidate = _source_candidate(str(hit.get("_id", "")), source)
    fragments = _mapping(hit.get("highlight")).get("searchable_text") or []
    if fragments:
        candidate["highlight"] = str(fragments[0])
    candidate["bm25_score"] = _safe_float(hit.get("_score"))
    candidate["retrieval_sources"] = {"bm25"}
    return candidate


def hydrate_candidates(
    client: Any, document_ids: Sequence[str], config: Settings = settings
) -> dict[str, Candidate]:
    """OpenSearch metadata for dense-only hits, in one mget."""
    unique_ids = list(
        dict.fromkeys(str(document_id) for document_id in document_ids if document_id)
    )
    if not unique_ids:
        return {}
    response = client.mget(
        index=config.opensearch_index,
        body={"ids": unique_ids},
        _source_excludes=["searchable_text"],
        request_timeout=config.opensearch_timeout_seconds,
    )
    hydrated: dict[str, Candidate] = {}
    for doc in response.get("docs", []) if isinstance(response, dict) else []:
        if isinstance(doc, dict) and doc.get("found"):
            candidate = _source_candidate(str(doc.get("_id", "")), _mapping(doc.get("_source")))
            hydrated[candidate["document_id"]] = candidate
    return hydrated


def merge_candidate_data(target: Candidate, incoming: Mapping[str, Any]) -> Candidate:
    """Merge `incoming` into `target`: best scores, union of sources, first non-empty values."""
    for key, value in incoming.items():
        if key == "retrieval_sources":
            target.setdefault("retrieval_sources", set()).update(value)
        elif key in {"bm25_score", "vector_score", "fusion_score", "rerank_score"}:
            target[key] = max(_safe_float(target.get(key)), _safe_float(value))
        elif value not in (None, "", [], {}) and target.get(key) in (None, "", [], {}):
            target[key] = value
    return target


def with_lightgbm_features(
    candidate: Candidate, search_input: SearchInput, expansion: QueryExpansion
) -> Candidate:
    """The reranker's features, from the candidate's metadata (no full text needed)."""
    enriched = dict(candidate)
    title = str(enriched.get("title", ""))
    text = " ".join(
        str(enriched.get(name) or "") for name in ("title", "description", "highlight", "summary")
    )
    terms = set(expansion.normalized.lower().split())
    enriched["bm25_score"] = _safe_float(enriched.get("bm25_score"))
    enriched["vector_score"] = _safe_float(enriched.get("vector_score"))
    enriched["title_match"] = 1 if terms & _terms(title) else 0
    enriched["geo_match"] = 1 if _geo_matches(enriched, search_input) else 0
    enriched["freshness"] = _freshness_score(enriched.get("published_at"))
    enriched["source_authority"] = 0
    requested_language = _active(search_input.language)
    if not requested_language:
        enriched["language_match"] = 0.5
    else:
        enriched["language_match"] = 1.0 if enriched.get("language") == requested_language else 0.0
    enriched["query_term_ratio"] = len(terms & _terms(text)) / len(terms) if terms else 0.0
    enriched["content_length"] = int(_safe_float(enriched.get("content_length")))
    return enriched


def candidate_to_hit(candidate: Mapping[str, Any]) -> PipelineHit:
    """A ranked candidate in the gRPC-facing hit shape."""
    snippet = (
        candidate.get("highlight")
        or candidate.get("description")
        or _shorten(str(candidate.get("summary") or ""))
    )
    file_info = _mapping(candidate.get("file_info"))
    return PipelineHit(
        id=str(candidate.get("document_id", "")),
        result_type=str(candidate.get("content_type") or "web_page"),
        title=str(candidate.get("title") or candidate.get("source_url") or ""),
        url=str(candidate.get("source_url") or ""),
        domain=str(candidate.get("domain") or ""),
        snippet=_shorten(str(snippet)),
        download_url="",
        file_size_bytes=int(_safe_float(file_info.get("size_bytes"))),
        relevance_score=_relevance(candidate),
        language=str(candidate.get("language") or ""),
        published_at=str(candidate.get("published_at") or ""),
        geo=_geo_ref(_mapping(candidate.get("geo"))),
    )


def _geo_ref(geo: Mapping[str, Any]) -> GeoRef | None:
    if not any(geo.get(code) for code in ("province_code", "district_code", "municipality_id")):
        return None
    return GeoRef(
        province_code=str(geo.get("province_code") or ""),
        province_name=str(geo.get("province_name_en") or ""),
        district_code=str(geo.get("district_code") or ""),
        district_name=str(geo.get("district_name_en") or ""),
        municipality_id=str(geo.get("municipality_id") or ""),
        municipality_name=str(geo.get("municipality_name_en") or ""),
        ward_number=int(_safe_float(geo.get("ward_number"))),
    )


def _geo_matches(candidate: Mapping[str, Any], search_input: SearchInput) -> bool:
    constraints = geo_constraints(search_input)
    if not constraints:
        return False
    geo = _mapping(candidate.get("geo"))
    return all(str(geo.get(name, "")) == str(value) for name, value in constraints.items())


def _terms(text: str) -> set[str]:
    return {part.lower() for part in text.split() if part.strip()}


def _freshness_score(value: Any) -> float:
    if not value:
        return 0.0
    try:
        published_at = datetime.fromisoformat(str(value))
    except ValueError:
        return 0.0
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=UTC)
    age_days = max((datetime.now(UTC) - published_at.astimezone(UTC)).days, 0)
    return 1 / (1 + age_days / 365)


def _relevance(candidate: Mapping[str, Any]) -> float:
    for key in ("rerank_score", "fusion_score", "bm25_score", "vector_score"):
        if key in candidate:
            return _safe_float(candidate.get(key))
    return 0.0


def _shorten(text: str, limit: int = SNIPPET_LENGTH) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else f"{text[: limit - 3].rstrip()}..."


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def default_pipeline(config: Settings = settings) -> FinalSearchPipeline:
    """The production wiring: OpenSearch, pgvector through pgs_db, LightGBM."""
    from pgs_db import make_session_factory

    from pgs_search.query.embeddings import get_query_embedder
    from pgs_search.ranking.lightgbm_reranker import rerank_results
    from pgs_search.retrieval.dense import DenseRetriever

    retriever = DenseRetriever(make_session_factory(), get_query_embedder())
    return FinalSearchPipeline(
        dense_search=retriever.search,
        reranker=rerank_results,
        config=config,
    )
