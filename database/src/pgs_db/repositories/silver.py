"""The only place that writes the Silver tables.

Takes the Spark ETL's output payload (`ETL/spark/README.md` §5.2) and lands it in
`pages` and the tables hanging off it: `page_geo_tags`, `page_contacts`,
`page_sources` (fetch history), `page_media`, `entities` / `page_entities` and
`page_embeddings`. See `docs/bronze-silver-contract.md`.

Reprocessing is the normal case, not the exception: Spark re-runs, Airflow backfills
and nightly deep-dedup all re-emit pages that already exist. Every write here is
keyed so that re-running produces the same rows rather than duplicates.

It also owns the Bronze -> Silver work queues (`claim_bronze`, `claim_stored_files`
and friends), since the ETL is the only consumer of `processing_status` on
`crawled_documents` and `stored_files`, and the stage-3 dedup lookups.
"""

import math
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from pgvector.sqlalchemy import Vector
from sqlalchemy import ColumnElement, and_, case, cast, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import BIT, insert
from sqlalchemy.orm import Session

from ..enums import (
    ContactType,
    EntityType,
    GeoTagMethod,
    Language,
    MediaType,
    ProcessingStatus,
    QuarantineStatus,
)
from ..models import (
    CrawledDocument,
    EmbeddingModel,
    Entity,
    LocalBody,
    Page,
    PageContact,
    PageEmbedding,
    PageEntity,
    PageGeoTag,
    PageMedia,
    PageSource,
    QuarantinedFile,
    StoredFile,
)
from ._mapping import blank_to_none, parse_timestamp, signed_simhash
from .bronze import SaveResult
from .reference import ReferenceRepository

# The two Bronze tables that carry an ETL work queue.
_QueueRow = TypeVar("_QueueRow", CrawledDocument, StoredFile)

# §5.2 emits contact_info as parallel lists; Silver normalizes them into rows.
_CONTACT_SOURCES: tuple[tuple[str, ContactType], ...] = (
    ("emails", ContactType.EMAIL),
    ("phones", ContactType.PHONE),
    ("social_links", ContactType.SOCIAL),
)

# §5.2 language_detected is a lowercase code; anything unrecognized is OTHER.
_LANGUAGES: dict[str, Language] = {
    "ne": Language.NE,
    "en": Language.EN,
    "mixed": Language.MIXED,
}


def to_language(raw: Any) -> Language:
    """Map §5.2 `language_detected` ("ne", "en-US", "mixed", ...) to `Language`."""
    value = blank_to_none(raw)
    if not isinstance(value, str):
        return Language.OTHER
    # "en-US" / "ne_NP" -> the primary subtag.
    primary = value.strip().lower().replace("_", "-").split("-")[0]
    return _LANGUAGES.get(primary, Language.OTHER)


def _media_rows(media: Any) -> list[dict[str, Any]]:
    """Validate a payload's `media` list; a URL listed twice keeps its last entry."""
    if not isinstance(media, Sequence) or isinstance(media, str):
        raise ValueError("media must be a list of objects")
    by_url: dict[str, dict[str, Any]] = {}
    for item in media:
        url = blank_to_none(item.get("url"))
        if not url:
            raise ValueError("media item is missing required field 'url'")
        raw_type = str(item.get("media_type") or "").upper()
        if raw_type not in MediaType.__members__:
            raise ValueError(f"media item has invalid media_type {item.get('media_type')!r}")
        by_url[str(url)] = {
            "url": str(url),
            "media_type": MediaType[raw_type],
            "alt_text": blank_to_none(item.get("alt_text")),
            "extracted_text": blank_to_none(item.get("extracted_text")),
            # The image indexer calls it surrounding_context.
            "context_text": blank_to_none(item.get("context_text"))
            or blank_to_none(item.get("surrounding_context")),
            "stored_file_id": item.get("stored_file_id"),
        }
    return list(by_url.values())


def _entity_key(entity_type: EntityType, name: str) -> str:
    return f"{entity_type.value}:{' '.join(name.casefold().split())}"[:255]


def _entity_rows(entities: Any) -> list[dict[str, Any]]:
    """Validate a payload's `entities` list; one entity named twice is merged."""
    if not isinstance(entities, Sequence) or isinstance(entities, str):
        raise ValueError("entities must be a list of objects")
    merged: dict[str, dict[str, Any]] = {}
    for item in entities:
        raw_type = str(item.get("type") or "").upper()
        if raw_type not in EntityType.__members__:
            raise ValueError(f"entity has invalid type {item.get('type')!r}")
        entity_type = EntityType[raw_type]
        name_en = blank_to_none(item.get("name_en"))
        name_ne = blank_to_none(item.get("name_ne"))
        name = blank_to_none(item.get("name")) or name_en or name_ne
        if not name:
            raise ValueError("entity needs a name, name_en or name_ne")
        if name_en is None and name_ne is None:
            name_en = name
        count = int(item.get("mention_count") or 1)
        if count < 1:
            raise ValueError("entity mention_count must be at least 1")
        salience = item.get("salience")
        if salience is not None and not 0 <= float(salience) <= 1:
            raise ValueError("entity salience must be between 0 and 1")
        key = str(blank_to_none(item.get("key")) or _entity_key(entity_type, str(name)))[:255]
        if key in merged:
            prev = merged[key]
            prev["mention_count"] += count
            if salience is not None:
                prev["salience"] = max(prev["salience"] or 0.0, float(salience))
            continue
        merged[key] = {
            "normalized_key": key,
            "type": entity_type,
            "name_en": name_en,
            "name_ne": name_ne,
            "mention_count": count,
            "salience": float(salience) if salience is not None else None,
        }
    return list(merged.values())


def _check_vector(vector: Any, dimensions: int | None = None) -> None:
    if not isinstance(vector, Sequence) or isinstance(vector, str) or not vector:
        raise ValueError("an embedding must be a non-empty list of numbers")
    if dimensions is not None and len(vector) != dimensions:
        raise ValueError(
            f"an embedding for this model must have exactly {dimensions} numbers, "
            f"got {len(vector)}"
        )
    if not all(isinstance(x, (int, float)) and math.isfinite(x) for x in vector):
        raise ValueError("an embedding must contain only finite numbers")


def _payload_simhash(payload: Mapping[str, Any]) -> int | None:
    """`sim_hash` (int) or the ETL's `simhash` (16 hex digits), as the signed BIGINT stored.

    Both are the 64-bit fingerprint; unsigned values are reinterpreted, not truncated.
    """
    raw = payload.get("sim_hash", payload.get("simhash"))
    if raw is None or raw == "":
        return None
    value = int(raw, 16) if isinstance(raw, str) else int(raw)
    if not -(2**63) <= value < 2**64:
        raise ValueError("simhash must be a 64-bit fingerprint")
    return signed_simhash(value)


def hnsw_index_name(model_name: str) -> str:
    """`ix_page_embeddings_hnsw_<model>`, within Postgres's 63-character limit."""
    slug = re.sub(r"[^a-z0-9]+", "_", model_name.lower()).strip("_")
    return f"ix_page_embeddings_hnsw_{slug}"[:63]


def _embedding_block(block: Any) -> tuple[str, str | None, list[dict[str, Any]]]:
    """Validate `{model_name, model_version?, chunks: [{text, vector, chunk_index?}]}`."""
    if not isinstance(block, Mapping):
        raise ValueError("embeddings must be an object with model_name and chunks")
    model_name = blank_to_none(block.get("model_name"))
    if not model_name:
        raise ValueError("embeddings is missing required field 'model_name'")
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for position, chunk in enumerate(block.get("chunks") or []):
        text_ = blank_to_none(chunk.get("text")) or blank_to_none(chunk.get("chunk_text"))
        if not text_:
            raise ValueError("embedding chunk is missing its 'text'")
        vector = chunk.get("vector", chunk.get("embedding"))
        _check_vector(vector)
        index = int(chunk.get("chunk_index", position))
        if index < 0 or index in seen:
            raise ValueError(f"embedding chunk_index {index} is negative or repeated")
        seen.add(index)
        rows.append(
            {
                "chunk_index": index,
                "chunk_text": str(text_),
                "embedding": [float(x) for x in vector],
            }
        )
    return str(model_name), blank_to_none(block.get("model_version")), rows


def _is_not_older(excluded: Any) -> ColumnElement[bool]:
    """Upsert guard: the incoming row may replace the page unless it is an older row
    of the same source table. A change of source table always replaces."""
    return or_(
        and_(
            Page.crawled_document_id.is_not(None),
            excluded.crawled_document_id.is_not(None),
            Page.crawled_document_id <= excluded.crawled_document_id,
        ),
        and_(
            Page.stored_file_id.is_not(None),
            excluded.stored_file_id.is_not(None),
            Page.stored_file_id <= excluded.stored_file_id,
        ),
        and_(Page.crawled_document_id.is_(None), excluded.crawled_document_id.is_not(None)),
        and_(Page.stored_file_id.is_(None), excluded.stored_file_id.is_not(None)),
    )


class SilverRepository:
    """Silver-layer writes for one SQLAlchemy session.

    The caller owns the transaction; nothing here commits, so one page plus its geo
    tags and contacts land as a single unit of work or not at all.
    """

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------- pages

    def save_page(
        self,
        payload: Mapping[str, Any],
        *,
        crawled_document_id: int | None = None,
        stored_file_id: int | None = None,
        domain_id: int | None = None,
        content_hash: str | None = None,
        sim_hash: int | None = None,
        replace_children: bool = True,
    ) -> SaveResult:
        """Upsert one Silver page from an ETL payload, with everything hanging off it.

        Keyed on `canonical_url`: every Bronze version of the same URL lands on one
        page, which is repointed at the newest Bronze row. The payload's
        `source_url` is optional; without it the Bronze row's `normalized_url` is used.
        `SaveResult.duplicate` is True when the page already existed.

        The source is exactly one of `crawled_document_id` (a crawled page) or
        `stored_file_id` (a PDF/image/... from `stored_files`, whose URL falls back to
        the file's `document_url`).

        "Newest" is the highest source id -- a content change inserts a new Bronze
        row, so ids grow with content versions. If an older row of the same source
        table is processed after a newer one (retries, backfills), the page is left
        untouched rather than rolled back to stale content. When the same URL was
        both crawled and stored as a file, the one processed last wins.

        Every call records the source in `page_sources` (fetch history), even when
        an older row leaves the page untouched. `version` goes up by one whenever
        the `content_hash` changes.

        `replace_children` re-derives geo tags and contacts from this payload,
        removing any that are no longer present -- so a reprocess that corrects a
        wrong geo tag actually corrects it instead of leaving both. `media`,
        `entities` and `embeddings` are replaced only when their key is in the
        payload, so a separate OCR, NER or embedding job's rows are not wiped by a
        payload that never mentioned them.
        """
        if (crawled_document_id is None) == (stored_file_id is None):
            raise ValueError("pass exactly one of crawled_document_id or stored_file_id")
        source_url, seen_at = self._source_info(crawled_document_id, stored_file_id)
        row = self._page_row(
            payload,
            crawled_document_id=crawled_document_id,
            stored_file_id=stored_file_id,
            fallback_url=source_url,
            domain_id=domain_id,
            content_hash=content_hash,
            sim_hash=sim_hash,
        )
        # Validate every child block before touching the database, so a bad payload
        # fails without a half-written page.
        geo_rows = self._geo_rows(payload.get("geo_location")) if replace_children else []
        media_rows = (
            _media_rows(payload["media"]) if replace_children and "media" in payload else None
        )
        entity_rows = (
            _entity_rows(payload["entities"])
            if replace_children and "entities" in payload
            else None
        )
        embedding_block = (
            self._checked_embedding_block(_embedding_block(payload["embeddings"]))
            if replace_children and "embeddings" in payload
            else None
        )

        previous_hash = self.session.scalar(
            select(Page.content_hash).where(Page.canonical_url == row["canonical_url"])
        )
        stmt = insert(Page).values(row)
        updates: dict[str, Any] = {
            col: getattr(stmt.excluded, col) for col in row if col != "canonical_url"
        }
        updates["last_seen_at"] = func.now()
        updates["version"] = case(
            (Page.content_hash != stmt.excluded.content_hash, Page.version + 1),
            else_=Page.version,
        )
        result = self.session.execute(
            stmt.on_conflict_do_update(
                index_elements=[Page.canonical_url],
                set_=updates,
                where=_is_not_older(stmt.excluded),
            ).returning(Page.id, text("(xmax = 0)"))
        ).one_or_none()

        if result is None:
            # Conflict, but the WHERE refused it: this Bronze row is older than the
            # one the page already reflects. Only its history is recorded.
            page_id = self.session.scalar(
                select(Page.id).where(Page.canonical_url == row["canonical_url"])
            )
            assert page_id is not None
            saved = SaveResult(id=int(page_id), inserted=False)
            self._record_source(
                saved.id, crawled_document_id, stored_file_id, seen_at, previous_hash, row
            )
            return saved

        saved = SaveResult(id=int(result[0]), inserted=bool(result[1]))
        self._record_source(
            saved.id, crawled_document_id, stored_file_id, seen_at, previous_hash, row
        )
        if replace_children:
            self._replace_geo_rows(saved.id, geo_rows)
            self.replace_contacts(saved.id, payload)
            if media_rows is not None:
                self._replace_media_rows(saved.id, media_rows)
            if entity_rows is not None:
                self._replace_entity_rows(saved.id, entity_rows)
            if embedding_block is not None:
                model_name, model_version, chunks = embedding_block
                self._replace_embedding_rows(
                    saved.id, model_name, model_version, chunks, str(row["content_hash"])
                )
        return saved

    def _source_info(
        self, crawled_document_id: int | None, stored_file_id: int | None
    ) -> tuple[str | None, datetime | None]:
        """The source row's URL and fetch time, or (None, None) if it does not exist."""
        if crawled_document_id is not None:
            stmt = select(CrawledDocument.normalized_url, CrawledDocument.fetched_at).where(
                CrawledDocument.id == crawled_document_id
            )
        else:
            stmt = select(StoredFile.document_url, StoredFile.stored_at).where(
                StoredFile.id == stored_file_id
            )
        found = self.session.execute(stmt).one_or_none()
        return (found[0], found[1]) if found else (None, None)

    def _record_source(
        self,
        page_id: int,
        crawled_document_id: int | None,
        stored_file_id: int | None,
        seen_at: datetime | None,
        previous_hash: str | None,
        row: Mapping[str, Any],
    ) -> None:
        self.session.execute(
            insert(PageSource)
            .values(
                page_id=page_id,
                crawled_document_id=crawled_document_id,
                stored_file_id=stored_file_id,
                fetched_at=seen_at or func.now(),
                content_changed=previous_hash is None or previous_hash != row["content_hash"],
            )
            .on_conflict_do_nothing(constraint="uq_page_sources_page_source")
        )

    def _page_row(
        self,
        payload: Mapping[str, Any],
        *,
        crawled_document_id: int | None,
        stored_file_id: int | None,
        fallback_url: str | None,
        domain_id: int | None,
        content_hash: str | None,
        sim_hash: int | None,
    ) -> dict[str, Any]:
        meta = payload.get("extracted_metadata") or {}

        # §5.2 calls it source_url; the column is canonical_url. The ETL may leave it
        # out: the Bronze row's normalized_url is already the scraper's identity for
        # the page (its declared <link rel="canonical"> if it had one); a stored
        # file's is its document_url.
        canonical_url = (
            blank_to_none(payload.get("canonical_url"))
            or blank_to_none(payload.get("source_url"))
            or fallback_url
        )
        if not canonical_url:
            source = (
                f"crawled_document {crawled_document_id}"
                if crawled_document_id is not None
                else f"stored_file {stored_file_id}"
            )
            raise ValueError(
                f"no URL for the page: the payload has no 'source_url' and "
                f"{source} does not exist"
            )

        # §5.2 calls it searchable_text.
        body_text = blank_to_none(payload.get("body_text")) or blank_to_none(
            payload.get("searchable_text")
        )
        if not body_text:
            raise ValueError("ETL payload is missing required field 'searchable_text'")
        body_text = str(body_text)

        resolved_hash = blank_to_none(content_hash) or blank_to_none(payload.get("content_hash"))
        if not resolved_hash:
            raise ValueError("a Silver page needs a content_hash for deduplication")

        word_count = payload.get("word_count")
        if word_count is None:
            word_count = len(body_text.split())

        published_raw = payload.get("published_at")
        published_at = (
            parse_timestamp(published_raw, "published_at")
            if blank_to_none(published_raw) is not None
            else None
        )

        language_confidence = payload.get("language_confidence")
        if language_confidence is not None and not 0 <= float(language_confidence) <= 1:
            raise ValueError("language_confidence must be between 0 and 1")

        return {
            # Both keys always, so switching a page's source clears the other one.
            "crawled_document_id": crawled_document_id,
            "stored_file_id": stored_file_id,
            "domain_id": domain_id,
            "canonical_url": str(canonical_url),
            # §5.2 nests these in extracted_metadata; the ETL's transform.py sends
            # them top-level. Either is accepted.
            "title": blank_to_none(meta.get("title")) or blank_to_none(payload.get("title")),
            "description": blank_to_none(meta.get("description"))
            or blank_to_none(payload.get("description")),
            "body_text": body_text,
            "word_count": int(word_count),
            "keywords": meta.get("keywords") or payload.get("keywords") or None,
            # §5.2 calls it language_detected; SearchDocument calls it language.
            "language": to_language(
                payload.get("language_detected") or payload.get("language")
            ),
            "language_confidence": (
                float(language_confidence) if language_confidence is not None else None
            ),
            "content_type": blank_to_none(payload.get("content_type")) or "web_page",
            "category": blank_to_none(payload.get("category")),
            "author": blank_to_none(payload.get("author")) or blank_to_none(meta.get("author")),
            "published_at": published_at,
            "quality_flags": list(payload.get("quality_flags") or []) or None,
            "content_hash": str(resolved_hash),
            "sim_hash": sim_hash if sim_hash is not None else _payload_simhash(payload),
            # New or changed content has to be (re)indexed.
            "processing_status": ProcessingStatus.UNPROCESSED,
            "processing_error": None,
        }

    # ------------------------------------------------------------------- media

    def replace_media(self, page_id: int, media: Sequence[Mapping[str, Any]]) -> list[int]:
        """Replace a page's images, videos and linked documents (with OCR/parsed text).

        Each item: `url`, `media_type` (image / video / document), and optional
        `alt_text`, `extracted_text`, `context_text` (or `surrounding_context`),
        `stored_file_id`. Without `stored_file_id` the newest stored file with that
        `document_url` is linked, if there is one.
        """
        return self._replace_media_rows(page_id, _media_rows(media))

    def _replace_media_rows(self, page_id: int, rows: list[dict[str, Any]]) -> list[int]:
        self.session.execute(delete(PageMedia).where(PageMedia.page_id == page_id))
        if not rows:
            return []
        for row in rows:
            if row["stored_file_id"] is None:
                row["stored_file_id"] = self.session.scalar(
                    select(StoredFile.id)
                    .where(StoredFile.document_url == row["url"])
                    .order_by(StoredFile.id.desc())
                    .limit(1)
                )
        return list(
            self.session.scalars(
                insert(PageMedia)
                .values([{"page_id": page_id, **row} for row in rows])
                .returning(PageMedia.id)
            )
        )

    # ---------------------------------------------------------------- entities

    def replace_entities(
        self, page_id: int, entities: Sequence[Mapping[str, Any]]
    ) -> list[int]:
        """Replace a page's named entities (NER output). Returns `page_entities` ids.

        Each item: `type` (person / organization / event / other), a name
        (`name`, `name_en` and/or `name_ne`), and optional `mention_count`,
        `salience` (0-1) and `key`. The entity itself is shared across pages:
        it is found or created by `key`, or by `TYPE:casefolded name`.
        """
        return self._replace_entity_rows(page_id, _entity_rows(entities))

    def _replace_entity_rows(self, page_id: int, rows: list[dict[str, Any]]) -> list[int]:
        self.session.execute(delete(PageEntity).where(PageEntity.page_id == page_id))
        if not rows:
            return []
        links = []
        for row in rows:
            stmt = insert(Entity).values(
                normalized_key=row["normalized_key"],
                type=row["type"],
                name_en=row["name_en"],
                name_ne=row["name_ne"],
            )
            entity_id = self.session.scalar(
                stmt.on_conflict_do_update(
                    index_elements=[Entity.normalized_key],
                    # Keep names already known; fill the ones that were missing.
                    set_={
                        "name_en": func.coalesce(Entity.name_en, stmt.excluded.name_en),
                        "name_ne": func.coalesce(Entity.name_ne, stmt.excluded.name_ne),
                    },
                ).returning(Entity.id)
            )
            links.append(
                {
                    "page_id": page_id,
                    "entity_id": entity_id,
                    "mention_count": row["mention_count"],
                    "salience": row["salience"],
                }
            )
        return list(
            self.session.scalars(insert(PageEntity).values(links).returning(PageEntity.id))
        )

    # -------------------------------------------------------------- embeddings

    def replace_embeddings(
        self,
        page_id: int,
        model_name: str,
        chunks: Sequence[Mapping[str, Any]],
        *,
        model_version: str | None = None,
    ) -> int:
        """Replace one model's vectors for a page. Returns the number of chunks stored.

        Each chunk: `text` and `vector` (the model's dimensions), optional
        `chunk_index` (defaults to its position). Other models' vectors are kept.
        The page's current `content_hash` is recorded, which is how
        `pages_missing_embeddings` spots vectors made from older text.
        """
        page_hash = self.session.scalar(select(Page.content_hash).where(Page.id == page_id))
        if page_hash is None:
            raise LookupError(f"page {page_id} does not exist")
        name, version, rows = self._checked_embedding_block(
            _embedding_block(
                {"model_name": model_name, "model_version": model_version, "chunks": chunks}
            )
        )
        return self._replace_embedding_rows(page_id, name, version, rows, page_hash)

    # --------------------------------------------------------- embedding models

    def embedding_model(self, name: str | None = None) -> EmbeddingModel:
        """A registered model by name, or the default model when `name` is None.

        A bare Hugging Face name (`all-MiniLM-L6-v2`) also finds its
        `sentence-transformers/` entry. Raises ValueError for an unknown model.
        """
        if name is None:
            model = self.session.scalar(
                select(EmbeddingModel).where(EmbeddingModel.is_default.is_(True))
            )
            if model is None:
                raise ValueError("no default embedding model is registered")
            return model
        candidates = [name] if "/" in name else [name, f"sentence-transformers/{name}"]
        model = self.session.scalar(
            select(EmbeddingModel)
            .where(EmbeddingModel.name.in_(candidates))
            .order_by(EmbeddingModel.name != name)
            .limit(1)
        )
        if model is None:
            raise ValueError(
                f"unknown embedding model {name!r}: register it with register_embedding_model"
            )
        return model

    def register_embedding_model(
        self,
        name: str,
        dimensions: int,
        *,
        description: str | None = None,
        make_default: bool = False,
    ) -> EmbeddingModel:
        """Register a model and create its HNSW index. Needs the schema owner's rights.

        Safe to re-run. `make_default` moves the default to this model: the ETL and
        search should switch together, or queries won't match documents.
        """
        if not 1 <= dimensions <= 2000:
            raise ValueError("dimensions must be between 1 and 2000 (HNSW's limit)")
        existing = self.session.scalar(select(EmbeddingModel).where(EmbeddingModel.name == name))
        if existing is not None and existing.dimensions != dimensions:
            raise ValueError(f"{name} is registered with {existing.dimensions} dimensions")
        if make_default:
            self.session.execute(
                update(EmbeddingModel)
                .where(EmbeddingModel.is_default.is_(True))
                .values(is_default=False)
            )
            self.session.flush()
        if existing is None:
            existing = EmbeddingModel(name=name, dimensions=dimensions, description=description)
            self.session.add(existing)
        existing.is_default = existing.is_default or make_default
        self.session.flush()
        literal_name = name.replace("'", "''")
        self.session.execute(
            text(
                f"CREATE INDEX IF NOT EXISTS {hnsw_index_name(name)} ON page_embeddings "
                f"USING hnsw ((embedding::vector({int(dimensions)})) vector_cosine_ops) "
                f"WHERE model_name = '{literal_name}'"
            )
        )
        return existing

    def _checked_embedding_block(
        self, block: tuple[str, str | None, list[dict[str, Any]]]
    ) -> tuple[str, str | None, list[dict[str, Any]]]:
        """Resolve the block's model and check every vector has its dimensions."""
        name, version, rows = block
        model = self.embedding_model(name)
        for row in rows:
            _check_vector(row["embedding"], model.dimensions)
        return model.name, version, rows

    def _replace_embedding_rows(
        self,
        page_id: int,
        model_name: str,
        model_version: str | None,
        chunks: list[dict[str, Any]],
        content_hash: str,
    ) -> int:
        self.session.execute(
            delete(PageEmbedding).where(
                PageEmbedding.page_id == page_id, PageEmbedding.model_name == model_name
            )
        )
        if chunks:
            self.session.execute(
                insert(PageEmbedding).values(
                    [
                        {
                            "page_id": page_id,
                            "model_name": model_name,
                            "model_version": model_version,
                            "content_hash": content_hash,
                            **chunk,
                        }
                        for chunk in chunks
                    ]
                )
            )
        return len(chunks)

    def pages_missing_embeddings(self, model_name: str, limit: int = 100) -> list[Page]:
        """Canonical pages with no vectors from `model_name` for their current text.

        The work list for an embedding job: new pages, and pages whose content
        changed since they were embedded. Duplicates are skipped -- search shows
        their canonical page.
        """
        model_name = self.embedding_model(model_name).name
        current = (
            select(PageEmbedding.id)
            .where(
                PageEmbedding.page_id == Page.id,
                PageEmbedding.model_name == model_name,
                PageEmbedding.content_hash == Page.content_hash,
            )
            .exists()
        )
        return list(
            self.session.scalars(
                select(Page)
                .where(Page.duplicate_of_id.is_(None), ~current)
                .order_by(Page.id)
                .limit(limit)
            )
        )

    def nearest_chunks(
        self, vector: Sequence[float], model_name: str, *, limit: int = 10
    ) -> list[tuple[PageEmbedding, float]]:
        """The chunks closest to `vector` by cosine distance (0 = identical), nearest first.

        Uses the model's HNSW index (the cast below matches its expression). Only
        canonical pages are searched. The query vector must come from the same model.
        """
        model = self.embedding_model(model_name)
        _check_vector(vector, model.dimensions)
        typed = cast(PageEmbedding.embedding, Vector(model.dimensions))
        distance = typed.cosine_distance(list(vector)).label("distance")
        rows = self.session.execute(
            select(PageEmbedding, distance)
            .join(Page, Page.id == PageEmbedding.page_id)
            .where(PageEmbedding.model_name == model.name, Page.duplicate_of_id.is_(None))
            .order_by(distance)
            .limit(limit)
        ).all()
        return [(chunk, float(dist)) for chunk, dist in rows]

    # ----------------------------------------------------------------- history

    def page_history(self, page_id: int) -> list[PageSource]:
        """Every Bronze row that fed this page, oldest fetch first."""
        return list(
            self.session.scalars(
                select(PageSource)
                .where(PageSource.page_id == page_id)
                .order_by(PageSource.fetched_at, PageSource.id)
            )
        )

    def mark_duplicate_of(self, page_id: int, canonical_page_id: int) -> None:
        """Fold a page into a canonical record (ETL stage 3 fuzzy/exact dedup)."""
        if page_id == canonical_page_id:
            raise ValueError("a page cannot be a duplicate of itself")
        page = self.session.get(Page, page_id)
        if page is None:
            raise LookupError(f"page {page_id} does not exist")
        page.duplicate_of_id = canonical_page_id

    # --------------------------------------------------------------- geo tags

    def replace_geo_tags(
        self, page_id: int, geo: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None
    ) -> list[int]:
        """Re-derive a page's geo tags from the payload's `geo_location` block.

        Accepts one block (§5.2) or a list, since a news article can legitimately be
        about several places. Codes are stored; names come from the gazetteer.
        """
        return self._replace_geo_rows(page_id, self._geo_rows(geo))

    def _geo_rows(
        self, geo: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None
    ) -> list[dict[str, Any]]:
        blocks: Sequence[Mapping[str, Any]]
        if geo is None:
            blocks = []
        elif isinstance(geo, Mapping):
            blocks = [geo]
        else:
            blocks = list(geo)
        return [row for block in blocks if (row := self._geo_row(block)) is not None]

    def _replace_geo_rows(self, page_id: int, rows: list[dict[str, Any]]) -> list[int]:
        self.session.execute(delete(PageGeoTag).where(PageGeoTag.page_id == page_id))
        if not rows:
            return []
        stmt = insert(PageGeoTag).values([{"page_id": page_id, **row} for row in rows])
        saved = self.session.execute(
            stmt.on_conflict_do_nothing(
                constraint="uq_page_geo_tags_page_location"
            ).returning(PageGeoTag.id)
        ).scalars()
        return list(saved)

    def _geo_row(self, block: Mapping[str, Any]) -> dict[str, Any] | None:
        province_code = blank_to_none(block.get("province_code"))
        district_code = blank_to_none(block.get("district_code"))
        # §5.2 calls it municipality_id; it is local_bodies.code (e.g. MUN414).
        local_body_code = blank_to_none(block.get("local_body_code")) or blank_to_none(
            block.get("municipality_id")
        )
        mention_text = blank_to_none(block.get("mention_text"))
        if province_code is None and district_code is None and local_body_code is None:
            # Place names instead of codes (the ETL's resolve_geo): look them up.
            names = {
                level: blank_to_none(block.get(level))
                for level in ("province", "district", "municipality")
            }
            if any(names.values()):
                codes = ReferenceRepository(self.session).codes_for_names(**names)
                if codes is not None:
                    province_code = codes["province_code"]
                    district_code = codes["district_code"]
                    local_body_code = codes["municipality_id"]
                    mention_text = mention_text or next(v for v in names.values() if v)
        if province_code is None and district_code is None and local_body_code is None:
            # Nothing resolved -- the CHECK would reject it, so skip rather than fail:
            # an untagged page is normal, not an error.
            return None

        # A resolved tag must say how it was resolved and how sure the ETL is.
        # No defaults: a silently-invented confidence would poison ranking.
        raw_method = blank_to_none(block.get("method"))
        if raw_method is None and block.get("source") == "seed_gazetteer":
            raw_method = GeoTagMethod.GAZETTEER.value  # the ETL's resolve_geo says so
        if raw_method is None:
            raise ValueError("geo tag is missing required field 'method'")
        try:
            method = GeoTagMethod(str(raw_method).upper())
        except ValueError:
            raise ValueError(f"unknown geo tag method {raw_method!r}") from None

        raw_confidence = block.get("confidence")
        if raw_confidence is None:
            raise ValueError("geo tag is missing required field 'confidence'")
        confidence = float(raw_confidence)
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"geo tag confidence must be within 0..1, got {confidence}")

        ward = int(block["ward_number"]) if block.get("ward_number") is not None else None
        if ward is not None and local_body_code is not None:
            ward_count = self.session.scalar(
                select(LocalBody.ward_count).where(LocalBody.code == local_body_code)
            )
            if ward_count is not None and not 1 <= ward <= ward_count:
                raise ValueError(
                    f"ward {ward} does not exist in {local_body_code} ({ward_count} wards)"
                )
        return {
            "province_code": province_code,
            "district_code": district_code,
            "local_body_code": local_body_code,
            "ward_number": ward,
            "method": method,
            "confidence": confidence,
            "mention_text": mention_text,
        }

    # --------------------------------------------------------------- contacts

    def replace_contacts(self, page_id: int, payload: Mapping[str, Any]) -> list[int]:
        """Re-derive a page's contacts from `extracted_metadata.contact_info`.

        Also accepts `social_links` beside `contact_info`, matching how Bronze keeps
        the three lists (`emails`, `phones`, `social_links`) on `crawled_documents`.
        """
        meta = payload.get("extracted_metadata") or {}
        contact_info = meta.get("contact_info") or {}

        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for key, contact_type in _CONTACT_SOURCES:
            values = contact_info.get(key)
            if values is None:
                values = meta.get(key)
            for raw in values or []:
                value = blank_to_none(raw)
                if value is None:
                    continue
                dedupe_key = (contact_type.value, str(value))
                if dedupe_key in seen:  # the same address listed twice on one page
                    continue
                seen.add(dedupe_key)
                rows.append({"page_id": page_id, "type": contact_type, "value": str(value)})

        self.session.execute(delete(PageContact).where(PageContact.page_id == page_id))
        if not rows:
            return []
        stmt = insert(PageContact).values(rows)
        saved = self.session.execute(
            stmt.on_conflict_do_nothing(
                constraint="uq_page_contacts_page_type_value"
            ).returning(PageContact.id)
        ).scalars()
        return list(saved)

    # ------------------------------------------------------ bronze work queue
    #
    # Two queues with the same shape: crawled pages (`crawled_documents`) and files
    # the scraper stored in MinIO (`stored_files`: PDFs, images, ...). Both move
    # UNPROCESSED -> PROCESSING -> PROCESSED | FAILED.

    def claim_bronze(self, limit: int = 100) -> list[CrawledDocument]:
        """Claim up to `limit` UNPROCESSED Bronze rows for this worker, oldest first.

        `FOR UPDATE SKIP LOCKED` lets several ETL workers claim concurrently without
        blocking on or double-claiming each other's rows. Claimed rows move to
        PROCESSING; commit promptly so other workers stop seeing them as locked
        candidates. `updated_at` doubles as the claim time for `release_stale`.
        """
        return self._claim(CrawledDocument, CrawledDocument.fetched_at, limit)

    def claim_stored_files(self, limit: int = 100) -> list[StoredFile]:
        """`claim_bronze` for stored files, oldest `stored_at` first.

        Save each as a page with `save_page(..., stored_file_id=file.id)`; the file
        bytes are at `file.storage_path` in MinIO.
        """
        return self._claim(StoredFile, StoredFile.stored_at, limit)

    def _claim(self, model: type[_QueueRow], order: Any, limit: int) -> list[_QueueRow]:
        # A MATERIALIZED CTE runs exactly once. As `WHERE id IN (subquery)`, the
        # planner may re-run the LIMIT ... SKIP LOCKED subquery per row and claim
        # more than `limit` rows.
        candidates = (
            select(model.id)
            .where(model.processing_status == ProcessingStatus.UNPROCESSED)
            .order_by(order, model.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .cte("candidates")
            .prefix_with("MATERIALIZED")
        )
        claimed = self.session.scalars(
            update(model)
            .where(model.id == candidates.c.id)
            .values(
                processing_status=ProcessingStatus.PROCESSING,
                processing_error=None,
                updated_at=func.now(),
            )
            .returning(model),
            execution_options={"synchronize_session": False},
        ).all()
        # UPDATE ... RETURNING does not preserve the subquery's order.
        key = order.key
        return sorted(claimed, key=lambda row: (getattr(row, key), row.id))

    def mark_bronze_processed(self, ids: Iterable[int]) -> int:
        """Mark Bronze rows done after their pages are saved. Returns rows updated."""
        return self._mark_processed(CrawledDocument, ids)

    def mark_stored_files_processed(self, ids: Iterable[int]) -> int:
        """Mark stored files done after their pages are saved. Returns rows updated."""
        return self._mark_processed(StoredFile, ids)

    def _mark_processed(self, model: type[_QueueRow], ids: Iterable[int]) -> int:
        id_list = list(ids)
        if not id_list:
            return 0
        result = self.session.execute(
            update(model)
            .where(model.id.in_(id_list))
            .values(processing_status=ProcessingStatus.PROCESSED, processing_error=None),
            execution_options={"synchronize_session": False},
        )
        return result.rowcount

    def mark_bronze_failed(self, crawled_document_id: int, error: str) -> None:
        """Park a Bronze row the ETL could not process, with the reason."""
        self._mark_failed(CrawledDocument, crawled_document_id, error, "crawled document")

    def mark_stored_file_failed(self, stored_file_id: int, error: str) -> None:
        """Park a stored file the ETL could not process (unreadable PDF, ...)."""
        self._mark_failed(StoredFile, stored_file_id, error, "stored file")

    def _mark_failed(self, model: type[_QueueRow], row_id: int, error: str, what: str) -> None:
        result = self.session.execute(
            update(model)
            .where(model.id == row_id)
            .values(processing_status=ProcessingStatus.FAILED, processing_error=error),
            execution_options={"synchronize_session": False},
        )
        if result.rowcount == 0:
            raise LookupError(f"{what} {row_id} does not exist")

    def release_stale(self, older_than: timedelta) -> int:
        """Return PROCESSING claims older than `older_than` to both queues.

        Recovers rows whose worker died after committing its claim. Pick a window
        well above the slowest batch, or a live worker's rows get double-processed
        (harmless -- `save_page` is idempotent -- but wasted work). Returns the
        number of rows released across both tables.
        """
        released = 0
        for model in (CrawledDocument, StoredFile):
            result = self.session.execute(
                update(model)
                .where(
                    model.processing_status == ProcessingStatus.PROCESSING,
                    model.updated_at < func.now() - older_than,
                )
                .values(processing_status=ProcessingStatus.UNPROCESSED),
                execution_options={"synchronize_session": False},
            )
            released += result.rowcount
        return released

    # ------------------------------------------------------------ quarantine

    def quarantine(
        self,
        *,
        threat_signature: str,
        quarantine_path: str,
        crawled_document_id: int | None = None,
        stored_file_id: int | None = None,
        scanner_engine: str = "ClamAV",
        scanner_version: str | None = None,
        scanned_at: datetime | None = None,
    ) -> SaveResult:
        """Record that ClamAV flagged a claimed Bronze row, and take it out of the pipeline.

        Call after moving the object to the quarantine bucket (`quarantine_path`).
        URL, hash, size and domain are copied from the Bronze row. The row is set
        to QUARANTINED, so it is never claimed again, and a page already built from
        it is deleted, so search never links to the file. Rescanning the same file
        (same `document_url` + `sha256`) updates its record; if an admin had
        deleted it, it is quarantined again.
        """
        if (crawled_document_id is None) == (stored_file_id is None):
            raise ValueError("pass exactly one of crawled_document_id or stored_file_id")
        if not blank_to_none(threat_signature):
            raise ValueError("quarantine needs a threat_signature")
        if not blank_to_none(quarantine_path):
            raise ValueError("quarantine needs the quarantine_path of the isolated object")

        model: type[CrawledDocument] | type[StoredFile]
        if crawled_document_id is not None:
            doc = self.session.get(CrawledDocument, crawled_document_id)
            if doc is None:
                raise LookupError(f"crawled document {crawled_document_id} does not exist")
            model, row_id = CrawledDocument, doc.id
            details = {
                "document_url": doc.url,
                "source_page_url": None,
                "original_path": doc.minio_path,
                "sha256": doc.content_hash,
                "size_bytes": None,
                "content_type": doc.content_type,
                "domain_id": doc.domain_id,
            }
        else:
            stored = self.session.get(StoredFile, stored_file_id)
            if stored is None:
                raise LookupError(f"stored file {stored_file_id} does not exist")
            model, row_id = StoredFile, stored.id
            details = {
                "document_url": stored.document_url,
                "source_page_url": stored.source_page_url,
                "original_path": stored.storage_path,
                "sha256": stored.sha256,
                "size_bytes": stored.size_bytes,
                "content_type": stored.content_type,
                "domain_id": (
                    stored.crawled_document.domain_id if stored.crawled_document else None
                ),
            }

        values = {
            **details,
            "crawled_document_id": crawled_document_id,
            "stored_file_id": stored_file_id,
            "quarantine_path": quarantine_path,
            "threat_signature": threat_signature,
            "scanner_engine": scanner_engine,
            "scanner_version": scanner_version,
            "scanned_at": scanned_at or datetime.now(UTC),
            "status": QuarantineStatus.QUARANTINED,
            "deleted_at": None,
            "deleted_by": None,
        }
        stmt = insert(QuarantinedFile).values(values)
        found = self.session.execute(
            stmt.on_conflict_do_update(
                constraint="uq_quarantined_files_document_url_sha256",
                set_={
                    col: getattr(stmt.excluded, col)
                    for col in values
                    if col not in ("document_url", "sha256")
                },
            ).returning(QuarantinedFile.id, text("(xmax = 0)"))
        ).one()

        self.session.execute(
            update(model)
            .where(model.id == row_id)
            .values(
                processing_status=ProcessingStatus.QUARANTINED,
                processing_error=f"quarantined: {threat_signature}",
            ),
            execution_options={"synchronize_session": False},
        )
        source_column = (
            Page.crawled_document_id if crawled_document_id is not None else Page.stored_file_id
        )
        self.session.execute(
            delete(Page).where(source_column == row_id),
            execution_options={"synchronize_session": False},
        )
        return SaveResult(id=int(found[0]), inserted=bool(found[1]))

    # ------------------------------------------------------ deduplication

    def find_exact_duplicate(
        self, content_hash: str, *, exclude_page_id: int | None = None
    ) -> Page | None:
        """ETL stage 3, exact match: the canonical page with this `content_hash`.

        Returns the oldest page that is not itself a duplicate, or None. Fold the new
        page into it with `mark_duplicate_of(new_id, found.id)`.
        """
        stmt = select(Page).where(
            Page.content_hash == content_hash, Page.duplicate_of_id.is_(None)
        )
        if exclude_page_id is not None:
            stmt = stmt.where(Page.id != exclude_page_id)
        return self.session.scalar(stmt.order_by(Page.id).limit(1))

    def find_near_duplicates(
        self,
        sim_hash: int,
        *,
        max_distance: int = 3,
        exclude_page_id: int | None = None,
        limit: int = 10,
    ) -> list[tuple[Page, int]]:
        """ETL stage 3, fuzzy match: canonical pages within `max_distance` SimHash bits.

        Returns `(page, distance)` pairs, closest first. The default of 3 differing
        bits out of 64 is the ETL spec's ">95% similar". `sim_hash` is the signed
        int64 stored in Postgres (the scraper's uint64, reinterpreted).

        This scans every page with a sim_hash: fine at thousands of pages, but at
        millions switch to band indexing (split the 64 bits into blocks and index
        each) before relying on it inside Spark.
        """
        distance = func.bit_count(Page.sim_hash.op("#")(sim_hash).cast(BIT(64))).label(
            "distance"
        )
        stmt = (
            select(Page, distance)
            .where(Page.sim_hash.is_not(None), Page.duplicate_of_id.is_(None))
            .where(distance <= max_distance)
            .order_by(distance, Page.id)
            .limit(limit)
        )
        if exclude_page_id is not None:
            stmt = stmt.where(Page.id != exclude_page_id)
        return [(page, int(dist)) for page, dist in self.session.execute(stmt).all()]

    # ------------------------------------------------------ page -> indexer

    def page_for_bronze_document(self, crawled_document_id: int) -> Page | None:
        return self.session.scalar(
            select(Page).where(Page.crawled_document_id == crawled_document_id)
        )

    def page_for_stored_file(self, stored_file_id: int) -> Page | None:
        return self.session.scalar(select(Page).where(Page.stored_file_id == stored_file_id))

    def mark_processed(
        self, page_id: int, *, when: datetime | None = None, error: str | None = None
    ) -> None:
        """Record the outcome of downstream processing (search indexing)."""
        page = self.session.get(Page, page_id)
        if page is None:
            raise LookupError(f"page {page_id} does not exist")
        page.processing_status = (
            ProcessingStatus.FAILED if error else ProcessingStatus.PROCESSED
        )
        page.processing_error = error
        page.updated_at = when or datetime.now(UTC)
