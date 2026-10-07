"""The rest of Silver: page history, media, entities, embeddings, the new page
columns, and the ready-made ETL loop in `pgs_db.etl`. Real PostgreSQL + pgvector."""

import math
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from pgs_db import BronzeRepository, ReferenceRepository, SilverRepository, make_engine
from pgs_db.enums import EntityType, MediaType, ProcessingStatus
from pgs_db.etl import process_bronze_batch, process_stored_file_batch
from pgs_db.models import (
    CrawledDocument,
    Entity,
    Page,
    PageEmbedding,
    PageEntity,
    PageMedia,
    StoredFile,
)
from pgs_db.schemas import PageBase, PageWithRelations

HOST = "pokharamun.gov.np"
_ANCIENT = datetime(1980, 1, 1, tzinfo=UTC)


@pytest.fixture()
def bronze(session: Session) -> BronzeRepository:
    return BronzeRepository(session)


@pytest.fixture()
def silver(session: Session) -> SilverRepository:
    return SilverRepository(session)


def bronze_doc(n: int = 1, **overrides: Any) -> dict[str, Any]:
    url = f"https://{HOST}/notice/{n}"
    doc: dict[str, Any] = {
        "url": url,
        "normalized_url": url,
        "host": HOST,
        "title": "Notice",
        "text": "text",
        "content_hash": f"{n:064x}",
        "content_type": "text/html",
        "fetched_at": (_ANCIENT + timedelta(days=n)).isoformat(),
        "status_code": 200,
        "depth": 1,
    }
    doc.update(overrides)
    return doc


def payload(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "searchable_text": "Pokhara Metropolitan City budget notice for ward 5",
        "language_detected": "en",
        "extracted_metadata": {"title": "Budget notice"},
        "content_hash": "a" * 64,
    }
    body.update(overrides)
    return body


# Registered models (migration f6a7b8c9d0e1).
MINI = "sentence-transformers/all-MiniLM-L6-v2"
MULTI = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
MINI_DIM = 384

def vec(*hot: int) -> list[float]:
    """A unit-ish vector with 1.0 at the given positions."""
    v = [0.0] * MINI_DIM
    for i in hot:
        v[i] = 1.0
    return v


def save(
    silver: SilverRepository, bronze: BronzeRepository, n: int = 1, **kw: Any
) -> tuple[int, int]:
    """Save Bronze row n and a page from it; returns (page_id, crawled_document_id)."""
    doc_id = bronze.save_document(bronze_doc(n)).id
    return silver.save_page(payload(**kw), crawled_document_id=doc_id).id, doc_id


class TestPageColumns:
    def test_new_columns_are_stored(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(
            silver,
            bronze,
            language_confidence=0.93,
            category="notice",
            author="Pokhara Metropolitan City",
            quality_flags=["thin_content"],
        )
        page = silver.session.get(Page, page_id)
        assert page is not None
        assert (page.language_confidence, page.category, page.author) == (
            0.93,
            "notice",
            "Pokhara Metropolitan City",
        )
        assert page.quality_flags == ["thin_content"]
        assert page.version == 1

    def test_author_falls_back_to_extracted_metadata(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(
            silver, bronze, extracted_metadata={"title": "t", "author": "Ward Office 5"}
        )
        page = silver.session.get(Page, page_id)
        assert page is not None and page.author == "Ward Office 5"

    def test_language_confidence_out_of_range_is_rejected(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        with pytest.raises(ValueError, match="language_confidence"):
            save(silver, bronze, language_confidence=1.5)

    def test_version_counts_content_changes_only(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        url = f"https://{HOST}/notice/versioned"
        docs = [
            bronze.save_document(bronze_doc(n, url=url, normalized_url=url)).id
            for n in (1, 2, 3)
        ]
        silver.save_page(payload(content_hash="a" * 64), crawled_document_id=docs[0])
        silver.save_page(payload(content_hash="a" * 64), crawled_document_id=docs[1])
        result = silver.save_page(payload(content_hash="b" * 64), crawled_document_id=docs[2])

        page = silver.session.get(Page, result.id)
        assert page is not None
        silver.session.refresh(page)
        assert page.version == 2

    def test_first_seen_stays_and_last_seen_moves(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(silver, bronze)
        silver.session.execute(
            update(Page)
            .where(Page.id == page_id)
            .values(
                first_seen_at=func.now() - timedelta(days=9),
                last_seen_at=func.now() - timedelta(days=9),
            )
        )
        doc2 = bronze.save_document(
            bronze_doc(2, url=f"https://{HOST}/notice/1", normalized_url=f"https://{HOST}/notice/1")
        ).id
        silver.save_page(payload(), crawled_document_id=doc2)

        page = silver.session.get(Page, page_id)
        assert page is not None
        silver.session.refresh(page)
        assert page.last_seen_at - page.first_seen_at > timedelta(days=8)

    def test_database_rejects_version_zero(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(silver, bronze)
        with pytest.raises(IntegrityError, match="ck_pages_version_positive"):
            silver.session.execute(update(Page).where(Page.id == page_id).values(version=0))


class TestPageSources:
    def test_every_source_is_recorded_with_its_fetch_time(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        url = f"https://{HOST}/notice/history"
        old, new = (
            bronze.save_document(bronze_doc(n, url=url, normalized_url=url)).id for n in (1, 2)
        )
        silver.save_page(payload(content_hash="a" * 64), crawled_document_id=new)
        # The older row arrives late: the page is untouched, but history keeps it.
        result = silver.save_page(payload(content_hash="b" * 64), crawled_document_id=old)

        history = silver.page_history(result.id)
        assert [h.crawled_document_id for h in history] == [old, new]
        assert history[0].fetched_at == _ANCIENT + timedelta(days=1)
        assert [h.content_changed for h in history] == [True, True]

    def test_reprocessing_the_same_source_adds_no_history(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, doc_id = save(silver, bronze)
        silver.save_page(payload(), crawled_document_id=doc_id)
        assert len(silver.page_history(page_id)) == 1

    def test_unchanged_content_is_marked_unchanged(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        url = f"https://{HOST}/notice/same"
        docs = [
            bronze.save_document(bronze_doc(n, url=url, normalized_url=url)).id for n in (1, 2)
        ]
        for doc_id in docs:
            result = silver.save_page(payload(), crawled_document_id=doc_id)
        assert [h.content_changed for h in silver.page_history(result.id)] == [True, False]

    def test_stored_file_sources_use_stored_at(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        file_id = bronze.save_stored_file(
            {
                "source_page_url": f"https://{HOST}/notice/1",
                "document_url": f"https://{HOST}/files/a.pdf",
                "storage_path": "raw/a.pdf",
                "sha256": "c" * 64,
                "size": 10,
                "stored_at": "2026-09-23T06:00:05Z",
            }
        ).id
        result = silver.save_page(payload(), stored_file_id=file_id)
        (source,) = silver.page_history(result.id)
        assert (source.stored_file_id, source.fetched_at) == (
            file_id,
            datetime(2026, 9, 23, 6, 0, 5, tzinfo=UTC),
        )


class TestPageMedia:
    def test_media_is_saved_and_linked_to_its_stored_file(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        image_url = f"https://{HOST}/images/budget.png"
        file_id = bronze.save_stored_file(
            {
                "source_page_url": f"https://{HOST}/notice/1",
                "document_url": image_url,
                "storage_path": "raw/budget.png",
                "sha256": "d" * 64,
                "size": 10,
                "stored_at": "2026-09-23T06:00:05Z",
            }
        ).id
        page_id, _ = save(
            silver,
            bronze,
            media=[
                {
                    "url": image_url,
                    "media_type": "image",
                    "alt_text": "Budget table",
                    "extracted_text": "बजेट २०८०/८१",
                },
                {"url": f"https://{HOST}/v.mp4", "media_type": "VIDEO"},
            ],
        )
        media = silver.session.scalars(
            select(PageMedia).where(PageMedia.page_id == page_id).order_by(PageMedia.url)
        ).all()
        assert [(m.url, m.media_type, m.stored_file_id) for m in media] == [
            (image_url, MediaType.IMAGE, file_id),
            (f"https://{HOST}/v.mp4", MediaType.VIDEO, None),
        ]
        assert media[0].extracted_text == "बजेट २०८०/८१"

    def test_a_payload_without_media_keeps_existing_rows(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, doc_id = save(
            silver, bronze, media=[{"url": f"https://{HOST}/a.png", "media_type": "image"}]
        )
        silver.save_page(payload(), crawled_document_id=doc_id)
        silver.replace_media(page_id, [{"url": f"https://{HOST}/b.png", "media_type": "image"}])
        urls = silver.session.scalars(select(PageMedia.url).where(PageMedia.page_id == page_id))
        assert list(urls) == [f"https://{HOST}/b.png"]

    def test_invalid_media_is_rejected_before_any_write(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        doc_id = bronze.save_document(bronze_doc()).id
        with pytest.raises(ValueError, match="media_type"):
            silver.save_page(
                payload(media=[{"url": "https://x/y", "media_type": "hologram"}]),
                crawled_document_id=doc_id,
            )
        assert silver.session.scalar(select(func.count()).select_from(Page)) == 0


class TestEntities:
    def test_an_entity_is_shared_across_pages(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        mayor = {"type": "person", "name_en": "Dhanraj Acharya", "salience": 0.8}
        first, _ = save(silver, bronze, 1, entities=[mayor])
        second, _ = save(
            silver,
            bronze,
            2,
            entities=[
                {"type": "PERSON", "name_en": "  DHANRAJ   acharya ", "name_ne": "धनराज आचार्य"}
            ],
        )
        entities = silver.session.scalars(select(Entity)).all()
        assert len(entities) == 1
        assert entities[0].normalized_key == "PERSON:dhanraj acharya"
        # The first name seen is kept; the missing Nepali name is filled in.
        assert (entities[0].name_en, entities[0].name_ne) == ("Dhanraj Acharya", "धनराज आचार्य")
        linked = silver.session.scalars(select(PageEntity.page_id).order_by(PageEntity.page_id))
        assert list(linked) == [first, second]

    def test_repeated_mentions_in_one_payload_are_merged(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(
            silver,
            bronze,
            entities=[
                {"type": "organization", "name": "Ward Office 5", "mention_count": 2},
                {"type": "organization", "name": "ward office 5", "salience": 0.4},
            ],
        )
        (link,) = silver.session.scalars(
            select(PageEntity).where(PageEntity.page_id == page_id)
        ).all()
        assert (link.mention_count, link.salience) == (3, 0.4)

    @pytest.mark.parametrize(
        "bad, match",
        [
            ({"type": "planet", "name": "x"}, "type"),
            ({"type": "person"}, "name"),
            ({"type": "person", "name": "x", "salience": 2}, "salience"),
        ],
    )
    def test_invalid_entities_are_rejected(
        self, silver: SilverRepository, bronze: BronzeRepository, bad: dict[str, Any], match: str
    ) -> None:
        doc_id = bronze.save_document(bronze_doc()).id
        with pytest.raises(ValueError, match=match):
            silver.save_page(payload(entities=[bad]), crawled_document_id=doc_id)

    def test_an_explicit_key_wins(self, silver: SilverRepository, bronze: BronzeRepository) -> None:
        save(silver, bronze, entities=[{"type": "event", "name": "Budget 2080", "key": "wd:Q1"}])
        assert silver.session.scalar(select(Entity.normalized_key)) == "wd:Q1"
        assert silver.session.scalar(select(Entity.type)) == EntityType.EVENT


class TestEmbeddings:
    def _embed(self, silver: SilverRepository, page_id: int, *hots: int, model: str = MINI) -> int:
        return silver.replace_embeddings(
            page_id, model, [{"text": f"chunk {h}", "vector": vec(h)} for h in hots]
        )

    def test_embeddings_from_the_payload(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(
            silver,
            bronze,
            embeddings={
                "model_name": "all-MiniLM-L6-v2",
                "model_version": "2",
                "chunks": [
                    {"text": "first", "vector": vec(0)},
                    {"text": "second", "vector": vec(1)},
                ],
            },
        )
        rows = silver.session.scalars(
            select(PageEmbedding).where(PageEmbedding.page_id == page_id).order_by(
                PageEmbedding.chunk_index
            )
        ).all()
        assert [(r.chunk_index, r.chunk_text) for r in rows] == [(0, "first"), (1, "second")]
        assert rows[0].content_hash == "a" * 64
        assert len(rows[0].embedding) == MINI_DIM
        # The short Hugging Face name resolves to the registered one.
        assert (rows[0].model_name, rows[0].dimensions) == (MINI, MINI_DIM)

    @pytest.mark.parametrize("vector", [[0.1] * 10, [math.nan] * MINI_DIM])
    def test_bad_vectors_are_rejected(
        self, silver: SilverRepository, bronze: BronzeRepository, vector: list[float]
    ) -> None:
        page_id, _ = save(silver, bronze)
        with pytest.raises(ValueError, match="embedding"):
            silver.replace_embeddings(page_id, MINI, [{"text": "t", "vector": vector}])

    def test_nearest_chunks_are_ordered_by_cosine_distance(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        near_page, _ = save(silver, bronze, 1)
        far_page, _ = save(silver, bronze, 2)
        self._embed(silver, near_page, 5)
        self._embed(silver, far_page, 200)

        found = silver.nearest_chunks(vec(5), MINI, limit=2)
        assert [c.page_id for c, _ in found] == [near_page, far_page]
        assert found[0][1] == pytest.approx(0.0)
        assert found[1][1] == pytest.approx(1.0)

    def test_duplicates_are_not_searched(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        original, _ = save(silver, bronze, 1)
        copy, _ = save(silver, bronze, 2)
        self._embed(silver, copy, 5)
        silver.mark_duplicate_of(copy, original)
        assert silver.nearest_chunks(vec(5), MINI) == []

    def test_pages_missing_embeddings_tracks_content_changes(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(silver, bronze, 1)

        def missing() -> list[int]:
            return [p.id for p in silver.pages_missing_embeddings(MINI, limit=1000)]

        assert page_id in missing()
        self._embed(silver, page_id, 1)
        assert page_id not in missing()
        labse_missing = silver.pages_missing_embeddings("sentence-transformers/LaBSE", limit=1000)
        assert page_id in [p.id for p in labse_missing]

        # New content: the old vectors no longer describe the page.
        url = f"https://{HOST}/notice/1"
        doc2 = bronze.save_document(bronze_doc(2, url=url, normalized_url=url)).id
        silver.save_page(payload(content_hash="b" * 64), crawled_document_id=doc2)
        assert page_id in missing()

    def test_replacing_one_model_keeps_the_others(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(silver, bronze)
        self._embed(silver, page_id, 1, 2, model=MINI)
        self._embed(silver, page_id, 3, model=MULTI)
        self._embed(silver, page_id, 4, model=MINI)
        by_model = silver.session.execute(
            select(PageEmbedding.model_name, func.count())
            .where(PageEmbedding.page_id == page_id)
            .group_by(PageEmbedding.model_name)
            .order_by(PageEmbedding.model_name)
        ).all()
        assert [tuple(r) for r in by_model] == [(MINI, 1), (MULTI, 1)]

    def test_missing_page_raises(self, silver: SilverRepository) -> None:
        with pytest.raises(LookupError):
            silver.replace_embeddings(9_999_999, MINI, [])


class TestSchemas:
    def test_a_saved_page_validates_with_all_relations(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        page_id, _ = save(
            silver,
            bronze,
            media=[{"url": f"https://{HOST}/a.png", "media_type": "image"}],
            entities=[{"type": "person", "name": "A"}],
        )
        page = silver.session.get(Page, page_id)
        assert page is not None
        silver.session.refresh(page)
        read = PageWithRelations.model_validate(page)
        assert read.version == 1 and len(read.media) == 1 and len(read.entities) == 1
        assert len(read.sources) == 1

    def test_page_schema_needs_exactly_one_source(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            PageBase(
                canonical_url="https://x/",
                content_hash="a" * 64,
                body_text="t",
                word_count=1,
                language="EN",
            )


# ------------------------------------------------------------------ the ETL loop


@pytest.fixture()
def session_factory() -> Iterator[sessionmaker[Session]]:
    """Sessions that commit into savepoints of one outer transaction, rolled back after.

    `pgs_db.etl` opens and commits its own sessions, as it would in Spark; this
    lets it do that without leaving rows behind.
    """
    engine = make_engine()
    conn = engine.connect()
    outer = conn.begin()
    try:
        yield sessionmaker(
            bind=conn, join_transaction_mode="create_savepoint", expire_on_commit=False
        )
    finally:
        outer.rollback()
        conn.close()


def _statuses(factory: sessionmaker[Session], ids: list[int]) -> list[Any]:
    with factory() as s:
        return list(
            s.scalars(
                select(CrawledDocument.processing_status)
                .where(CrawledDocument.id.in_(ids))
                .order_by(CrawledDocument.id)
            )
        )


class TestEtlLoop:
    def _docs(self, factory: sessionmaker[Session], *ns: int, **kw: Any) -> list[int]:
        with factory() as s, s.begin():
            repo = BronzeRepository(s)
            return [
                repo.save_document(bronze_doc(n, **kw), register_unknown_domains=True).id
                for n in ns
            ]

    def test_pages_are_saved_marked_and_domain_tagged(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        ids = self._docs(session_factory, 1, 2)
        with session_factory() as s, s.begin():
            ReferenceRepository(s).link_domains_to_local_bodies()

        result = process_bronze_batch(
            session_factory,
            lambda doc: payload(content_hash=f"{doc.id:064x}"),
            limit=1000,
        )
        assert (result.saved, result.failed) == (result.claimed, 0)
        assert _statuses(session_factory, ids) == [ProcessingStatus.PROCESSED] * 2
        with session_factory() as s:
            codes = s.scalars(
                select(Page.id).where(Page.crawled_document_id.in_(ids))
            ).all()
            page = s.get(Page, codes[0])
            assert page is not None
            assert [t.local_body_code for t in page.geo_tags] == ["MUN414"]

    def test_a_failing_transform_parks_only_its_row(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        good, bad = self._docs(session_factory, 1, 2)

        def transform(doc: CrawledDocument) -> dict[str, Any]:
            if doc.id == bad:
                raise RuntimeError("unreadable PDF")
            return payload(content_hash=f"{doc.id:064x}")

        result = process_bronze_batch(session_factory, transform, limit=1000)
        assert result.errors[bad] == "RuntimeError: unreadable PDF"
        assert _statuses(session_factory, [good, bad]) == [
            ProcessingStatus.PROCESSED,
            ProcessingStatus.FAILED,
        ]

    def test_same_content_on_two_urls_is_folded(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        first, second = self._docs(session_factory, 1, 2)
        result = process_bronze_batch(
            session_factory, lambda doc: payload(content_hash="e" * 64), limit=1000
        )
        assert result.duplicates == 1
        with session_factory() as s:
            pages = {
                p.crawled_document_id: p
                for p in s.scalars(
                    select(Page).where(Page.crawled_document_id.in_([first, second]))
                )
            }
            assert pages[first].duplicate_of_id is None
            assert pages[second].duplicate_of_id == pages[first].id

    def test_stored_files_become_pages(self, session_factory: sessionmaker[Session]) -> None:
        (doc_id,) = self._docs(session_factory, 1)
        with session_factory() as s, s.begin():
            file_id = BronzeRepository(s).save_stored_file(
                {
                    "source_page_url": f"https://{HOST}/notice/1",
                    "document_url": f"https://{HOST}/files/budget.pdf",
                    "storage_path": "raw/budget.pdf",
                    "sha256": "f" * 64,
                    "size": 10,
                    "stored_at": _ANCIENT.isoformat(),
                },
                crawled_document_id=doc_id,
            ).id

        seen: list[str] = []

        def transform(stored: StoredFile) -> dict[str, Any]:
            seen.append(stored.storage_path)
            return {"searchable_text": "budget PDF text", "language_detected": "en"}

        result = process_stored_file_batch(session_factory, transform, limit=1000)
        assert result.failed == 0 and "raw/budget.pdf" in seen
        with session_factory() as s:
            page = s.scalar(select(Page).where(Page.stored_file_id == file_id))
            assert page is not None
            assert page.content_hash == "f" * 64  # the file's sha256
            assert page.canonical_url == f"https://{HOST}/files/budget.pdf"
            assert page.domain_id is not None
            status = s.scalar(select(StoredFile.processing_status).where(StoredFile.id == file_id))
            assert status == ProcessingStatus.PROCESSED
