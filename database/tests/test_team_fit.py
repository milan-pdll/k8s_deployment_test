"""What the other groups' current branches send and expect (migration f6a7b8c9d0e1).

- ETL `etl/workflow_saurav`: `transform.py` records (top-level title, hex simhash,
  place-name geo, 768-d LaBSE embedding) land in Silver unchanged.
- Search: LaBSE is the default model; the proto's request values work as sent.
- Scraper (Temporal lineage): lower-case run statuses, run config, `links` only.
- Image indexer: `surrounding_context` is kept.
- API: the document download lookup.
"""

import math
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from pgs_db import (
    BronzeRepository,
    ReferenceRepository,
    SearchRepository,
    SilverRepository,
    StatsRepository,
)
from pgs_db.enums import CrawlRunStatus
from pgs_db.etl import payload_from_transform, save_transformed
from pgs_db.models import (
    DEFAULT_EMBEDDING_MODEL,
    CrawlRun,
    District,
    EmbeddingModel,
    LocalBody,
    Page,
    PageEmbedding,
    PageGeoTag,
    PageMedia,
    QuarantinedFile,
)
from pgs_db.repositories.silver import hnsw_index_name

HOST = "teamfit.gov.np"
LABSE_DIM = 768
_ANCIENT = datetime(1975, 1, 1, tzinfo=UTC)


def unit(i: int, dims: int = LABSE_DIM) -> list[float]:
    v = [0.0] * dims
    v[i] = 1.0
    return v


def code_for(session: Session, model: type[District] | type[LocalBody], name: str) -> str:
    code = session.scalar(select(model.code).where(model.name_en == name))
    assert code is not None, name
    return code


def crawl(bronze: BronzeRepository, n: int, **overrides: Any) -> int:
    url = f"https://{HOST}/{n}"
    doc: dict[str, Any] = {
        "url": url,
        "normalized_url": url,
        "host": HOST,
        "content_hash": f"{n:064x}",
        "fetched_at": (_ANCIENT + timedelta(days=n)).isoformat(),
    }
    doc.update(overrides)
    return bronze.save_document(doc, register_unknown_domains=True).id


def etl_record(**overrides: Any) -> dict[str, Any]:
    """A record exactly as ETL/spark/transform.py's transform_document emits it."""
    record: dict[str, Any] = {
        "document_id": "doc_abc123abc123",
        "source_url": f"https://{HOST}/notice",
        "object_key": "raw/teamfit/notice.html",
        "target_domain": HOST,
        "title": "Pokhara budget notice",
        "language_detected": "mixed",
        "searchable_text": "Pokhara metropolitan budget notice for fiscal year",
        "word_count": 7,
        "char_count": 50,
        "content_sha256": "c" * 64,
        "simhash": "f0f0f0f0f0f0f0f0",  # > int64 max: must be stored as its signed twin
        "geo_location": {
            "province": "Gandaki Province",
            "district": "Kaski",
            "municipality": "Pokhara Metropolitan City",
            "source": "seed_gazetteer",
        },
        "duplicate": False,
        "duplicate_type": None,
        "duplicate_of": None,
        "embedding": unit(3),
        "embedding_model": "sentence-transformers/LaBSE",
        "embedding_dim": LABSE_DIM,
    }
    record.update(overrides)
    return record


# --------------------------------------------------------------- embeddings


class TestEmbeddingModels:
    def test_labse_is_the_default_and_the_minilm_models_are_registered(
        self, session: Session
    ) -> None:
        silver = SilverRepository(session)
        default = silver.embedding_model()
        assert (default.name, default.dimensions) == (DEFAULT_EMBEDDING_MODEL, LABSE_DIM)
        assert silver.embedding_model("all-MiniLM-L6-v2").dimensions == 384
        with pytest.raises(ValueError, match="unknown embedding model"):
            silver.embedding_model("no-such-model")

    def test_vectors_must_match_their_models_size(
        self, session: Session
    ) -> None:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        page_id = silver.save_page(
            {"searchable_text": "x", "content_hash": "a" * 64}, crawled_document_id=crawl(bronze, 1)
        ).id
        assert silver.replace_embeddings(page_id, "sentence-transformers/LaBSE",
                                         [{"text": "t", "vector": unit(1)}]) == 1
        with pytest.raises(ValueError, match="exactly 768"):
            silver.replace_embeddings(page_id, "sentence-transformers/LaBSE",
                                      [{"text": "t", "vector": unit(1, 384)}])
        with pytest.raises(ValueError, match="unknown embedding model"):
            silver.replace_embeddings(page_id, "gpt-embed", [{"text": "t", "vector": unit(1)}])

    def test_the_database_itself_refuses_a_mismatched_vector(self, session: Session) -> None:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        page_id = silver.save_page(
            {"searchable_text": "x", "content_hash": "b" * 64}, crawled_document_id=crawl(bronze, 2)
        ).id
        session.flush()
        insert = text(
            "INSERT INTO page_embeddings (page_id, model_name, chunk_index, "
            "chunk_text, embedding, content_hash, embedded_at) VALUES "
            "(:p, :m, 0, 't', :v, :h, now())"
        )
        # A Go or Spark writer bypassing pgs_db: 384 numbers under LaBSE's name. LaBSE's
        # HNSW index casts to vector(768) and rejects it; the foreign key backs that up.
        with (
            pytest.raises((DataError, IntegrityError), match=r"768|fk_page_embeddings"),
            session.begin_nested(),
        ):
            session.execute(
                insert,
                {"p": page_id, "m": DEFAULT_EMBEDDING_MODEL, "v": str(unit(0, 384)), "h": "b" * 64},
            )
        # A model nobody registered is refused by the foreign key.
        with (
            pytest.raises(IntegrityError, match="fk_page_embeddings_model_name_embedding_models"),
            session.begin_nested(),
        ):
            session.execute(
                insert, {"p": page_id, "m": "rogue-model", "v": str(unit(0, 8)), "h": "b" * 64}
            )

    def test_nearest_chunks_uses_the_models_hnsw_index(self, session: Session) -> None:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        page_id = silver.save_page(
            {"searchable_text": "x", "content_hash": "d" * 64}, crawled_document_id=crawl(bronze, 3)
        ).id
        silver.replace_embeddings(page_id, DEFAULT_EMBEDDING_MODEL,
                                  [{"text": "t", "vector": unit(7)}])
        session.flush()
        session.execute(text("SET LOCAL enable_seqscan = off"))
        plan = "\n".join(
            session.scalars(
                text(
                    "EXPLAIN SELECT id FROM page_embeddings "
                    "WHERE model_name = 'sentence-transformers/LaBSE' "
                    "ORDER BY embedding::vector(768) <=> :q LIMIT 5"
                ),
                {"q": str(unit(7))},
            )
        )
        assert hnsw_index_name(DEFAULT_EMBEDDING_MODEL) in plan
        found = silver.nearest_chunks(unit(7), DEFAULT_EMBEDDING_MODEL, limit=1)
        assert found[0][0].page_id == page_id and found[0][1] == pytest.approx(0.0)

    def test_registering_a_model_and_moving_the_default(self, session: Session) -> None:
        silver = SilverRepository(session)
        model = silver.register_embedding_model("test/tiny-8", 8, description="test")
        assert silver.register_embedding_model("test/tiny-8", 8).id == model.id  # re-run
        with pytest.raises(ValueError, match="registered with 8"):
            silver.register_embedding_model("test/tiny-8", 16)
        index = session.scalar(
            text("SELECT indexname FROM pg_indexes WHERE indexname = :n"),
            {"n": hnsw_index_name("test/tiny-8")},
        )
        assert index is not None
        silver.register_embedding_model("test/tiny-8", 8, make_default=True)
        assert silver.embedding_model().name == "test/tiny-8"
        defaults = session.scalars(
            select(EmbeddingModel.name).where(EmbeddingModel.is_default.is_(True))
        ).all()
        assert defaults == ["test/tiny-8"]


# ------------------------------------------------------------------- ETL fit


class TestEtlTransformOutput:
    def test_payload_conversion(self) -> None:
        payload = payload_from_transform(etl_record(), geo_confidence=0.7)
        assert payload["content_hash"] == "c" * 64
        assert payload["geo_location"]["method"] == "GAZETTEER"
        assert payload["geo_location"]["confidence"] == 0.7
        assert payload["embeddings"]["model_name"] == "sentence-transformers/LaBSE"
        assert "duplicate_of" not in payload
        with pytest.raises(ValueError):
            payload_from_transform(etl_record(), geo_confidence=1.5)

    def test_a_record_lands_in_silver_by_its_object_key(self, session: Session) -> None:
        bronze = BronzeRepository(session)
        doc_id = bronze.save_document(
            {
                "url": f"https://{HOST}/notice",
                "normalized_url": f"https://{HOST}/notice",
                "host": HOST,
                "content_hash": "e" * 64,
                "fetched_at": _ANCIENT.isoformat(),
            },
            minio_path="raw/teamfit/notice.html",
            register_unknown_domains=True,
        ).id
        session.flush()
        factory = sessionmaker(bind=session.connection(), join_transaction_mode="create_savepoint")
        saved = save_transformed(factory, etl_record(), geo_confidence=0.7)

        assert saved.crawled_document_id == doc_id
        page = session.get(Page, saved.page_id)
        assert page is not None
        session.refresh(page)
        assert page.title == "Pokhara budget notice"  # top-level title kept
        assert page.sim_hash == int("f0f0f0f0f0f0f0f0", 16) - 2**64
        tag = session.scalar(select(PageGeoTag).where(PageGeoTag.page_id == page.id))
        assert tag is not None
        assert (tag.local_body_code, tag.district_code, tag.confidence) == ("MUN414", "D38", 0.7)
        vectors = session.scalars(
            select(PageEmbedding).where(PageEmbedding.page_id == page.id)
        ).all()
        assert [(v.model_name, v.dimensions) for v in vectors] == [
            ("sentence-transformers/LaBSE", LABSE_DIM)
        ]
        status = session.scalar(
            text("SELECT processing_status FROM crawled_documents WHERE id = :i"), {"i": doc_id}
        )
        assert status == "PROCESSED"

    def test_a_pdf_keeps_its_own_url_not_the_linking_pages(self, session: Session) -> None:
        bronze = BronzeRepository(session)
        page_url = f"https://{HOST}/notices"
        crawl(bronze, 40, url=page_url, normalized_url=page_url)
        file_id = bronze.save_stored_file(
            {
                "source_page_url": page_url,
                "document_url": f"https://{HOST}/budget.pdf",
                "storage_path": "raw/teamfit/budget.pdf",
                "content_type": "application/pdf",
                "sha256": "7" * 64,
                "size": 10,
                "stored_at": _ANCIENT.isoformat(),
            }
        ).id
        session.flush()
        factory = sessionmaker(bind=session.connection(), join_transaction_mode="create_savepoint")
        saved = save_transformed(
            factory,
            etl_record(object_key="raw/teamfit/budget.pdf", source_url=page_url, embedding=None),
            geo_confidence=0.5,
        )
        assert saved.stored_file_id == file_id
        page = session.get(Page, saved.page_id)
        assert page is not None and page.canonical_url == f"https://{HOST}/budget.pdf"

    def test_no_bronze_row_is_an_error(self, session: Session) -> None:
        factory = sessionmaker(bind=session.connection(), join_transaction_mode="create_savepoint")
        with pytest.raises(LookupError, match="no Bronze row"):
            save_transformed(
                factory,
                etl_record(object_key="raw/missing", source_url="https://missing.np/x"),
                geo_confidence=0.5,
            )


class TestPlaceNames:
    def test_the_etl_sample_rules_resolve(self, session: Session) -> None:
        ref = ReferenceRepository(session)
        assert ref.codes_for_names("Gandaki Province", "Kaski", "Pokhara Metropolitan City") == {
            "province_code": "P4",
            "district_code": "D38",
            "municipality_id": "MUN414",
        }
        kathmandu = ref.codes_for_names(
            "Bagmati Province", "Kathmandu", "Kathmandu Metropolitan City"
        )
        assert kathmandu is not None
        assert kathmandu["municipality_id"] == code_for(session, LocalBody, "Kathmandu")
        # "Janakpur" is a unique prefix of "Janakpurdham" inside Dhanusha.
        janakpur = ref.codes_for_names(
            "Madhesh Province", "Dhanusha", "Janakpur Sub-Metropolitan City"
        )
        assert janakpur is not None
        assert janakpur["municipality_id"] == code_for(session, LocalBody, "Janakpurdham")

    def test_nepali_names_codes_and_partial_answers(self, session: Session) -> None:
        ref = ReferenceRepository(session)
        pokhara = session.scalar(select(LocalBody).where(LocalBody.code == "MUN414"))
        assert pokhara is not None
        assert ref.codes_for_names(municipality=pokhara.name_ne)["municipality_id"] == "MUN414"
        assert ref.codes_for_names("P4", "D38")["district_code"] == "D38"
        # An unknown municipality still yields the district it was said to be in.
        assert ref.codes_for_names(district="Kaski", municipality="Atlantis") == {
            "province_code": "P4",
            "district_code": "D38",
            "municipality_id": None,
        }
        assert ref.codes_for_names("Nowhere") is None

    def test_a_ward_outside_the_local_body_is_rejected(self, session: Session) -> None:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        with pytest.raises(ValueError, match="ward 40 does not exist in MUN414"):
            silver.save_page(
                {
                    "searchable_text": "x",
                    "content_hash": "f" * 64,
                    "geo_location": {"municipality_id": "MUN414", "ward_number": 40,
                                     "method": "GAZETTEER", "confidence": 0.9},
                },
                crawled_document_id=crawl(bronze, 5),
            )


# ---------------------------------------------------------------- scraper fit


class TestTemporalScraper:
    def test_lower_case_statuses_and_run_config(self, session: Session) -> None:
        run_id = session.scalar(
            text(
                "INSERT INTO crawl_runs (status, started_at, seed_count, max_depth, max_pages, "
                "fetched_count, succeeded_count, failed_count, skipped_count, unique_url_count) "
                "VALUES ('running', now(), 12, 3, 500, 0, 0, 0, 0, 0) RETURNING id"
            )
        )
        session.execute(
            text("UPDATE crawl_runs SET status = 'failed', error = 'worker lost' WHERE id = :i"),
            {"i": run_id},
        )
        run = session.get(CrawlRun, run_id)
        assert run is not None
        session.refresh(run)
        assert (run.status, run.seed_count, run.error) == (CrawlRunStatus.FAILED, 12, "worker lost")

    def test_repository_records_config_cap_and_error(self, session: Session) -> None:
        bronze = BronzeRepository(session)
        run_id = bronze.start_crawl_run(seed_count=4, max_depth=2, max_pages=100)
        bronze.finish_crawl_run(
            run_id,
            status=CrawlRunStatus.FAILED,
            stats={"fetched": 10, "domain_capped": 3},
            error="temporal timeout",
        )
        run = session.get(CrawlRun, run_id)
        assert run is not None
        assert (run.max_pages, run.domain_capped_count, run.error) == (100, 3, "temporal timeout")

    def test_discovered_links_fall_back_to_same_host_links(self, session: Session) -> None:
        bronze = BronzeRepository(session)
        crawl(bronze, 60, links=[
            f"https://{HOST}/a", f"https://www.{HOST}/b", "https://elsewhere.np/c",
        ])
        domain_id = bronze.domain_id_for_host(HOST)
        assert domain_id is not None
        StatsRepository(session).refresh_domain_stats([domain_id])
        rows, _ = StatsRepository(session).list_domains(search=HOST)
        assert rows[0]["discovered_links"] == 2  # a and b; elsewhere.np is external


# --------------------------------------------------------------- search & API


class TestSearchFit:
    def _page_with_vector(self, session: Session, n: int, **payload: Any) -> int:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        page_id = silver.save_page(
            {"searchable_text": "x", "content_hash": f"{n:064x}", **payload},
            crawled_document_id=crawl(bronze, n),
        ).id
        chunks = [{"text": "t", "vector": unit(9)}]
        silver.replace_embeddings(page_id, DEFAULT_EMBEDDING_MODEL, chunks)
        return page_id

    def test_proto_request_values_work_as_sent(self, session: Session) -> None:
        search = SearchRepository(session)
        in_pokhara = self._page_with_vector(session, 70, language_detected="ne", geo_location={
            "municipality_id": "MUN414", "method": "GAZETTEER", "confidence": 0.9})
        elsewhere = self._page_with_vector(session, 71)

        # SearchRequest defaults: "" codes, ward 0, content_type "all", language "auto".
        everything = search.vector_search(
            unit(9), province_code="", district_code="", municipality_id="", ward_number=0,
            content_type="all", language="auto", limit=500,
        )
        assert {in_pokhara, elsewhere} <= {h["page_id"] for h in everything}

        pokhara = search.vector_search(unit(9), municipality_id="MUN414", language="ne", limit=500)
        assert {h["page_id"] for h in pokhara} >= {in_pokhara}
        assert elsewhere not in {h["page_id"] for h in pokhara}
        with pytest.raises(ValueError, match="768"):
            search.vector_search(unit(9, 384))

    def test_file_location_for_downloads(self, session: Session) -> None:
        bronze, silver, search = (
            BronzeRepository(session), SilverRepository(session), SearchRepository(session)
        )
        file_id = bronze.save_stored_file(
            {
                "source_page_url": f"https://{HOST}/x",
                "document_url": f"https://{HOST}/files/Budget.pdf?v=1",
                "storage_path": "raw/teamfit/Budget.pdf",
                "content_type": "application/pdf",
                "sha256": "8" * 64,
                "size": 2048,
                "stored_at": _ANCIENT.isoformat(),
            }
        ).id
        pdf_page = silver.save_page(
            {"searchable_text": "b", "content_hash": "8" * 64}, stored_file_id=file_id
        ).id
        web_page = silver.save_page(
            {"searchable_text": "w", "content_hash": "9" * 64},
            crawled_document_id=crawl(bronze, 80),
        ).id
        location = search.file_location(pdf_page)
        assert location is not None
        assert (location["storage_path"], location["filename"], location["size_bytes"]) == (
            "raw/teamfit/Budget.pdf", "Budget.pdf", 2048,
        )
        assert search.file_location(web_page) is None

        # A file with the same bytes as a quarantined one is never served.
        session.add(QuarantinedFile(
            document_url="https://evil.np/x.pdf", quarantine_path="s3://q/x", sha256="8" * 64,
            threat_signature="Eicar", scanned_at=_ANCIENT,
        ))
        session.flush()
        assert search.file_location(pdf_page) is None


class TestImageContext:
    def test_surrounding_context_is_kept(self, session: Session) -> None:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        page_id = silver.save_page(
            {
                "searchable_text": "x",
                "content_hash": "1" * 64,
                "media": [{
                    "url": f"https://{HOST}/img.jpg",
                    "media_type": "image",
                    "alt_text": "Ward office",
                    "extracted_text": "OCR text",
                    "surrounding_context": "Ward 8 office opening, Pokhara",
                }],
            },
            crawled_document_id=crawl(bronze, 90),
        ).id
        media = session.scalar(select(PageMedia).where(PageMedia.page_id == page_id))
        assert media is not None and media.context_text == "Ward 8 office opening, Pokhara"


def test_nan_guard_still_applies(session: Session) -> None:
    bronze, silver = BronzeRepository(session), SilverRepository(session)
    page_id = silver.save_page(
        {"searchable_text": "x", "content_hash": "2" * 64}, crawled_document_id=crawl(bronze, 95)
    ).id
    with pytest.raises(ValueError, match="finite"):
        silver.replace_embeddings(
            page_id, DEFAULT_EMBEDDING_MODEL, [{"text": "t", "vector": [math.nan] * LABSE_DIM}]
        )
