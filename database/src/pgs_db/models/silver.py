"""Silver layer: one clean record per page, deduplicated and geo-tagged.

Written by the Spark ETL, read by the search indexer and the API. Every column
traces to the ETL team's own output contract (`ETL/spark/README.md` §5.2) or to
the search team's consuming model (`pgs_search.models.SearchDocument` /
`GeoLocation`). See `docs/bronze-silver-contract.md` for the field-by-field
provenance.

A page is keyed by its canonical URL, not by the Bronze row it came from: a
recrawl produces a new `crawled_documents` row, and Silver repoints the existing
page at it rather than creating a second page.

The denormalized geography names from §5.2 (`province_name_en`,
`municipality_type`, ...) are deliberately **not** stored here -- they are
derivable by joining `page_geo_tags` to the seeded gazetteer, and duplicating
837 rows of names onto every page would guarantee they drift.
"""

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Computed,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ..base import Base, IdMixin, TimestampMixin
from ..enums import (
    ContactType,
    EntityType,
    GeoTagMethod,
    Language,
    MediaType,
    ProcessingStatus,
    QuarantineStatus,
)
from ._types import str_enum
from .crawl import CrawledDocument, StoredFile
from .domain import Domain
from .geography import District, LocalBody, Province

TextArray = ARRAY(Text)

# The model the ETL writes and search queries with (embedding_models.is_default).
# LaBSE: multilingual, so Nepali and English land in one space. 768 dimensions.
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/LaBSE"


class Page(IdMixin, TimestampMixin, Base):
    """One clean, deduplicated page. Silver output of `crawled_documents`."""

    __tablename__ = "pages"
    __table_args__ = (
        # A page comes from exactly one source: a crawled page or a stored file.
        CheckConstraint(
            "num_nonnulls(crawled_document_id, stored_file_id) = 1", name="one_source"
        ),
        CheckConstraint(
            "language_confidence IS NULL"
            " OR (language_confidence >= 0 AND language_confidence <= 1)",
            name="language_confidence_range",
        ),
        CheckConstraint("version >= 1", name="version_positive"),
        Index("ix_pages_processing_status", "processing_status"),
        Index("ix_pages_category", "category"),
        Index("ix_pages_content_hash", "content_hash"),
        Index("ix_pages_published_at", "published_at"),
    )

    # --- Bronze linkage -----------------------------------------------------
    # The latest Bronze row this page was built from: a crawled page, or a file
    # (PDF, image, ...) the scraper stored in MinIO. Exactly one is set. Not
    # unique: it moves forward on every recrawl. RESTRICT so Bronze retention
    # can't silently delete a live page -- repoint or delete the page first.
    crawled_document_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("crawled_documents.id", ondelete="RESTRICT"), index=True
    )
    stored_file_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("stored_files.id", ondelete="RESTRICT"), index=True
    )
    domain_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("domains.id"), index=True
    )

    # --- identity (§5.2 source_url) -----------------------------------------
    # UNIQUE: the idempotency key for the whole layer. Reprocessing any Bronze
    # row for the same canonical URL updates this page.
    canonical_url: Mapped[str] = mapped_column(Text, unique=True)

    # --- extracted content (§5.2 extracted_metadata, SearchDocument) --------
    title: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)
    body_text: Mapped[str] = mapped_column(Text)
    word_count: Mapped[int] = mapped_column(Integer)
    keywords: Mapped[list[str] | None] = mapped_column(TextArray)
    language: Mapped[Language] = mapped_column(str_enum(Language, "language"))
    language_confidence: Mapped[float | None] = mapped_column(Float)  # 0..1, from the detector
    content_type: Mapped[str] = mapped_column(String(32), default="web_page")
    # Content class the ETL assigns (notice, news, tender, ...); free text for now.
    category: Mapped[str | None] = mapped_column(String(64))
    author: Mapped[str | None] = mapped_column(String(255))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # ETL quality warnings, e.g. {"thin_content", "boilerplate", "ocr_low_confidence"}.
    quality_flags: Mapped[list[str] | None] = mapped_column(TextArray)

    # --- history ------------------------------------------------------------
    # first_seen_at: when Silver first saved this URL. last_seen_at: the latest
    # save. version: starts at 1, +1 each time the content_hash changes.
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")

    # --- deduplication (ETL §3: SHA256 exact + SimHash fuzzy) ---------------
    content_hash: Mapped[str] = mapped_column(String(64))
    sim_hash: Mapped[int | None] = mapped_column(BigInteger)
    # Set when this page was folded into a canonical record; NULL means canonical.
    duplicate_of_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="SET NULL"), index=True
    )

    # --- downstream processing state ----------------------------------------
    # UNPROCESSED until the search indexer has taken this page, mirroring the
    # same column on crawled_documents.
    processing_status: Mapped[ProcessingStatus] = mapped_column(
        str_enum(ProcessingStatus, "processing_status"),
        default=ProcessingStatus.UNPROCESSED,
        server_default=ProcessingStatus.UNPROCESSED.value,
    )
    processing_error: Mapped[str | None] = mapped_column(Text)

    crawled_document: Mapped[CrawledDocument | None] = relationship()
    stored_file: Mapped[StoredFile | None] = relationship()
    domain: Mapped[Domain | None] = relationship()
    duplicate_of: Mapped["Page | None"] = relationship(remote_side="Page.id")
    geo_tags: Mapped[list["PageGeoTag"]] = relationship(
        back_populates="page", cascade="all, delete-orphan"
    )
    contacts: Mapped[list["PageContact"]] = relationship(
        back_populates="page", cascade="all, delete-orphan"
    )
    sources: Mapped[list["PageSource"]] = relationship(
        back_populates="page", cascade="all, delete-orphan"
    )
    media: Mapped[list["PageMedia"]] = relationship(
        back_populates="page", cascade="all, delete-orphan"
    )
    entities: Mapped[list["PageEntity"]] = relationship(
        back_populates="page", cascade="all, delete-orphan"
    )
    embeddings: Mapped[list["PageEmbedding"]] = relationship(
        back_populates="page", cascade="all, delete-orphan"
    )


class PageGeoTag(IdMixin, TimestampMixin, Base):
    """Where a page is about, resolved against the seeded gazetteer.

    Codes are stored, never names: `province_name_en`, `municipality_type` and the
    rest of §5.2's `geo_location` block are produced by joining to `provinces`,
    `districts` and `local_bodies`.

    All three levels are nullable because resolution is partial in practice -- a
    municipality site resolves to a local body, while a national news article may
    only resolve to a district or province. The CHECK guarantees at least one.
    """

    __tablename__ = "page_geo_tags"
    __table_args__ = (
        CheckConstraint(
            "province_code IS NOT NULL OR district_code IS NOT NULL "
            "OR local_body_code IS NOT NULL",
            name="at_least_one_level",
        ),
        CheckConstraint("ward_number IS NULL OR ward_number > 0", name="ward_number_positive"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="confidence_range"),
        # NULLS NOT DISTINCT (PostgreSQL 15+) so re-tagging a page with the same
        # partially-NULL location is idempotent rather than inserting a duplicate.
        UniqueConstraint(
            "page_id",
            "province_code",
            "district_code",
            "local_body_code",
            "ward_number",
            name="uq_page_geo_tags_page_location",
            postgresql_nulls_not_distinct=True,
        ),
        Index("ix_page_geo_tags_local_body_code", "local_body_code"),
        Index("ix_page_geo_tags_district_code", "district_code"),
        Index("ix_page_geo_tags_province_code", "province_code"),
    )

    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), index=True
    )
    province_code: Mapped[str | None] = mapped_column(
        String(4), ForeignKey("provinces.code")
    )  # §5.2 geo_location.province_code
    district_code: Mapped[str | None] = mapped_column(
        String(4), ForeignKey("districts.code")
    )  # §5.2 geo_location.district_code
    local_body_code: Mapped[str | None] = mapped_column(
        String(16), ForeignKey("local_bodies.code")
    )  # §5.2 geo_location.municipality_id
    ward_number: Mapped[int | None] = mapped_column(Integer)  # §5.2 geo_location.ward_number

    # --- provenance of the tag ----------------------------------------------
    method: Mapped[GeoTagMethod] = mapped_column(str_enum(GeoTagMethod, "geo_tag_method"))
    confidence: Mapped[float] = mapped_column(Float)
    # The span that produced the tag, e.g. "पोखरा" or "Pokhara Metropolitan City".
    mention_text: Mapped[str | None] = mapped_column(Text)

    page: Mapped[Page] = relationship(back_populates="geo_tags")
    province: Mapped[Province | None] = relationship()
    district: Mapped[District | None] = relationship()
    local_body: Mapped[LocalBody | None] = relationship()


class PageContact(IdMixin, TimestampMixin, Base):
    """One contact detail found on a page (§5.2 extracted_metadata.contact_info).

    Bronze stores emails, phones and social links as three parallel arrays on
    `crawled_documents`; Silver normalizes them into rows so the API can query
    "every phone number for pages in Kaski" without unnesting arrays.
    """

    __tablename__ = "page_contacts"
    __table_args__ = (
        UniqueConstraint("page_id", "type", "value", name="uq_page_contacts_page_type_value"),
        Index("ix_page_contacts_type_value", "type", "value"),
    )

    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), index=True
    )
    type: Mapped[ContactType] = mapped_column(str_enum(ContactType, "contact_type"))
    value: Mapped[str] = mapped_column(Text)

    page: Mapped[Page] = relationship(back_populates="contacts")


class PageSource(IdMixin, TimestampMixin, Base):
    """Every Bronze row that fed a page: its fetch history.

    `pages.crawled_document_id` / `stored_file_id` only point at the newest source;
    this keeps all of them, so "when did this notice change?" is answerable after
    the page has been repointed. Written by `save_page`, one row per source row.
    """

    __tablename__ = "page_sources"
    __table_args__ = (
        CheckConstraint(
            "num_nonnulls(crawled_document_id, stored_file_id) = 1", name="one_source"
        ),
        UniqueConstraint(
            "page_id",
            "crawled_document_id",
            "stored_file_id",
            name="uq_page_sources_page_source",
            postgresql_nulls_not_distinct=True,
        ),
    )

    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), index=True
    )
    crawled_document_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("crawled_documents.id", ondelete="CASCADE"), index=True
    )
    stored_file_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("stored_files.id", ondelete="CASCADE"), index=True
    )
    # crawled_documents.fetched_at, or stored_files.stored_at.
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    # True when this source's content differed from what the page held before it.
    content_changed: Mapped[bool] = mapped_column(Boolean)

    page: Mapped[Page] = relationship(back_populates="sources")


class PageMedia(IdMixin, TimestampMixin, Base):
    """An image, video or linked document on a page, with any text pulled out of it.

    `extracted_text` holds OCR (images) or parsed text (PDFs), so a scanned notice
    is searchable. `stored_file_id` links the MinIO copy when the scraper kept one.
    """

    __tablename__ = "page_media"
    __table_args__ = (UniqueConstraint("page_id", "url", name="uq_page_media_page_url"),)

    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), index=True
    )
    stored_file_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("stored_files.id", ondelete="SET NULL"), index=True
    )
    url: Mapped[str] = mapped_column(Text)
    media_type: Mapped[MediaType] = mapped_column(str_enum(MediaType, "media_type"))
    alt_text: Mapped[str | None] = mapped_column(Text)
    extracted_text: Mapped[str | None] = mapped_column(Text)
    # Text around the media on its page (caption, nearby paragraph): search context.
    context_text: Mapped[str | None] = mapped_column(Text)

    page: Mapped[Page] = relationship(back_populates="media")
    stored_file: Mapped[StoredFile | None] = relationship()


class Entity(IdMixin, TimestampMixin, Base):
    """A person, organization or event named across pages (NER output).

    One row per real-world entity, shared by every page that mentions it.
    `normalized_key` is the dedup key (`TYPE:casefolded name` unless the ETL
    supplies its own), so "Mayor Dhanraj Acharya" found on 40 pages is one row.
    Places are not entities here: they are `page_geo_tags` against the gazetteer.
    """

    __tablename__ = "entities"

    normalized_key: Mapped[str] = mapped_column(String(255), unique=True)
    type: Mapped[EntityType] = mapped_column(str_enum(EntityType, "entity_type"))
    name_en: Mapped[str | None] = mapped_column(String(255))
    name_ne: Mapped[str | None] = mapped_column(String(255))


class PageEntity(IdMixin, TimestampMixin, Base):
    """How strongly one page mentions one entity."""

    __tablename__ = "page_entities"
    __table_args__ = (
        UniqueConstraint("page_id", "entity_id", name="uq_page_entities_page_entity"),
        CheckConstraint("mention_count >= 1", name="mention_count_positive"),
        CheckConstraint(
            "salience IS NULL OR (salience >= 0 AND salience <= 1)", name="salience_range"
        ),
    )

    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), index=True
    )
    entity_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("entities.id", ondelete="CASCADE"), index=True
    )
    mention_count: Mapped[int] = mapped_column(Integer, default=1)
    salience: Mapped[float | None] = mapped_column(Float)

    page: Mapped[Page] = relationship(back_populates="entities")
    entity: Mapped[Entity] = relationship()


class EmbeddingModel(IdMixin, TimestampMixin, Base):
    """A sentence-embedding model the pipeline may store vectors for.

    Documents and queries must be embedded by the same model to be comparable, so
    every `page_embeddings` row names one of these, and its vector must have exactly
    this model's `dimensions` (a composite foreign key enforces both). Each model has
    its own HNSW index; `SilverRepository.register_embedding_model` adds one.

    `is_default` marks the model the ETL writes and search queries with. Seeded:
    LaBSE (768, default; multilingual, good for Nepali) and two 384-dimension MiniLM
    models the search branches used first.
    """

    __tablename__ = "embedding_models"
    __table_args__ = (
        # HNSW indexes vectors of up to 2000 dimensions.
        CheckConstraint("dimensions BETWEEN 1 AND 2000", name="dimensions_range"),
        UniqueConstraint("name", "dimensions", name="uq_embedding_models_name_dimensions"),
        Index(
            "uq_embedding_models_one_default",
            "is_default",
            unique=True,
            postgresql_where=text("is_default"),
        ),
    )

    name: Mapped[str] = mapped_column(String(128), unique=True)
    dimensions: Mapped[int] = mapped_column(SmallInteger)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    description: Mapped[str | None] = mapped_column(Text)


class PageEmbedding(IdMixin, TimestampMixin, Base):
    """One embedded chunk of a page's text, for vector (semantic) search.

    A page is split into chunks; each chunk has one vector per model. The vector's
    size is the model's (`embedding_models.dimensions`): `dimensions` is computed from
    the vector and the (model_name, dimensions) pair must match a registered model.
    `content_hash` records which version of the page was embedded, so a page whose
    text changed shows up in `SilverRepository.pages_missing_embeddings`.

    The per-model HNSW indexes are partial expression indexes, created by migrations
    and `register_embedding_model`, not declared here.
    """

    __tablename__ = "page_embeddings"
    __table_args__ = (
        UniqueConstraint(
            "page_id", "model_name", "chunk_index", name="uq_page_embeddings_page_model_chunk"
        ),
        CheckConstraint("chunk_index >= 0", name="chunk_index_non_negative"),
        ForeignKeyConstraint(
            ["model_name", "dimensions"],
            ["embedding_models.name", "embedding_models.dimensions"],
            name="fk_page_embeddings_model_name_embedding_models",
        ),
        Index("ix_page_embeddings_model_name_dimensions", "model_name", "dimensions"),
    )

    page_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), index=True
    )
    model_name: Mapped[str] = mapped_column(String(128))
    model_version: Mapped[str | None] = mapped_column(String(64))
    chunk_index: Mapped[int] = mapped_column(Integer)
    chunk_text: Mapped[str] = mapped_column(Text)
    embedding: Mapped[list[float]] = mapped_column(Vector())
    dimensions: Mapped[int] = mapped_column(
        SmallInteger, Computed("vector_dims(embedding)", persisted=True)
    )
    content_hash: Mapped[str] = mapped_column(String(64))
    embedded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    page: Mapped[Page] = relationship(back_populates="embeddings")


class QuarantinedFile(IdMixin, TimestampMixin, Base):
    """A payload ClamAV flagged during ETL, moved to the quarantine bucket.

    Written by the ETL when its virus scan of a claimed Bronze row fails
    (`SilverRepository.quarantine`); read by the API's admin security endpoints.
    The Bronze row is parked as QUARANTINED so it is never claimed again, and no
    page is built from it. Deleting the object (admin action) keeps this row as
    the audit record, with status DELETED.
    """

    __tablename__ = "quarantined_files"
    __table_args__ = (
        # One source when written; both NULL once Bronze retention deletes it (SET
        # NULL), so the audit record outlives the raw row.
        CheckConstraint(
            "num_nonnulls(crawled_document_id, stored_file_id) <= 1", name="at_most_one_source"
        ),
        # Rescanning the same file updates its record instead of adding one.
        UniqueConstraint(
            "document_url", "sha256", name="uq_quarantined_files_document_url_sha256"
        ),
        CheckConstraint("size_bytes IS NULL OR size_bytes >= 0", name="size_non_negative"),
        Index("ix_quarantined_files_status_scanned_at", "status", "scanned_at"),
    )

    # The Bronze row that carried the payload: a crawled page or a stored file.
    crawled_document_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("crawled_documents.id", ondelete="SET NULL"), index=True
    )
    stored_file_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("stored_files.id", ondelete="SET NULL"), index=True
    )
    domain_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("domains.id"), index=True
    )

    document_url: Mapped[str] = mapped_column(Text)  # what was downloaded
    source_page_url: Mapped[str | None] = mapped_column(Text)  # the page that linked it
    original_path: Mapped[str | None] = mapped_column(Text)  # where it sat in the raw bucket
    quarantine_path: Mapped[str] = mapped_column(Text)  # s3://quarantine-lake/...
    sha256: Mapped[str] = mapped_column(String(64), index=True)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    content_type: Mapped[str | None] = mapped_column(String(255))

    threat_signature: Mapped[str] = mapped_column(Text)  # e.g. Win.Trojan.Generic-998
    scanner_engine: Mapped[str] = mapped_column(String(64), default="ClamAV")
    scanner_version: Mapped[str | None] = mapped_column(String(64))
    scanned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    status: Mapped[QuarantineStatus] = mapped_column(
        str_enum(QuarantineStatus, "quarantine_status"),
        default=QuarantineStatus.QUARANTINED,
        server_default=QuarantineStatus.QUARANTINED.value,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # The admin's username. Free text, not an FK, so the audit record outlives the account.
    deleted_by: Mapped[str | None] = mapped_column(String(255))

    domain: Mapped[Domain | None] = relationship()
