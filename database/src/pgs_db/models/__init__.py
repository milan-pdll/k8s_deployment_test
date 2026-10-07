"""Import every model here so Alembic autogenerate sees all tables."""

from .crawl import BronzeIngestState, CrawledDocument, CrawlRun, StoredFile
from .domain import Domain
from .geography import District, LocalBody, Province, RegionLink
from .gold import (
    DomainStats,
    GeoContentStats,
    PageScore,
    RelevanceJudgment,
    SearchClick,
    SearchQuery,
)
from .ops import AdminUser, ErrorLog
from .silver import (
    DEFAULT_EMBEDDING_MODEL,
    EmbeddingModel,
    Entity,
    Page,
    PageContact,
    PageEmbedding,
    PageEntity,
    PageGeoTag,
    PageMedia,
    PageSource,
    QuarantinedFile,
)
from .views import page_geo_codes, search_documents

__all__ = [
    "DEFAULT_EMBEDDING_MODEL",
    # ops
    "AdminUser",
    # bronze
    "BronzeIngestState",
    "CrawlRun",
    "CrawledDocument",
    "District",
    # reference
    "Domain",
    # gold
    "DomainStats",
    "EmbeddingModel",
    "Entity",
    "ErrorLog",
    "GeoContentStats",
    "LocalBody",
    # silver
    "Page",
    "PageContact",
    "PageEmbedding",
    "PageEntity",
    "PageGeoTag",
    "PageMedia",
    "PageScore",
    "PageSource",
    "Province",
    "QuarantinedFile",
    "RegionLink",
    "RelevanceJudgment",
    "SearchClick",
    "SearchQuery",
    "StoredFile",
    # views (read-only)
    "page_geo_codes",
    "search_documents",
]
