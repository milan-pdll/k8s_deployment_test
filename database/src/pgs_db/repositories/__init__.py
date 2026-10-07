"""Repository layer: the only code that writes the database tables."""

from ._mapping import (
    crawl_stats_columns,
    document_row,
    signed_simhash,
    stored_file_row,
    unsigned_simhash,
)
from .bronze import BronzeRepository, SaveResult
from .ops import OpsRepository
from .quarantine import QuarantineRepository
from .ranking import RankingRepository
from .reference import ReferenceRepository
from .search import SearchRepository, search_language
from .search_log import SearchLogRepository, normalize_query
from .silver import SilverRepository
from .stats import StatsRepository

__all__ = [
    "BronzeRepository",
    "OpsRepository",
    "QuarantineRepository",
    "RankingRepository",
    "ReferenceRepository",
    "SaveResult",
    "SearchLogRepository",
    "SearchRepository",
    "SilverRepository",
    "StatsRepository",
    "crawl_stats_columns",
    "document_row",
    "normalize_query",
    "search_language",
    "signed_simhash",
    "stored_file_row",
    "unsigned_simhash",
]
