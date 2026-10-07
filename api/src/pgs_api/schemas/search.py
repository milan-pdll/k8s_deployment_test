"""`GET /api/v1/search`: query parameters and response."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from .geo import DISTRICT_CODE, LOCAL_BODY_CODE, PROVINCE_CODE

CONTENT_TYPE = r"^[a-z0-9_]{1,32}$"

# The search engine's limits (search-engine/proto/search.proto).
MAX_QUERY_LENGTH = 512
MAX_LIMIT = 100
MAX_RESULT_WINDOW = 500

Language = Literal["auto", "ne", "en", "mixed"]


class SearchParams(BaseModel):
    """Query string of `GET /api/v1/search`. Empty filter values mean "no filter"."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    q: str = Field(
        min_length=1, max_length=MAX_QUERY_LENGTH, description="Search text, 1-512 characters"
    )
    page: int = Field(default=1, ge=1, description="1-based page number")
    limit: int = Field(default=10, ge=1, le=MAX_LIMIT, description="Results per page")
    province_code: str | None = Field(default=None, pattern=PROVINCE_CODE, examples=["P4"])
    district_code: str | None = Field(default=None, pattern=DISTRICT_CODE, examples=["D38"])
    municipality_id: str | None = Field(
        default=None, pattern=LOCAL_BODY_CODE, description="Local body code", examples=["MUN414"]
    )
    ward_number: int | None = Field(default=None, ge=1, le=99)
    language: Language = Field(default="auto", description="auto = no language filter")
    content_type: str = Field(
        default="all", pattern=CONTENT_TYPE, description="all = no content-type filter"
    )

    @field_validator("province_code", "district_code", "municipality_id", mode="before")
    @classmethod
    def _code(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().upper() or None
        return value

    @field_validator("ward_number", mode="before")
    @classmethod
    def _ward(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("language", "content_type", mode="before")
    @classmethod
    def _keyword(cls, value: object, info: ValidationInfo) -> object:
        if isinstance(value, str):
            return value.strip().lower() or ("auto" if info.field_name == "language" else "all")
        return value

    @model_validator(mode="after")
    def _within_result_window(self) -> SearchParams:
        if self.page * self.limit > MAX_RESULT_WINDOW:
            raise ValueError(f"page * limit must not exceed {MAX_RESULT_WINDOW}")
        return self

    def filters(self) -> dict[str, Any]:
        """The filters actually applied, for the search log."""
        applied: dict[str, Any] = {
            "province_code": self.province_code,
            "district_code": self.district_code,
            "municipality_id": self.municipality_id,
            "ward_number": self.ward_number,
            "language": None if self.language == "auto" else self.language,
            "content_type": None if self.content_type == "all" else self.content_type,
        }
        return {key: value for key, value in applied.items() if value is not None}


class GeoRef(BaseModel):
    """A result's primary location. Fields the engine does not know are null."""

    province_code: str | None = None
    province_name: str | None = None
    district_code: str | None = None
    district_name: str | None = None
    municipality_id: str | None = None
    municipality_name: str | None = None
    ward_number: int | None = None


class SearchResult(BaseModel):
    id: str = Field(description="pages.id in PostgreSQL")
    title: str
    url: str
    domain: str
    snippet: str
    result_type: str
    language: str = Field(description="ne | en | mixed | unknown")
    published_at: str | None = Field(description="RFC 3339, null when unknown")
    relevance_score: float
    geo: GeoRef | None


class SearchResponse(BaseModel):
    query: str
    page: int
    limit: int
    total_hits: int = Field(description="Ranked results available for paging (capped at 500)")
    took_ms: int = Field(description="Time the gateway waited for the search engine")
    query_language: str = Field(description="ne | en | mixed | unknown")
    degraded: bool = Field(
        description="An optional stage (vectors, translation, rerank) failed; results are partial"
    )
    results: list[SearchResult]
