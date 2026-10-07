"""Pydantic schemas for websites registered for crawling."""

import re

from pydantic import Field, field_validator

from pgs_db.enums import DomainCategory, DomainPriority, DomainStatus

from .base import ReadSchema, SchemaBase

DOMAIN_PATTERN = re.compile(
    r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
    r"[a-zA-Z]{2,63}$"
)


def validate_domain_name(value: str) -> str:
    """Normalize and validate a hostname such as example.gov.np."""

    normalized = value.strip().lower().rstrip(".")

    if "://" in normalized or "/" in normalized:
        raise ValueError("domain must be a hostname, not a complete URL")

    if not DOMAIN_PATTERN.fullmatch(normalized):
        raise ValueError("invalid domain name")

    return normalized


class DomainBase(SchemaBase):
    """Fields shared by all domain schemas."""

    domain: str = Field(min_length=4, max_length=255)
    website_name: str | None = Field(default=None, max_length=255)
    category: DomainCategory = DomainCategory.OTHER
    status: DomainStatus = DomainStatus.PENDING
    priority: DomainPriority = DomainPriority.NORMAL
    rate_limit_per_sec: int = Field(default=1, gt=0)
    local_body_id: int | None = Field(default=None, gt=0)

    @field_validator("domain")
    @classmethod
    def check_domain(cls, value: str) -> str:
        """Validate and normalize the domain name."""

        return validate_domain_name(value)


class DomainCreate(DomainBase):
    """Data required when registering a domain."""


class DomainUpdate(SchemaBase):
    """Fields that may be changed for an existing domain."""

    website_name: str | None = Field(default=None, max_length=255)
    category: DomainCategory | None = None
    status: DomainStatus | None = None
    priority: DomainPriority | None = None
    rate_limit_per_sec: int | None = Field(default=None, gt=0)
    local_body_id: int | None = Field(default=None, gt=0)


class DomainRead(DomainBase, ReadSchema):
    """Domain data returned by the application."""
