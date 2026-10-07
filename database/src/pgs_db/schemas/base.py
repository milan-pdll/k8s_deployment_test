"""Shared Pydantic schema configuration."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SchemaBase(BaseModel):
    """Base class used by all request and response schemas."""

    model_config = ConfigDict(
        from_attributes=True,
        extra="forbid",
        str_strip_whitespace=True,
    )


class ReadSchema(SchemaBase):
    """Fields returned for records loaded from the database."""

    id: int
    created_at: datetime
    updated_at: datetime
