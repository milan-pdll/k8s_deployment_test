"""`/api/v1/auth/*`: login request and responses."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class LoginRequest(BaseModel):
    """Log in with a username or an email (exactly one of them) and the password."""

    model_config = ConfigDict(extra="forbid")

    username: str | None = Field(default=None, min_length=1, max_length=64)
    email: str | None = Field(default=None, min_length=3, max_length=255)
    password: str = Field(min_length=1, max_length=1024)

    @field_validator("username", "email", mode="before")
    @classmethod
    def _strip(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip() or None
        return value

    @model_validator(mode="after")
    def _one_login(self) -> LoginRequest:
        if (self.username is None) == (self.email is None):
            raise ValueError("send either username or email")
        return self

    @property
    def login(self) -> str:
        return self.username or self.email or ""


class User(BaseModel):
    username: str
    email: str | None
    role: str = Field(description="SUPER_ADMIN | SYSTEM_OPERATOR | AUDITOR")


class LoginResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int = Field(description="Seconds until the token expires")
    user: User
