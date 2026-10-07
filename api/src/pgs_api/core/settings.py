"""Typed configuration of the API gateway, read once from the environment at startup.

Every setting comes from one environment variable (see `ENV_VARS`). An empty value counts
as unset. `Settings.from_env` raises `SettingsError` with a message that names the
variables at fault and never echoes their values, so a bad secret does not end up in logs.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator

# API_AUTH_SECRET signs the admin tokens (HMAC-SHA256); anything shorter is guessable.
MIN_AUTH_SECRET_LENGTH = 32

# env var -> Settings field
ENV_VARS: dict[str, str] = {
    "DATABASE_URL": "database_url",
    "SEARCH_GRPC_HOST": "search_grpc_host",
    "SEARCH_GRPC_PORT": "search_grpc_port",
    "SEARCH_TIMEOUT_SECONDS": "search_timeout_seconds",
    "API_AUTH_SECRET": "auth_secret",
    "API_TOKEN_TTL_SECONDS": "token_ttl_seconds",
    "API_CORS_ORIGINS": "cors_origins",
    "LOG_LEVEL": "log_level",
}

_ORIGIN = re.compile(r"^https?://[^/\s]+$")


class SettingsError(RuntimeError):
    """The environment does not describe a valid configuration."""


class Settings(BaseModel):
    """The gateway's configuration. Frozen: read once, at startup."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # postgresql+psycopg://pgs_api:...@postgres:5432/pgs -- the API's own role.
    database_url: SecretStr
    search_grpc_host: str = Field(default="localhost", min_length=1)
    search_grpc_port: int = Field(default=50051, ge=1, le=65535)
    # Deadline of one ExecuteSearch call.
    search_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    # Unset: the API serves search, geo and health, and the auth/admin endpoints answer 503.
    auth_secret: SecretStr | None = None
    token_ttl_seconds: int = Field(default=8 * 3600, ge=60, le=7 * 24 * 3600)
    # Browser origins allowed to call the API cross-origin. Empty: no CORS headers at all
    # (the UI goes through nginx on the same origin, or calls the API server-side).
    cors_origins: tuple[str, ...] = ()
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("auth_secret")
    @classmethod
    def _secret_long_enough(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and len(value.get_secret_value()) < MIN_AUTH_SECRET_LENGTH:
            raise ValueError(f"must be at least {MIN_AUTH_SECRET_LENGTH} characters")
        return value

    @field_validator("cors_origins")
    @classmethod
    def _valid_origins(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for origin in value:
            if origin != "*" and not _ORIGIN.match(origin):
                raise ValueError(
                    f"{origin!r} is not an origin (scheme://host[:port], no path or trailing /)"
                )
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @property
    def search_target(self) -> str:
        return f"{self.search_grpc_host}:{self.search_grpc_port}"

    @property
    def auth_enabled(self) -> bool:
        return self.auth_secret is not None

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        raw: dict[str, object] = {}
        for name, field in ENV_VARS.items():
            value = env.get(name, "").strip()
            if not value:
                continue
            if field == "cors_origins":
                raw[field] = tuple(item.strip() for item in value.split(",") if item.strip())
            else:
                raw[field] = value
        try:
            return cls.model_validate(raw)
        except ValidationError as exc:
            raise SettingsError(_describe(exc)) from None


def _describe(exc: ValidationError) -> str:
    """One line per problem, by env var name. Never includes the offending values."""
    env_name = {field: name for name, field in ENV_VARS.items()}
    problems: list[str] = []
    for error in exc.errors(include_input=False, include_url=False):
        field = str(error["loc"][0]) if error["loc"] else ""
        problems.append(f"{env_name.get(field, field)}: {error['msg']}")
    return "invalid API configuration: " + "; ".join(problems)
