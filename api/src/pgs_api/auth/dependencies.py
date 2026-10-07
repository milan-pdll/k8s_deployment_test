"""Admin authentication: password check at login, bearer-token check on admin routes.

Passwords are verified with pgs_db.security (Argon2id). An unknown login still costs one
hash verification, so response time does not reveal which usernames exist. A valid token
is not enough on its own: the account must still exist and be active, and its current
role (from the database, not the token) is what the request gets.
"""

from __future__ import annotations

import functools
import logging
import secrets
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from pgs_db.security import hash_password, needs_rehash, verify_password

from ..core.services import ServicesDep
from .tokens import TokenCodec, TokenError

logger = logging.getLogger(__name__)

INVALID_CREDENTIALS = "Invalid username/email or password."
AUTH_NOT_CONFIGURED = (
    "Authentication is not configured on this server (API_AUTH_SECRET is not set)."
)

bearer_scheme = HTTPBearer(
    auto_error=False,
    description="Access token from POST /api/v1/auth/login",
)


@dataclass(frozen=True)
class Principal:
    user_id: int
    username: str
    email: str | None
    role: str


def unauthorized(detail: str) -> HTTPException:
    return HTTPException(
        status.HTTP_401_UNAUTHORIZED, detail=detail, headers={"WWW-Authenticate": "Bearer"}
    )


def require_token_codec(services: ServicesDep) -> TokenCodec:
    if services.tokens is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail=AUTH_NOT_CONFIGURED)
    return services.tokens


@functools.cache
def _dummy_hash() -> str:
    return hash_password(secrets.token_urlsafe(32))


def burn_password_check(password: str) -> None:
    """Spend the time of a real verification when the login matched no account."""
    verify_password(_dummy_hash(), password)


def upgraded_hash(password_hash: str, password: str) -> str | None:
    """A fresh hash when the stored one uses weaker parameters than today's defaults."""
    try:
        return hash_password(password) if needs_rehash(password_hash) else None
    except ValueError:
        # The password predates the current minimum length; keep the hash it has.
        return None


def current_admin(
    services: ServicesDep,
    tokens: Annotated[TokenCodec, Depends(require_token_codec)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
) -> Principal:
    if credentials is None:
        raise unauthorized("Missing bearer token.")
    try:
        claims = tokens.verify(credentials.credentials)
    except TokenError as exc:
        logger.info("bearer token rejected", extra={"reason": str(exc)})
        raise unauthorized("Invalid or expired token.") from None
    account = services.db.find_admin(claims.subject)
    if account is None or account.id != claims.user_id:
        raise unauthorized("This account is no longer active.")
    return Principal(
        user_id=account.id, username=account.username, email=account.email, role=account.role
    )


CurrentAdmin = Annotated[Principal, Depends(current_admin)]
