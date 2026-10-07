"""`POST /api/v1/auth/login` and `GET /api/v1/auth/me`."""

from __future__ import annotations

import logging
from typing import Annotated, Any

from fastapi import APIRouter, Depends

from pgs_db.security import verify_password

from ..auth.dependencies import (
    INVALID_CREDENTIALS,
    CurrentAdmin,
    burn_password_check,
    require_token_codec,
    unauthorized,
    upgraded_hash,
)
from ..auth.tokens import TokenCodec
from ..core.services import ServicesDep
from ..schemas.auth import LoginRequest, LoginResponse, User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

_AUTH_ERRORS: dict[int | str, dict[str, Any]] = {
    401: {"description": "Wrong credentials, or a missing/invalid/expired token"},
    503: {"description": "API_AUTH_SECRET is not set on the server"},
}


@router.post(
    "/login",
    response_model=LoginResponse,
    summary="Exchange an admin's username or email and password for an access token",
    responses=_AUTH_ERRORS,
)
def login(
    body: LoginRequest,
    services: ServicesDep,
    tokens: Annotated[TokenCodec, Depends(require_token_codec)],
) -> LoginResponse:
    account = services.db.find_admin(body.login)
    if account is None:
        burn_password_check(body.password)
        logger.info("admin login failed")
        raise unauthorized(INVALID_CREDENTIALS)
    if not verify_password(account.password_hash, body.password):
        logger.info("admin login failed")
        raise unauthorized(INVALID_CREDENTIALS)
    try:
        services.db.record_login(account.id, upgraded_hash(account.password_hash, body.password))
    except LookupError:  # deleted between the two statements
        raise unauthorized(INVALID_CREDENTIALS) from None
    logger.info("admin login", extra={"username": account.username})
    return LoginResponse(
        access_token=tokens.issue(subject=account.username, user_id=account.id, role=account.role),
        expires_in=tokens.ttl_seconds,
        user=User(username=account.username, email=account.email, role=account.role),
    )


@router.get(
    "/me",
    response_model=User,
    summary="The admin the bearer token belongs to",
    responses=_AUTH_ERRORS,
)
def me(admin: CurrentAdmin) -> User:
    return User(username=admin.username, email=admin.email, role=admin.role)
