"""Admin access tokens: compact JWS (JWT) signed with HMAC-SHA256, standard library only.

`<base64url(header)>.<base64url(claims)>.<base64url(signature)>`, header
`{"alg": "HS256", "typ": "JWT"}`, claims `iss`, `sub` (username), `uid` (admin_users.id),
`role`, `iat` and `exp` (Unix seconds). Verification recomputes the signature and
compares it in constant time before it reads the claims, accepts HS256 only, and
rejects expired tokens. The secret is `API_AUTH_SECRET`.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

ISSUER = "pgs-api"
MAX_TOKEN_LENGTH = 4096
_HEADER = {"alg": "HS256", "typ": "JWT"}
_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")


class TokenError(Exception):
    """The token is malformed, forged or expired. The message is safe to log."""


@dataclass(frozen=True)
class TokenClaims:
    subject: str  # admin_users.username
    user_id: int
    role: str
    issued_at: int
    expires_at: int


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(segment: str) -> bytes:
    if not _SEGMENT.match(segment):
        raise TokenError("malformed token")
    try:
        return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    except (binascii.Error, ValueError):
        raise TokenError("malformed token") from None


def _json_segment(value: dict[str, Any]) -> str:
    return _b64encode(json.dumps(value, separators=(",", ":"), sort_keys=True).encode())


def _decode_object(segment: str) -> dict[str, Any]:
    try:
        value: object = json.loads(_b64decode(segment))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise TokenError("malformed token") from None
    if not isinstance(value, dict):
        raise TokenError("malformed token")
    return cast("dict[str, Any]", value)  # JSON object keys are always strings


def _int_claim(claims: dict[str, Any], name: str) -> int:
    value = claims.get(name)
    # bool is an int subclass; a JSON true is not a timestamp.
    if not isinstance(value, int) or isinstance(value, bool):
        raise TokenError(f"claim {name} missing or not an integer")
    return value


def _str_claim(claims: dict[str, Any], name: str) -> str:
    value = claims.get(name)
    if not isinstance(value, str) or not value:
        raise TokenError(f"claim {name} missing or empty")
    return value


class TokenCodec:
    """Issues and verifies tokens with one secret and lifetime."""

    def __init__(
        self, secret: str, ttl_seconds: int, *, clock: Callable[[], float] = time.time
    ) -> None:
        if not secret:
            raise ValueError("secret must not be empty")
        self._key = secret.encode()
        self.ttl_seconds = ttl_seconds
        self._clock = clock

    def _sign(self, signing_input: str) -> bytes:
        return hmac.new(self._key, signing_input.encode("ascii"), hashlib.sha256).digest()

    def issue(self, *, subject: str, user_id: int, role: str) -> str:
        now = int(self._clock())
        claims = {
            "iss": ISSUER,
            "sub": subject,
            "uid": user_id,
            "role": role,
            "iat": now,
            "exp": now + self.ttl_seconds,
        }
        signing_input = f"{_json_segment(_HEADER)}.{_json_segment(claims)}"
        return f"{signing_input}.{_b64encode(self._sign(signing_input))}"

    def verify(self, token: str) -> TokenClaims:
        if len(token) > MAX_TOKEN_LENGTH:
            raise TokenError("token too long")
        parts = token.split(".")
        if len(parts) != 3:
            raise TokenError("malformed token")
        header_segment, claims_segment, signature_segment = parts
        signature = _b64decode(signature_segment)
        expected = self._sign(f"{header_segment}.{claims_segment}")
        if not hmac.compare_digest(signature, expected):
            raise TokenError("bad signature")
        # Signed by us, so these are our own bytes; still checked, never trusted blindly.
        if _decode_object(header_segment) != _HEADER:
            raise TokenError("unsupported token header")
        claims = _decode_object(claims_segment)
        if claims.get("iss") != ISSUER:
            raise TokenError("wrong issuer")
        expires_at = _int_claim(claims, "exp")
        if expires_at <= int(self._clock()):
            raise TokenError("token expired")
        return TokenClaims(
            subject=_str_claim(claims, "sub"),
            user_id=_int_claim(claims, "uid"),
            role=_str_claim(claims, "role"),
            issued_at=_int_claim(claims, "iat"),
            expires_at=expires_at,
        )
