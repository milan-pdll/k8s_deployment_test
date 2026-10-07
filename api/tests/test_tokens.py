from __future__ import annotations

import base64
import json

import pytest

from pgs_api.auth.tokens import MAX_TOKEN_LENGTH, TokenCodec, TokenError

SECRET = "s" * 40


def _codec(now: float = 1_000_000.0, ttl: int = 600) -> TokenCodec:
    return TokenCodec(SECRET, ttl, clock=lambda: now)


def _segment(value: object) -> str:
    return base64.urlsafe_b64encode(json.dumps(value).encode()).rstrip(b"=").decode()


def test_round_trip() -> None:
    token = _codec().issue(subject="ops", user_id=7, role="AUDITOR")

    claims = _codec(now=1_000_100.0).verify(token)

    assert (claims.subject, claims.user_id, claims.role) == ("ops", 7, "AUDITOR")
    assert (claims.issued_at, claims.expires_at) == (1_000_000, 1_000_600)


def test_expired() -> None:
    token = _codec().issue(subject="ops", user_id=7, role="AUDITOR")

    with pytest.raises(TokenError, match="expired"):
        _codec(now=1_000_600.0).verify(token)


def test_other_secret() -> None:
    token = TokenCodec("t" * 40, 600).issue(subject="ops", user_id=7, role="AUDITOR")

    with pytest.raises(TokenError, match="signature"):
        TokenCodec(SECRET, 600).verify(token)


def test_tampered_claims() -> None:
    header, _, signature = _codec().issue(subject="ops", user_id=7, role="AUDITOR").split(".")
    claims = _segment(
        {"iss": "pgs-api", "sub": "ops", "uid": 7, "role": "SUPER_ADMIN", "iat": 0, "exp": 2**40}
    )

    with pytest.raises(TokenError, match="signature"):
        _codec().verify(f"{header}.{claims}.{signature}")


def test_alg_none_is_rejected() -> None:
    _, claims, _ = _codec().issue(subject="ops", user_id=7, role="AUDITOR").split(".")
    header = _segment({"alg": "none", "typ": "JWT"})

    with pytest.raises(TokenError):
        _codec().verify(f"{header}.{claims}.")


@pytest.mark.parametrize(
    "token",
    ["", "abc", "a.b", "a.b.c.d", "a.b.c", "a.b.!!!", "é.é.é", "a" * (MAX_TOKEN_LENGTH + 1)],
)
def test_malformed(token: str) -> None:
    with pytest.raises(TokenError):
        _codec().verify(token)
