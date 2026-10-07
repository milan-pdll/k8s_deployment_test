from __future__ import annotations

from collections.abc import Callable

import pytest
from argon2 import PasswordHasher

from pgs_api.auth.tokens import TokenCodec
from pgs_api.db.database import AdminAccount
from pgs_db.security import hash_password

from .conftest import AUTH_SECRET, PASSWORD, Harness

ADMIN = AdminAccount(
    id=7,
    username="ops",
    email="ops@example.gov.np",
    role="SYSTEM_OPERATOR",
    password_hash=hash_password(PASSWORD),
)


@pytest.fixture
def harness(make_harness: Callable[..., Harness]) -> Harness:
    built = make_harness()
    built.db.admins[ADMIN.username] = ADMIN
    return built


def _login(harness: Harness, **body: object) -> str:
    response = harness.client.post("/api/v1/auth/login", json=body)
    assert response.status_code == 200, response.text
    return str(response.json()["access_token"])


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.parametrize("login", [{"username": "ops"}, {"email": "OPS@example.gov.np "}])
def test_login_returns_a_token_and_the_user(harness: Harness, login: dict[str, str]) -> None:
    response = harness.client.post("/api/v1/auth/login", json={**login, "password": PASSWORD})

    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert body["expires_in"] == 8 * 3600
    assert body["user"] == {
        "username": "ops",
        "email": "ops@example.gov.np",
        "role": "SYSTEM_OPERATOR",
    }
    claims = TokenCodec(AUTH_SECRET, 60).verify(body["access_token"])
    assert (claims.subject, claims.user_id, claims.role) == ("ops", 7, "SYSTEM_OPERATOR")
    assert harness.db.logins == [(7, None)]


def test_wrong_password_is_401(harness: Harness) -> None:
    response = harness.client.post(
        "/api/v1/auth/login", json={"username": "ops", "password": "wrong password!"}
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid username/email or password."}
    assert response.headers["www-authenticate"] == "Bearer"
    assert harness.db.logins == []


def test_unknown_user_is_401_with_the_same_message(harness: Harness) -> None:
    response = harness.client.post(
        "/api/v1/auth/login", json={"username": "nobody", "password": PASSWORD}
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid username/email or password."}


@pytest.mark.parametrize(
    "body",
    [
        {"password": PASSWORD},
        {"username": "ops", "email": "ops@example.gov.np", "password": PASSWORD},
        {"username": "ops"},
        {"username": "ops", "password": ""},
        {"username": "ops", "password": PASSWORD, "remember": True},
    ],
)
def test_malformed_login_is_400(harness: Harness, body: dict[str, object]) -> None:
    response = harness.client.post("/api/v1/auth/login", json=body)

    assert response.status_code == 400


def test_login_errors_never_echo_the_password(harness: Harness) -> None:
    response = harness.client.post(
        "/api/v1/auth/login",
        json={"username": "ops", "password": ["hunter2-is-my-password"]},
    )

    assert response.status_code == 400
    assert "hunter2" not in response.text


def test_a_weak_hash_is_upgraded_at_login(harness: Harness) -> None:
    weak = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1).hash(PASSWORD)
    harness.db.admins["ops"] = AdminAccount(7, "ops", None, "AUDITOR", weak)

    _login(harness, username="ops", password=PASSWORD)

    ((user_id, new_hash),) = harness.db.logins
    assert user_id == 7
    assert new_hash is not None and new_hash != weak


def test_me_returns_the_tokens_user(harness: Harness) -> None:
    token = _login(harness, username="ops", password=PASSWORD)

    response = harness.client.get("/api/v1/auth/me", headers=_bearer(token))

    assert response.status_code == 200
    assert response.json() == {
        "username": "ops",
        "email": "ops@example.gov.np",
        "role": "SYSTEM_OPERATOR",
    }


def test_role_comes_from_the_database_not_the_token(harness: Harness) -> None:
    token = _login(harness, username="ops", password=PASSWORD)
    harness.db.admins["ops"] = AdminAccount(7, "ops", None, "AUDITOR", ADMIN.password_hash)

    response = harness.client.get("/api/v1/auth/me", headers=_bearer(token))

    assert response.json()["role"] == "AUDITOR"


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Basic b3BzOnB3"},
        {"Authorization": "Bearer not-a-token"},
        {"Authorization": "Bearer a.b.c"},
    ],
)
def test_me_without_a_valid_token_is_401(harness: Harness, headers: dict[str, str]) -> None:
    response = harness.client.get("/api/v1/auth/me", headers=headers)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_forged_token_is_401(harness: Harness) -> None:
    forged = TokenCodec("another-secret-" + "y" * 40, 3600).issue(
        subject="ops", user_id=7, role="SUPER_ADMIN"
    )

    response = harness.client.get("/api/v1/auth/me", headers=_bearer(forged))

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or expired token."}


def test_expired_token_is_401(harness: Harness) -> None:
    expired = TokenCodec(AUTH_SECRET, 60, clock=lambda: 1_000_000.0).issue(
        subject="ops", user_id=7, role="SYSTEM_OPERATOR"
    )

    response = harness.client.get("/api/v1/auth/me", headers=_bearer(expired))

    assert response.status_code == 401


def test_token_of_a_deactivated_or_replaced_account_is_401(harness: Harness) -> None:
    token = _login(harness, username="ops", password=PASSWORD)
    # Same username, different row (deleted and re-created).
    harness.db.admins["ops"] = AdminAccount(8, "ops", None, "AUDITOR", ADMIN.password_hash)
    assert harness.client.get("/api/v1/auth/me", headers=_bearer(token)).status_code == 401

    del harness.db.admins["ops"]  # deactivated: get_admin hides inactive accounts
    assert harness.client.get("/api/v1/auth/me", headers=_bearer(token)).status_code == 401


def test_admin_summary_requires_a_token(harness: Harness) -> None:
    assert harness.client.get("/api/v1/admin/summary").status_code == 401


def test_admin_summary(harness: Harness) -> None:
    token = _login(harness, username="ops", password=PASSWORD)

    response = harness.client.get("/api/v1/admin/summary", headers=_bearer(token))

    assert response.status_code == 200
    body = response.json()
    assert body["domains"]["total_registered"] == 3
    assert body["errors_last_24h"] == {"WARN": 1, "ERROR": 0, "FATAL": 0}
    assert body["search_traffic"]["searches"] == 2
    assert body["search_traffic"]["since"] < body["search_traffic"]["until"]
    assert response.headers["cache-control"] == "no-store"


def test_without_auth_secret_auth_endpoints_are_503(
    make_harness: Callable[..., Harness],
) -> None:
    harness = make_harness(auth=False)

    login = harness.client.post("/api/v1/auth/login", json={"username": "x", "password": "y"})
    me = harness.client.get("/api/v1/auth/me", headers=_bearer("a.b.c"))
    summary = harness.client.get("/api/v1/admin/summary")

    assert (login.status_code, me.status_code, summary.status_code) == (503, 503, 503)
    assert "API_AUTH_SECRET" in login.json()["detail"]
    # Public endpoints keep working.
    assert harness.client.get("/api/v1/search", params={"q": "x"}).status_code == 200
