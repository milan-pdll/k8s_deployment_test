from __future__ import annotations

import pytest

from pgs_api.core.settings import Settings, SettingsError

DB = "postgresql+psycopg://pgs_api:db-password@postgres:5432/pgs"


def test_defaults() -> None:
    settings = Settings.from_env({"DATABASE_URL": DB})

    assert settings.database_url.get_secret_value() == DB
    assert settings.search_target == "localhost:50051"
    assert settings.search_timeout_seconds == 5.0
    assert settings.auth_enabled is False
    assert settings.token_ttl_seconds == 28800
    assert settings.cors_origins == ()
    assert settings.log_level == "INFO"


def test_all_variables() -> None:
    settings = Settings.from_env(
        {
            "DATABASE_URL": DB,
            "SEARCH_GRPC_HOST": "search-engine",
            "SEARCH_GRPC_PORT": "50052",
            "SEARCH_TIMEOUT_SECONDS": "2.5",
            "API_AUTH_SECRET": "z" * 32,
            "API_TOKEN_TTL_SECONDS": "3600",
            "API_CORS_ORIGINS": "https://pgs.example.org, http://localhost:3000,",
            "LOG_LEVEL": "debug",
        }
    )

    assert settings.search_target == "search-engine:50052"
    assert settings.search_timeout_seconds == 2.5
    assert settings.auth_enabled is True
    assert settings.token_ttl_seconds == 3600
    assert settings.cors_origins == ("https://pgs.example.org", "http://localhost:3000")
    assert settings.log_level == "DEBUG"


def test_empty_values_count_as_unset() -> None:
    settings = Settings.from_env(
        {"DATABASE_URL": DB, "API_AUTH_SECRET": "", "SEARCH_GRPC_HOST": " "}
    )

    assert settings.auth_enabled is False
    assert settings.search_grpc_host == "localhost"


def test_missing_database_url() -> None:
    with pytest.raises(SettingsError, match="DATABASE_URL"):
        Settings.from_env({})


def test_errors_name_the_variable_but_never_its_value() -> None:
    with pytest.raises(SettingsError) as caught:
        Settings.from_env(
            {"DATABASE_URL": DB, "API_AUTH_SECRET": "short-secret", "SEARCH_TIMEOUT_SECONDS": "0"}
        )

    message = str(caught.value)
    assert "API_AUTH_SECRET" in message
    assert "SEARCH_TIMEOUT_SECONDS" in message
    assert "short-secret" not in message
    assert "db-password" not in message


@pytest.mark.parametrize("origins", ["https://pgs.example.org/", "pgs.example.org", "https://a b"])
def test_invalid_cors_origin(origins: str) -> None:
    with pytest.raises(SettingsError, match="API_CORS_ORIGINS"):
        Settings.from_env({"DATABASE_URL": DB, "API_CORS_ORIGINS": origins})


def test_secret_is_not_in_repr() -> None:
    settings = Settings.from_env({"DATABASE_URL": DB, "API_AUTH_SECRET": "q" * 40})

    assert "q" * 40 not in repr(settings)
    assert "db-password" not in repr(settings)
