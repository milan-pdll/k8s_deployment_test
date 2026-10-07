"""Password hashing for `admin_users`: one format for the API, the admin CLI and tests.

Argon2id with argon2-cffi's defaults (RFC 9106 low-memory profile). Install the extra:
`pip install "pgs-db[auth]"`. The API calls `verify_password` at login and, when it
returns True, `needs_rehash` -- if that is True, store `hash_password(password)` again
so old hashes are upgraded as parameters are raised.
"""

from typing import Any


def _hasher() -> Any:
    try:
        from argon2 import PasswordHasher
    except ImportError as exc:  # pragma: no cover - depends on the install
        raise RuntimeError(
            'password hashing needs the auth extra: pip install "pgs-db[auth]"'
        ) from exc
    return PasswordHasher()


MIN_PASSWORD_LENGTH = 12


def hash_password(password: str) -> str:
    """An Argon2id hash for `admin_users.password_hash`. Rejects short passwords."""
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"password must be at least {MIN_PASSWORD_LENGTH} characters")
    return str(_hasher().hash(password))


def verify_password(password_hash: str, password: str) -> bool:
    """True when `password` matches. Never raises for a wrong password or a bad hash."""
    from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

    try:
        return bool(_hasher().verify(password_hash, password))
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def needs_rehash(password_hash: str) -> bool:
    """True when the hash was made with weaker parameters than the current defaults."""
    return bool(_hasher().check_needs_rehash(password_hash))
