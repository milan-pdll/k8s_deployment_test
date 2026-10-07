"""Admin accounts and the shared error log.

`admin_users` backs the API's `POST /api/v1/auth/login`: the API verifies the
password against `password_hash` itself, then calls `record_login`. `error_logs`
is written by every service through `log_error` and read by
`GET /api/v1/admin/logs/errors` through `list_errors`.
"""

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from ..enums import AdminRole, LogSeverity, ServiceName
from ..models import AdminUser, Domain, ErrorLog


def _utcnow() -> datetime:
    return datetime.now(UTC)


class OpsRepository:
    """Admin-user and error-log access for one SQLAlchemy session."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ---------------------------------------------------------------- admin users

    def create_admin(
        self,
        username: str,
        *,
        password_hash: str,
        role: AdminRole,
        email: str | None = None,
    ) -> AdminUser:
        """Add an admin account. Hash the password before calling; this stores the hash as is.

        Raises `IntegrityError` on flush if the username or email is taken.
        """
        name = username.strip().lower()
        if not name:
            raise ValueError("username must not be empty")
        if "@" in name:
            raise ValueError("username must not contain '@' (login uses it to spot an email)")
        if not password_hash:
            raise ValueError("password_hash must not be empty")
        user = AdminUser(
            username=name,
            email=email.strip().lower() if email else None,
            password_hash=password_hash,
            role=role,
        )
        self.session.add(user)
        self.session.flush()
        return user

    def get_admin(self, login: str, *, active_only: bool = True) -> AdminUser | None:
        """The account to check a login against, by username or email.

        The API spec logs in with a username, the UI with an email; both work.
        A value with "@" is matched against email, anything else against username
        (usernames cannot contain "@"). Deactivated accounts are hidden by default.
        """
        value = login.strip().lower()
        column = AdminUser.email if "@" in value else AdminUser.username
        stmt = select(AdminUser).where(column == value)
        if active_only:
            stmt = stmt.where(AdminUser.is_active.is_(True))
        return self.session.scalar(stmt)

    def list_admins(self, *, include_inactive: bool = False) -> list[AdminUser]:
        stmt = select(AdminUser).order_by(AdminUser.username)
        if not include_inactive:
            stmt = stmt.where(AdminUser.is_active.is_(True))
        return list(self.session.scalars(stmt).all())

    def record_login(self, user_id: int, *, when: datetime | None = None) -> None:
        """Stamp a successful login. Call only after the password check passed."""
        self._require_admin(user_id).last_login_at = when or _utcnow()

    def set_password_hash(self, user_id: int, password_hash: str) -> None:
        if not password_hash:
            raise ValueError("password_hash must not be empty")
        self._require_admin(user_id).password_hash = password_hash

    def set_role(self, user_id: int, role: AdminRole) -> AdminUser:
        """Change an admin's role. Refuses to demote the last active SUPER_ADMIN."""
        user = self._require_admin(user_id)
        if role != AdminRole.SUPER_ADMIN:
            self._guard_last_super_admin(user)
        user.role = role
        return user

    def set_active(self, user_id: int, active: bool) -> AdminUser:
        """Deactivate or reactivate an account.

        Refuses to deactivate the last active SUPER_ADMIN."""
        user = self._require_admin(user_id)
        if not active:
            self._guard_last_super_admin(user)
        user.is_active = active
        return user

    def _require_admin(self, user_id: int) -> AdminUser:
        user = self.session.get(AdminUser, user_id)
        if user is None:
            raise LookupError(f"admin user {user_id} does not exist")
        return user

    def _guard_last_super_admin(self, user: AdminUser) -> None:
        """Nobody could manage admins again if the last super admin lost the role."""
        if user.role != AdminRole.SUPER_ADMIN or not user.is_active:
            return
        others = self.session.scalar(
            select(func.count()).where(
                AdminUser.role == AdminRole.SUPER_ADMIN,
                AdminUser.is_active.is_(True),
                AdminUser.id != user.id,
            )
        )
        if not others:
            raise ValueError("cannot remove the last active SUPER_ADMIN")

    # ----------------------------------------------------------------- error logs

    def log_error(
        self,
        service: ServiceName,
        severity: LogSeverity,
        message: str,
        *,
        instance: str | None = None,
        error_type: str | None = None,
        url: str | None = None,
        context: dict[str, Any] | None = None,
        domain_id: int | None = None,
        crawl_run_id: int | None = None,
        crawled_document_id: int | None = None,
        page_id: int | None = None,
        occurred_at: datetime | None = None,
    ) -> ErrorLog:
        """Record one failure. `domain_id` is looked up from `url` when not given."""
        if not message.strip():
            raise ValueError("message must not be empty")
        if domain_id is None and url:
            domain_id = self._domain_id_for_url(url)
        entry = ErrorLog(
            service=service,
            severity=severity,
            message=message,
            instance=instance,
            error_type=error_type,
            url=url,
            context=context,
            domain_id=domain_id,
            crawl_run_id=crawl_run_id,
            crawled_document_id=crawled_document_id,
            page_id=page_id,
        )
        if occurred_at is not None:
            entry.occurred_at = occurred_at
        self.session.add(entry)
        self.session.flush()
        return entry

    def list_errors(
        self,
        *,
        service: ServiceName | None = None,
        severity: LogSeverity | None = None,
        domain_id: int | None = None,
        since: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[ErrorLog], int]:
        """One page of log entries, newest first, plus the total matching count."""
        stmt = select(ErrorLog)
        if service is not None:
            stmt = stmt.where(ErrorLog.service == service)
        if severity is not None:
            stmt = stmt.where(ErrorLog.severity == severity)
        if domain_id is not None:
            stmt = stmt.where(ErrorLog.domain_id == domain_id)
        if since is not None:
            stmt = stmt.where(ErrorLog.occurred_at >= since)
        total = self.session.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = self.session.scalars(
            stmt.order_by(ErrorLog.occurred_at.desc(), ErrorLog.id.desc())
            .limit(limit)
            .offset(offset)
        ).all()
        return list(rows), int(total)

    def count_errors_since(self, since: datetime) -> dict[str, int]:
        """Entries per severity since `since`, every severity present (0 when none)."""
        counts = {severity.value: 0 for severity in LogSeverity}
        rows = self.session.execute(
            select(ErrorLog.severity, func.count())
            .where(ErrorLog.occurred_at >= since)
            .group_by(ErrorLog.severity)
        ).all()
        for severity, count in rows:
            counts[LogSeverity(severity).value] = int(count)
        return counts

    def purge_errors(self, older_than: datetime) -> int:
        """Delete entries that occurred before `older_than`. Returns how many went."""
        result = self.session.execute(delete(ErrorLog).where(ErrorLog.occurred_at < older_than))
        return int(result.rowcount or 0)  # type: ignore[attr-defined]

    def _domain_id_for_url(self, url: str) -> int | None:
        host = urlsplit(url.strip()).hostname
        if not host:
            return None
        return self.session.scalar(
            select(Domain.id).where(Domain.domain.in_({host, host.removeprefix("www.")})).limit(1)
        )
