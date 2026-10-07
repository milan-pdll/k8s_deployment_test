"""Database roles: one per service, each with only the privileges it needs.

Role -- used by -- can write:

- `pgs_scraper` -- the Go scraper, if it writes Postgres directly -- Bronze
  (`crawl_runs`, `crawled_documents`, `stored_files`), `domains`.
- `pgs_etl` -- the ETL worker (Spark driver) and the Airflow DAGs -- Bronze (the scraper
  writes only S3, so the ETL saves each page's Document), Silver, unknown `domains`,
  reference contact fields.
- `pgs_search` -- the search engine and the search indexer -- `pages` indexing state.
- `pgs_api` -- the FastAPI gateway -- admin tables, domains, quick links, quarantine,
  search log, labels.
- `pgs_jobs` -- `python -m pgs_db.jobs` -- S3 -> Bronze ingest, Gold summaries and
  scores, log retention.
- `pgs_readonly` -- analysts, dashboards -- nothing.

Every service may append to `error_logs`. Only `pgs_api` can read `admin_users`
(password hashes). The roles are created LOGIN without a password: an operator sets
one per environment (`ALTER ROLE pgs_api PASSWORD '...'`), which also keeps each
role's `statement_timeout` in force (role settings apply to the role that logs in).

Roles are server-wide: several databases on one server (production, staging) share
them, and a downgrade revokes this database's grants but never drops a role.

`apply(execute)` is idempotent. The migration that creates the roles calls it, and
every later migration that adds a table must add the table to `PRIVILEGES` and call
it again. `tests/test_hardening.py` fails if a table or view is missing here.
"""

from collections.abc import Callable

ROLES = {
    "pgs_scraper": "30s",
    "pgs_etl": "5min",
    "pgs_search": "5s",
    "pgs_api": "10s",
    "pgs_jobs": "30min",
    "pgs_readonly": "60s",
}

_BRONZE = ("crawl_runs", "crawled_documents", "stored_files")
_INGEST = ("bronze_ingest_state",)
_REFERENCE = ("provinces", "districts", "local_bodies", "domains", "region_links")
_SILVER = (
    "pages",
    "page_geo_tags",
    "page_contacts",
    "page_sources",
    "page_media",
    "entities",
    "page_entities",
    "page_embeddings",
    "quarantined_files",
)
# Registering a model creates an index, so only the schema owner writes it.
_REGISTRIES = ("embedding_models",)
_GOLD_SUMMARIES = ("domain_stats", "geo_content_stats", "page_scores")
_SEARCH_LOG = ("search_queries", "search_clicks", "relevance_judgments")
_VIEWS = ("search_documents", "page_geo_codes")
_OPS = ("error_logs",)
SECRET_TABLES = ("admin_users",)

# Everything a service may read: all tables and views except the secret ones.
READABLE = (
    _BRONZE
    + _INGEST
    + _REFERENCE
    + _REGISTRIES
    + _SILVER
    + _GOLD_SUMMARIES
    + _SEARCH_LOG
    + _VIEWS
    + _OPS
)
ALL_OBJECTS = READABLE + SECRET_TABLES

# role -> {table: privileges beyond SELECT}
PRIVILEGES: dict[str, dict[str, str]] = {
    "pgs_scraper": {
        **dict.fromkeys(_BRONZE, "INSERT, UPDATE"),
        "domains": "INSERT, UPDATE",
        "error_logs": "INSERT",
    },
    "pgs_etl": {
        # save_transformed(bronze_document=...) upserts the crawl run and the page's
        # Bronze row: the scraper writes only S3 (ETL/spark/site_pipeline.py).
        "crawl_runs": "INSERT, UPDATE",
        "crawled_documents": "INSERT, UPDATE",
        "stored_files": "UPDATE",
        **dict.fromkeys(_SILVER, "INSERT, UPDATE, DELETE"),
        "local_bodies": "UPDATE",  # contact backfill
        # Register a crawled host that is not seeded yet; link a domain to its local body.
        "domains": "INSERT, UPDATE",
        "error_logs": "INSERT",
    },
    "pgs_search": {
        "pages": "UPDATE",  # indexing claim / outcome
        "error_logs": "INSERT",
    },
    "pgs_api": {
        "admin_users": "INSERT, UPDATE",
        "domains": "INSERT, UPDATE",
        "region_links": "INSERT, UPDATE, DELETE",
        "local_bodies": "UPDATE",
        "quarantined_files": "UPDATE",
        "search_queries": "INSERT",
        "search_clicks": "INSERT",
        "relevance_judgments": "INSERT, UPDATE, DELETE",
        "error_logs": "INSERT",
    },
    "pgs_jobs": {
        **dict.fromkeys(_GOLD_SUMMARIES, "INSERT, UPDATE, DELETE"),
        "pages": "UPDATE",  # release stale indexing claims
        # The S3 loader (pgs_db.ingest) writes Bronze on the scraper's behalf; UPDATE
        # also releases stale ETL claims.
        "crawl_runs": "INSERT, UPDATE",
        "crawled_documents": "INSERT, UPDATE",
        "stored_files": "UPDATE",
        "bronze_ingest_state": "INSERT, UPDATE, DELETE",
        "domains": "INSERT, UPDATE",  # ingest registers hosts; link_domains_to_local_bodies
        "local_bodies": "UPDATE",  # fill_local_body_contacts
        "search_queries": "DELETE",  # retention
        "search_clicks": "DELETE",
        "error_logs": "INSERT, DELETE",
    },
    "pgs_readonly": {},
}

# Which roles may read the secret tables.
SECRET_READERS = {"pgs_api": SECRET_TABLES}


def _grant(privileges: str, table: str, role: str) -> str:
    # Skipped for tables that do not exist yet, so an older migration can call apply()
    # on a fresh database even after later tables were added to the matrix.
    return (
        f"DO $$ BEGIN IF to_regclass('public.{table}') IS NOT NULL THEN "
        f"EXECUTE 'GRANT {privileges} ON {table} TO {role}'; END IF; END $$"
    )


def statements() -> list[str]:
    """The SQL that brings every role's privileges up to the matrix above."""
    sql: list[str] = []
    for role, timeout in ROLES.items():
        sql.append(
            f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') "
            f"THEN CREATE ROLE {role} LOGIN; END IF; END $$"
        )
        sql.append(f"ALTER ROLE {role} SET statement_timeout = '{timeout}'")
        sql.append(f"ALTER ROLE {role} SET idle_in_transaction_session_timeout = '5min'")
        sql.append(f"GRANT USAGE ON SCHEMA public TO {role}")
        for table in (*READABLE, "alembic_version", *SECRET_READERS.get(role, ())):
            sql.append(_grant("SELECT", table, role))
        for table, privileges in PRIVILEGES[role].items():
            sql.append(_grant(privileges, table, role))
    return sql


def apply(execute: Callable[[str], object]) -> None:
    """Run `statements()` through `execute` (e.g. Alembic's `op.execute`)."""
    for statement in statements():
        execute(statement)


def revoke_all(execute: Callable[[str], object]) -> None:
    """Undo `apply` in this database (the downgrade).

    The roles themselves are left in place: roles belong to the whole server, and
    other databases on it (staging next to production, a test copy) may still grant
    them privileges, so dropping them here would fail or break those databases. An
    operator drops them with DROP ROLE once no database uses them.
    """
    for role in ROLES:
        execute(
            f"DO $$ BEGIN IF EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
            f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {role}; "
            f"REVOKE USAGE ON SCHEMA public FROM {role}; END IF; END $$"
        )
