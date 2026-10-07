"""the ETL writes Bronze: grant pgs_etl INSERT on crawl_runs, crawled_documents, domains

The scraper writes only S3; ETL/spark/site_pipeline.py saves each page's scraper
Document to Bronze in the same transaction as its Silver page
(pgs_db.etl.save_transformed(..., bronze_document=...)), and registers hosts that are
not seeded yet. Privileges only: no table changes.

Revision ID: b8c9d0e1f2a3
Revises: a7b8c9d0e1f2
Create Date: 2026-10-07 01:00:00.000000

"""
from typing import Sequence, Union

from alembic import op

from pgs_db import grants


# revision identifiers, used by Alembic.
revision: str = 'b8c9d0e1f2a3'
down_revision: Union[str, Sequence[str], None] = 'a7b8c9d0e1f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Bring every role up to the privilege matrix in pgs_db.grants."""
    grants.apply(op.execute)


def downgrade() -> None:
    """Take back what this revision added (the matrix of a7b8c9d0e1f2)."""
    for table in ("crawl_runs", "crawled_documents", "domains"):
        op.execute(f"REVOKE INSERT ON {table} FROM pgs_etl")
    op.execute("REVOKE UPDATE ON crawl_runs FROM pgs_etl")
