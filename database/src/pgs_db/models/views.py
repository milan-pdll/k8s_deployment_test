"""Read-only views over Silver, for the search team and the API.

Views, not tables: they are always 1:1 with the rows they read, so nothing has to
be refreshed or kept in sync. The SQL lives in the migrations (`b7c1d2e3f4a5`
creates them, `d4e5f6a7b8c9` adds the ranking columns); these `Table` objects only
describe the columns so repositories can query them. They sit on their own `MetaData`,
so Alembic autogenerate never
tries to create them as tables.

- `page_geo_codes`: every geo tag with its full code chain. A tag resolved only to
  a local body still gets its district and province, so filtering "pages in D38"
  finds pages tagged with any municipality in Kaski.
- `search_documents`: one row per canonical page (duplicates excluded), flat, with
  the primary location, domain and file details. The shape the search team's
  `SELECT ... FROM documents` queries expect, with our column names.
"""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)
from sqlalchemy.dialects.postgresql import ARRAY

view_metadata = MetaData()

page_geo_codes = Table(
    "page_geo_codes",
    view_metadata,
    Column("tag_id", BigInteger, primary_key=True),
    Column("page_id", BigInteger),
    Column("province_code", String(4)),
    Column("district_code", String(4)),
    Column("local_body_code", String(16)),
    Column("ward_number", Integer),
    Column("method", String(32)),
    Column("confidence", Float),
)

search_documents = Table(
    "search_documents",
    view_metadata,
    Column("page_id", BigInteger, primary_key=True),
    Column("url", Text),
    Column("domain_id", BigInteger),
    Column("domain", String(255)),
    Column("title", Text),
    Column("description", Text),
    Column("language", String(32)),
    Column("content_type", String(32)),
    Column("category", String(64)),
    Column("author", String(255)),
    Column("published_at", DateTime(timezone=True)),
    Column("keywords", ARRAY(Text)),
    Column("word_count", Integer),
    # Primary location: the most confident tag, most specific first on a tie.
    Column("province_code", String(4)),
    Column("district_code", String(4)),
    Column("local_body_code", String(16)),
    Column("ward_number", Integer),
    # Set only for pages built from a stored file (PDF, DOCX, image, ...).
    Column("file_mime_type", String(255)),
    Column("file_size_bytes", BigInteger),
    Column("file_extension", String(16)),
    # pages.processing_status: UNPROCESSED until the search indexer has taken it.
    Column("index_status", String(32)),
    Column("version", Integer),
    Column("content_hash", String(64)),
    Column("updated_at", DateTime(timezone=True)),
    # Ranking signals (page_scores, domain_stats); NULL until the first score refresh.
    Column("pagerank", Float),
    Column("inbound_links", Integer),
    Column("freshness_score", Float),
    Column("quality_score", Float),
    Column("static_rank", Float),
    Column("domain_authority", Float),
)
