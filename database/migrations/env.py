from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from pgs_db import (
    Base,
    get_database_url,
    models,  # noqa: F401  (registers all tables)
)

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

config.set_main_option("sqlalchemy.url", get_database_url())
target_metadata = Base.metadata


def include_object(
    obj: object, name: str | None, type_: str, reflected: bool, compare_to: object
) -> bool:
    """Keep autogenerate to the tables this project actually owns.

    The postgis/postgis image installs ~37 PostGIS and TIGER geocoder tables
    (spatial_ref_sys, topology, tract, zip_lookup, ...) into the same database.
    Alembic sees them in the database but not in our metadata, so without this
    filter `alembic revision --autogenerate` emits DROP TABLE for every one of
    them -- which would tear out PostGIS the first time anyone adds a column.
    """
    if type_ == "table" and reflected and name not in target_metadata.tables:
        return False
    # One HNSW index per embedding model, created by migrations and
    # SilverRepository.register_embedding_model rather than declared on the model.
    per_model_hnsw = (name or "").startswith("ix_page_embeddings_hnsw_")
    return not (type_ == "index" and reflected and per_model_hnsw)


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
