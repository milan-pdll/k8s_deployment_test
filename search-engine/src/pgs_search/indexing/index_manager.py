"""Create the versioned search index and point the alias at it (idempotent).

Run on every start of the search engine and the indexer (both race safely):

    python -m pgs_search.indexing.index_manager

States it handles:
- nothing exists: create `<alias>_v<N>` and the alias;
- the alias already points at `<alias>_v<N>`: nothing to do;
- an EMPTY concrete index named like the alias (the layout before aliases): it is
  replaced; one with documents is never deleted -- the operator reindexes it;
- the alias points at an older version: refused, with the reindex steps (a mapping
  change is a deliberate migration, not a side effect of a restart).
"""

from __future__ import annotations

import logging
from typing import Any

from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError

from pgs_search.config import Settings, settings
from pgs_search.indexing.mappings import INDEX_VERSION, index_body

logger = logging.getLogger(__name__)


class IndexMigrationRequired(RuntimeError):
    """The index in OpenSearch is not the version this code needs; a reindex is due."""


def concrete_index_name(alias: str, version: int = INDEX_VERSION) -> str:
    return f"{alias}_v{version}"


def ensure_index(client: OpenSearch, config: Settings = settings) -> str:
    """Make sure the alias points at the current index version; return that index."""
    alias = config.opensearch_index
    concrete = concrete_index_name(alias)

    if client.indices.exists_alias(name=alias):
        targets = set(client.indices.get_alias(name=alias))
        if concrete in targets:
            return concrete
        raise IndexMigrationRequired(
            f"alias {alias} points at {sorted(targets)}, this code needs {concrete}: create "
            f"{concrete}, reindex into it (POST _reindex), then move the alias"
        )

    if client.indices.exists(index=alias):
        count = int(client.count(index=alias)["count"])
        if count:
            raise IndexMigrationRequired(
                f"{alias} is a concrete index with {count} document(s): reindex it into "
                f"{concrete} and replace it with an alias"
            )
        logger.warning("replacing the empty pre-alias index %s with %s", alias, concrete)
        client.indices.delete(index=alias)

    _create(client, concrete, index_body(config.opensearch_replicas))
    client.indices.update_aliases(
        body={"actions": [{"add": {"index": concrete, "alias": alias, "is_write_index": True}}]}
    )
    logger.info("index %s ready behind alias %s", concrete, alias)
    return concrete


def _create(client: OpenSearch, index: str, body: dict[str, Any]) -> None:
    if client.indices.exists(index=index):
        return
    try:
        client.indices.create(index=index, body=body)
    except RequestError as exc:
        # Another process (search engine vs indexer) created it first.
        if exc.error != "resource_already_exists_exception":
            raise


def main() -> None:
    from pgs_search.client.opensearch import get_opensearch_client

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ensure_index(get_opensearch_client())


if __name__ == "__main__":
    main()
