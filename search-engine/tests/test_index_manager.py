from __future__ import annotations

from typing import Any

import pytest

from pgs_search.config import settings
from pgs_search.indexing.index_manager import (
    IndexMigrationRequired,
    concrete_index_name,
    ensure_index,
)
from pgs_search.indexing.mappings import INDEX_VERSION

ALIAS = settings.opensearch_index
CURRENT = concrete_index_name(ALIAS)


class FakeIndices:
    def __init__(
        self, indices: dict[str, int] | None = None, aliases: dict[str, str] | None = None
    ):
        self.indices = dict(indices or {})  # name -> document count
        self.aliases = dict(aliases or {})  # alias -> index
        self.created: list[tuple[str, dict[str, Any]]] = []
        self.deleted: list[str] = []

    def exists_alias(self, name: str) -> bool:
        return name in self.aliases

    def get_alias(self, name: str) -> dict[str, Any]:
        return {self.aliases[name]: {"aliases": {name: {}}}}

    def exists(self, index: str) -> bool:
        return index in self.indices

    def create(self, index: str, body: dict[str, Any]) -> None:
        self.indices[index] = 0
        self.created.append((index, body))

    def delete(self, index: str) -> None:
        del self.indices[index]
        self.deleted.append(index)

    def update_aliases(self, body: dict[str, Any]) -> None:
        for action in body["actions"]:
            add = action["add"]
            self.aliases[add["alias"]] = add["index"]


class FakeClient:
    def __init__(self, indices: FakeIndices) -> None:
        self.indices = indices

    def count(self, index: str) -> dict[str, int]:
        return {"count": self.indices.indices[index]}


def test_creates_the_versioned_index_behind_the_alias() -> None:
    indices = FakeIndices()
    assert ensure_index(FakeClient(indices)) == CURRENT
    assert CURRENT == f"{ALIAS}_v{INDEX_VERSION}"
    assert indices.aliases == {ALIAS: CURRENT}
    [(_name, body)] = indices.created
    assert body["mappings"]["dynamic"] == "strict"


def test_is_idempotent() -> None:
    indices = FakeIndices({CURRENT: 10}, {ALIAS: CURRENT})
    assert ensure_index(FakeClient(indices)) == CURRENT
    assert indices.created == [] and indices.deleted == []


def test_replaces_an_empty_pre_alias_index() -> None:
    indices = FakeIndices({ALIAS: 0})
    ensure_index(FakeClient(indices))
    assert indices.deleted == [ALIAS]
    assert indices.aliases == {ALIAS: CURRENT}


def test_never_deletes_an_index_with_documents() -> None:
    indices = FakeIndices({ALIAS: 5})
    with pytest.raises(IndexMigrationRequired):
        ensure_index(FakeClient(indices))
    assert indices.deleted == []


def test_an_alias_on_an_older_version_needs_a_migration() -> None:
    indices = FakeIndices({f"{ALIAS}_v0": 3}, {ALIAS: f"{ALIAS}_v0"})
    with pytest.raises(IndexMigrationRequired):
        ensure_index(FakeClient(indices))
