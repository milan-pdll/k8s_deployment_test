from __future__ import annotations

import numpy as np
import pytest

from pgs_search.config import settings
from pgs_search.query.embeddings import QueryEmbedder


class FakeModel:
    def __init__(self, dimensions: int = 768) -> None:
        self.dimensions = dimensions
        self.calls: list[dict] = []

    def get_embedding_dimension(self) -> int:
        return self.dimensions

    def encode(self, text, **kwargs):
        self.calls.append({"text": text, **kwargs})
        vector = np.zeros(self.dimensions)
        vector[0] = 1.0
        return vector


def test_query_vectors_are_normalized_floats_of_the_document_model() -> None:
    model = FakeModel()
    vector = QueryEmbedder(settings, model).embed("pokhara budget")
    assert len(vector) == settings.embedding_dimensions
    assert all(isinstance(value, float) for value in vector)
    assert model.calls[0]["normalize_embeddings"] is True


def test_the_query_model_defaults_to_the_etl_document_model() -> None:
    # The ETL embeds documents with this name (ETL/spark/embeddings.py); a different
    # query model would make every dense score meaningless.
    assert settings.embedding_model_name == "sentence-transformers/LaBSE"
    assert settings.embedding_dimensions == 768


def test_empty_query_is_rejected() -> None:
    with pytest.raises(ValueError):
        QueryEmbedder(settings, FakeModel()).embed("   ")


def test_a_model_of_another_dimension_is_refused(monkeypatch) -> None:
    import sys
    from types import ModuleType

    fake_module = ModuleType("sentence_transformers")
    fake_module.SentenceTransformer = lambda *args, **kwargs: FakeModel(384)
    monkeypatch.setitem(sys.modules, "sentence_transformers", fake_module)
    with pytest.raises(RuntimeError, match="384-d"):
        QueryEmbedder(settings).load()
