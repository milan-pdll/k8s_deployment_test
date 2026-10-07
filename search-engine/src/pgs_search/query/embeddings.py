"""Query embeddings for dense retrieval.

The same model AND revision the ETL embeds documents with (ETL/spark/embeddings.py,
EMBEDDING_MODEL_NAME / EMBEDDING_MODEL_REVISION): LaBSE, 768-d, L2-normalized. A query
vector from any other model is not comparable with the stored page_embeddings, which is
why the dimension is checked when the model loads.
"""

from __future__ import annotations

import logging
import threading
from functools import lru_cache
from typing import Any

from pgs_search.config import Settings, settings

logger = logging.getLogger(__name__)


class QueryEmbedder:
    def __init__(self, config: Settings = settings, model: Any = None) -> None:
        self.config = config
        self._model = model
        # Hugging Face fast tokenizers must not be used from two threads at once, and
        # the gRPC server runs several requests in parallel.
        self._lock = threading.Lock()

    @property
    def model_name(self) -> str:
        return self.config.embedding_model_name

    def load(self) -> Any:
        with self._lock:
            return self._load_locked()

    def _load_locked(self) -> Any:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            logger.info(
                "loading query embedding model %s (revision %s)",
                self.config.embedding_model_name,
                self.config.embedding_model_revision or "unpinned",
            )
            model = SentenceTransformer(
                self.config.embedding_model_name,
                revision=self.config.embedding_model_revision,
                device="cpu",
            )
            get_dimension = getattr(model, "get_embedding_dimension", None) or (
                model.get_sentence_embedding_dimension
            )
            dimensions = get_dimension()
            if dimensions != self.config.embedding_dimensions:
                raise RuntimeError(
                    f"{self.config.embedding_model_name} produces {dimensions}-d vectors; "
                    f"EMBEDDING_DIMENSIONS is {self.config.embedding_dimensions}"
                )
            self._model = model
        return self._model

    def embed(self, text: str) -> list[float]:
        if not text.strip():
            raise ValueError("cannot embed an empty query")
        with self._lock:
            model = self._load_locked()
            vector = model.encode(text, normalize_embeddings=True, convert_to_numpy=True)
        return [float(value) for value in vector.tolist()]


@lru_cache(maxsize=1)
def get_query_embedder() -> QueryEmbedder:
    return QueryEmbedder()
