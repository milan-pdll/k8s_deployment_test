"""LaBSE document embeddings, computed on the ETL driver.

One model instance per ETL worker process, loaded on first use and pinned to an exact
Hugging Face revision (EMBEDDING_MODEL_REVISION): the search engine embeds queries with
the same name and revision, so documents and queries live in one vector space, and a
new upstream commit can never change the vectors between runs.

Long pages are split into windows of the model's maximum sequence length; the
document vector is the L2-normalized mean of its window vectors (at most
EMBEDDING_MAX_CHUNKS windows, i.e. the first few thousand tokens), which bounds the
CPU spent on one page.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmbeddingConfig:
    model_name: str
    revision: str | None
    dimensions: int
    batch_size: int
    max_chunks: int

    @classmethod
    def from_env(cls) -> EmbeddingConfig:
        config = cls(
            model_name=os.environ.get("EMBEDDING_MODEL_NAME", "sentence-transformers/LaBSE"),
            revision=os.environ.get("EMBEDDING_MODEL_REVISION") or None,
            dimensions=int(os.environ.get("EMBEDDING_DIMENSIONS", "768")),
            batch_size=int(os.environ.get("EMBEDDING_BATCH_SIZE", "32")),
            max_chunks=int(os.environ.get("EMBEDDING_MAX_CHUNKS", "16")),
        )
        if min(config.batch_size, config.max_chunks, config.dimensions) < 1:
            raise ValueError(
                "EMBEDDING_BATCH_SIZE, EMBEDDING_MAX_CHUNKS and EMBEDDING_DIMENSIONS must be >= 1"
            )
        return config


class Embedder:
    """Thread-safe wrapper around one SentenceTransformer.

    Encoding is serialized with a lock: Hugging Face fast tokenizers are not safe to
    call from several threads at once ("Already borrowed"), and the ETL worker runs
    several sites concurrently. Torch already uses every core for one batch.
    """

    def __init__(self, config: EmbeddingConfig, model: Any = None) -> None:
        self.config = config
        self._model = model
        self._lock = threading.Lock()

    @property
    def model_name(self) -> str:
        return self.config.model_name

    def _load(self) -> Any:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            logger.info(
                "loading embedding model %s (revision %s)",
                self.config.model_name,
                self.config.revision or "unpinned",
            )
            model = SentenceTransformer(
                self.config.model_name, revision=self.config.revision, device="cpu"
            )
            # sentence-transformers 6 renamed get_sentence_embedding_dimension.
            get_dimension = getattr(model, "get_embedding_dimension", None) or (
                model.get_sentence_embedding_dimension
            )
            dimensions = get_dimension()
            if dimensions != self.config.dimensions:
                raise RuntimeError(
                    f"{self.config.model_name} produces {dimensions}-d vectors, but "
                    f"EMBEDDING_DIMENSIONS is {self.config.dimensions}"
                )
            self._model = model
        return self._model

    def embed(self, texts: Sequence[str]) -> list[list[float] | None]:
        """One normalized vector per text (None for blank text), in input order."""
        results: list[list[float] | None] = [None] * len(texts)
        if not any(text and text.strip() for text in texts):
            return results
        with self._lock:
            model = self._load()
            chunks, owners = self._chunk(model, texts)
            if not chunks:
                return results
            vectors = model.encode(
                chunks,
                batch_size=self.config.batch_size,
                normalize_embeddings=True,
                convert_to_numpy=True,
                show_progress_bar=False,
            )
        grouped: dict[int, list[Any]] = {}
        for owner, vector in zip(owners, vectors, strict=True):
            grouped.setdefault(owner, []).append(vector)
        for index, document_vectors in grouped.items():
            mean = sum(document_vectors) / len(document_vectors)
            norm = float((mean**2).sum() ** 0.5)
            if norm:
                results[index] = [float(value) for value in mean / norm]
        return results

    def _chunk(self, model: Any, texts: Sequence[str]) -> tuple[list[str], list[int]]:
        tokenizer = model.tokenizer
        window = model.max_seq_length - tokenizer.num_special_tokens_to_add(pair=False)
        if window < 1:
            raise ValueError("the model's max_seq_length leaves no room for text tokens")
        chunks: list[str] = []
        owners: list[int] = []
        for index, text in enumerate(texts):
            if not text or not text.strip():
                continue
            token_ids = tokenizer(text, add_special_tokens=False, truncation=False)["input_ids"]
            limit = window * self.config.max_chunks
            for start in range(0, min(len(token_ids), limit), window):
                window_ids = token_ids[start : start + window]
                chunk = tokenizer.decode(window_ids, skip_special_tokens=True)
                if chunk.strip():
                    chunks.append(chunk)
                    owners.append(index)
        return chunks, owners


_default: Embedder | None = None
_default_lock = threading.Lock()


def default_embedder() -> Embedder:
    """The process-wide embedder, configured from the environment."""
    global _default
    with _default_lock:
        if _default is None:
            _default = Embedder(EmbeddingConfig.from_env())
        return _default
