"""LightGBM reranking of the fused candidates.

The model is loaded from LightGBM's native text format (models/lightgbm_reranker.txt,
written by scripts/train_reranker.py) with `lightgbm.Booster` -- no pickle is
deserialized at serving time. It only reorders the bounded candidate list the pipeline
gives it; any failure leaves the fusion order in place (the pipeline reports the
response as degraded).
"""

from __future__ import annotations

import logging
import math
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

FEATURES = [
    "bm25_score",
    "vector_score",
    "title_match",
    "geo_match",
    "freshness",
    "source_authority",
    "language_match",
    "query_term_ratio",
    "content_length",
]

SEARCH_ENGINE_ROOT = Path(__file__).resolve().parents[3]
MODEL_PATH = SEARCH_ENGINE_ROOT / "models" / "lightgbm_reranker.txt"


@lru_cache(maxsize=1)
def get_model() -> Any:
    """Load and cache the trained LightGBM booster."""
    import lightgbm as lgb

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"LightGBM reranker model is missing at {MODEL_PATH}; run "
            "`PYTHONPATH=search-engine/src python search-engine/scripts/train_reranker.py`."
        )
    logger.info("loading LightGBM reranker from %s", MODEL_PATH)
    booster = lgb.Booster(model_file=str(MODEL_PATH))
    if booster.feature_name() != FEATURES:
        raise ValueError(f"reranker features {booster.feature_name()} != {FEATURES}")
    return booster


def _feature_value(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def rerank_results(results: list[dict[str, Any]], top_k: int | None = None) -> list[dict[str, Any]]:
    """`results` reordered by the model's score (stable for ties), with `rerank_score`
    set on each; at most `top_k` of them."""
    if not isinstance(results, list):
        raise TypeError("results must be provided as a list.")
    if not results:
        return []
    if top_k is not None and (isinstance(top_k, bool) or top_k <= 0):
        raise ValueError("top_k must be a positive integer.")

    import numpy as np

    matrix = np.array(
        [[_feature_value(result.get(feature)) for feature in FEATURES] for result in results],
        dtype=float,
    )
    scores = get_model().predict(matrix)
    ranked = []
    for result, score in zip(results, scores, strict=True):
        ranked.append({**result, "rerank_score": _feature_value(score)})
    # sorted() is stable: equal scores keep the fusion order.
    ranked.sort(key=lambda result: result["rerank_score"], reverse=True)
    return ranked[:top_k] if top_k is not None else ranked
