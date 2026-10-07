"""Reciprocal Rank Fusion of the lexical and dense result lists.

BM25 scores (unbounded, query-dependent) and cosine similarities (-1..1) are not on a
comparable scale, so the lists are fused by rank, not by score:
score(d) = sum over lists of 1 / (k + rank of d in that list).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

RRF_K = 60


def reciprocal_rank_fusion(
    result_lists: Sequence[Sequence[dict[str, Any]]], k: int = RRF_K
) -> list[tuple[str, float]]:
    """(document_id, fusion score) pairs, best first; ties keep first-seen order."""
    scores: dict[str, float] = {}
    for results in result_lists:
        seen: set[str] = set()
        for rank, result in enumerate(results, start=1):
            document_id = str(result["document_id"])
            if document_id in seen:  # count a document once per list
                continue
            seen.add(document_id)
            scores[document_id] = scores.get(document_id, 0.0) + 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda item: item[1], reverse=True)
