from __future__ import annotations

import pytest

from pgs_search.ranking import lightgbm_reranker
from pgs_search.ranking.fusion import reciprocal_rank_fusion
from pgs_search.ranking.lightgbm_reranker import FEATURES, get_model, rerank_results


def candidate(document_id: str, **features: float) -> dict:
    return {"document_id": document_id, **dict.fromkeys(FEATURES, 0.0), **features}


def test_the_shipped_model_loads_without_pickle_and_has_the_feature_contract() -> None:
    assert lightgbm_reranker.MODEL_PATH.suffix == ".txt"
    booster = get_model()
    assert booster.feature_name() == FEATURES


def test_rerank_sets_scores_sorts_and_caps() -> None:
    results = [candidate("a"), candidate("b", bm25_score=30.0, title_match=1), candidate("c")]
    ranked = rerank_results(results, top_k=2)
    assert len(ranked) == 2
    assert all("rerank_score" in result for result in ranked)
    assert ranked[0]["rerank_score"] >= ranked[1]["rerank_score"]


def test_ties_keep_the_incoming_order() -> None:
    results = [candidate(str(i)) for i in range(5)]  # identical features
    ranked = rerank_results(results)
    assert [result["document_id"] for result in ranked] == ["0", "1", "2", "3", "4"]


def test_invalid_feature_values_count_as_zero() -> None:
    weird = candidate("x", bm25_score=float("nan"), content_length=float("inf"))
    weird["vector_score"] = "not a number"
    plain = candidate("y")
    assert rerank_results([weird])[0]["rerank_score"] == rerank_results([plain])[0]["rerank_score"]


def test_input_validation() -> None:
    assert rerank_results([]) == []
    with pytest.raises(TypeError):
        rerank_results("nope")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        rerank_results([candidate("a")], top_k=0)


def test_rrf_fuses_by_rank_and_counts_a_document_once_per_list() -> None:
    bm25 = [{"document_id": "a"}, {"document_id": "b"}, {"document_id": "a"}]
    dense = [{"document_id": "b"}, {"document_id": "c"}]
    fused = dict(reciprocal_rank_fusion([bm25, dense], k=60))
    assert fused["b"] == pytest.approx(1 / 62 + 1 / 61)
    assert fused["a"] == pytest.approx(1 / 61)
    assert fused["c"] == pytest.approx(1 / 62)
    assert next(doc for doc, _ in reciprocal_rank_fusion([bm25, dense])) == "b"
