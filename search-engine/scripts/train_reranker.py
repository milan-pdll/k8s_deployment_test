"""Train the LightGBM search-result reranker."""

from __future__ import annotations

import logging
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import ndcg_score
from sklearn.model_selection import train_test_split

from pgs_search.ranking.lightgbm_reranker import FEATURES

logger = logging.getLogger(__name__)

SEARCH_ENGINE_ROOT = Path(__file__).resolve().parents[1]
TRAINING_DATA_PATH = SEARCH_ENGINE_ROOT / "training_data" / "ranking_training_data.csv"
# LightGBM's native text format: the server loads it with lightgbm.Booster, no pickle.
MODEL_PATH = SEARCH_ENGINE_ROOT / "models" / "lightgbm_reranker.txt"
FEATURE_IMPORTANCE_PATH = SEARCH_ENGINE_ROOT / "training_data" / "feature_importance.csv"
RERANKED_RESULTS_PATH = SEARCH_ENGINE_ROOT / "training_data" / "reranked_results.csv"

TARGET = "label"
QUERY_COLUMN = "query"


def _validate_training_data(df: pd.DataFrame) -> None:
    """Validate the columns and shape required for LambdaRank training."""
    required_columns = [*FEATURES, TARGET, QUERY_COLUMN]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"Training data is missing required column(s): {missing_columns}")

    if df.empty:
        raise ValueError("Training data is empty.")

    query_count = df[QUERY_COLUMN].nunique(dropna=True)
    if query_count < 2:
        raise ValueError("Training data must contain at least two distinct queries.")

    if df[QUERY_COLUMN].isna().any():
        raise ValueError("Training data contains rows without a query value.")


def _load_training_data(path: Path = TRAINING_DATA_PATH) -> pd.DataFrame:
    """Load, validate, and normalize LightGBM ranking training data."""
    if not path.exists():
        raise FileNotFoundError(
            f"Training data not found at {path}. Expected Rashik's data copied to "
            "search-engine/training_data/ranking_training_data.csv."
        )

    df = pd.read_csv(path)
    _validate_training_data(df)
    df[FEATURES] = df[FEATURES].apply(pd.to_numeric, errors="coerce")
    df[FEATURES] = df[FEATURES].replace([float("inf"), float("-inf")], pd.NA).fillna(0)
    df[TARGET] = pd.to_numeric(df[TARGET], errors="coerce").fillna(0)
    return df


def _query_groups(df: pd.DataFrame) -> list[int]:
    """Return LightGBM group sizes for a query-sorted data frame."""
    return df.groupby(QUERY_COLUMN, sort=False).size().tolist()


def train() -> lgb.LGBMRanker:
    """Train the reranker and write model, feature-importance, and test-output artifacts."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    df = _load_training_data()
    logger.info("Dataset shape: %s", df.shape)

    queries = df[QUERY_COLUMN].dropna().unique()
    train_queries, test_queries = train_test_split(queries, test_size=0.25, random_state=42)

    train_df = df[df[QUERY_COLUMN].isin(train_queries)].sort_values(QUERY_COLUMN).copy()
    test_df = df[df[QUERY_COLUMN].isin(test_queries)].sort_values(QUERY_COLUMN).copy()

    train_groups = _query_groups(train_df)
    test_groups = _query_groups(test_df)

    logger.info("Training rows: %d", len(train_df))
    logger.info("Testing rows: %d", len(test_df))
    logger.info("Training groups: %s", train_groups)
    logger.info("Testing groups: %s", test_groups)

    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        ndcg_at=[5, 10],
        learning_rate=0.05,
        n_estimators=300,
        num_leaves=31,
        random_state=42,
        verbosity=-1,
    )

    x_train = train_df[FEATURES]
    y_train = train_df[TARGET]
    x_test = test_df[FEATURES]
    y_test = test_df[TARGET]

    logger.info("Training LightGBM reranker...")
    model.fit(
        x_train,
        y_train,
        group=train_groups,
        eval_set=[(x_test, y_test)],
        eval_group=[test_groups],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=True)],
    )
    logger.info("Training completed.")

    test_df["rerank_score"] = model.predict(x_test)
    scores = []
    for _, group in test_df.groupby(QUERY_COLUMN):
        if len(group) < 2:
            continue

        scores.append(
            ndcg_score(
                [group[TARGET].to_numpy()],
                [group["rerank_score"].to_numpy()],
                k=min(5, len(group)),
            )
        )

    if scores:
        logger.info("Average NDCG@5: %.4f", float(np.mean(scores)))
        logger.info("Number of queries evaluated: %d", len(scores))

    importance = pd.DataFrame(
        {"feature": FEATURES, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    FEATURE_IMPORTANCE_PATH.parent.mkdir(parents=True, exist_ok=True)

    importance.to_csv(FEATURE_IMPORTANCE_PATH, index=False)
    test_df.sort_values([QUERY_COLUMN, "rerank_score"], ascending=[True, False]).to_csv(
        RERANKED_RESULTS_PATH,
        index=False,
    )
    model.booster_.save_model(str(MODEL_PATH))

    logger.info("Model saved to %s", MODEL_PATH)
    logger.info("Feature importance saved to %s", FEATURE_IMPORTANCE_PATH)
    logger.info("Reranked results saved to %s", RERANKED_RESULTS_PATH)
    return model


if __name__ == "__main__":
    train()
