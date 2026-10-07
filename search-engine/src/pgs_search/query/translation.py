"""English <-> Nepali query translation with NLLB-200 (pinned revision).

Used by query expansion (normalizer.expand_query_terms) to add the other language's
wording of a single-language query to the lexical (BM25) search; dense retrieval does
not need it (LaBSE is cross-lingual). Bounded: queries longer than
TRANSLATION_MAX_QUERY_CHARS are not translated, output is capped at MAX_NEW_TOKENS, and
results are cached. Calls are serialized (the shared tokenizer's src_lang is mutable
state, and fast tokenizers are not thread-safe).
"""

from __future__ import annotations

import logging
import threading
from functools import lru_cache
from typing import Any

from pgs_search.config import settings

logger = logging.getLogger(__name__)

NEPALI_LANG_CODE = "npi_Deva"
ENGLISH_LANG_CODE = "eng_Latn"
MAX_NEW_TOKENS = 64

_lock = threading.Lock()


@lru_cache(maxsize=1)
def get_model_and_tokenizer() -> tuple[Any, Any]:
    """Load the NLLB model and tokenizer once and cache them."""
    from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

    name, revision = settings.translation_model_name, settings.translation_model_revision
    logger.info("loading translation model %s (revision %s)", name, revision or "unpinned")
    tokenizer = AutoTokenizer.from_pretrained(name, revision=revision)
    model = AutoModelForSeq2SeqLM.from_pretrained(name, revision=revision)
    model.eval()
    return model, tokenizer


@lru_cache(maxsize=4096)
def _translate(text: str, source_lang_code: str, target_lang_code: str) -> str:
    import torch

    with _lock:
        model, tokenizer = get_model_and_tokenizer()
        tokenizer.src_lang = source_lang_code
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=MAX_NEW_TOKENS)
        forced_bos_token_id = tokenizer.convert_tokens_to_ids(target_lang_code)
        with torch.inference_mode():
            output = model.generate(
                **inputs,
                forced_bos_token_id=forced_bos_token_id,
                max_new_tokens=MAX_NEW_TOKENS,
            )
        return tokenizer.batch_decode(output, skip_special_tokens=True)[0].strip()


def _bounded(text: str) -> bool:
    return settings.translation_enabled and 0 < len(text) <= settings.translation_max_query_chars


def translate_to_nepali(text: str) -> str:
    """English -> Nepali (Devanagari); "" when translation is disabled or not applicable."""
    return _translate(text, ENGLISH_LANG_CODE, NEPALI_LANG_CODE) if _bounded(text) else ""


def translate_to_english(text: str) -> str:
    """Nepali (Devanagari) -> English; "" when translation is disabled or not applicable."""
    return _translate(text, NEPALI_LANG_CODE, ENGLISH_LANG_CODE) if _bounded(text) else ""
