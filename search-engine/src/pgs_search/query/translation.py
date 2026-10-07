"""English <-> Nepali query translation: NLLB-200 (pinned revision) or a remote chat API.

Used by query expansion (normalizer.expand_query_terms) to add the other language's
wording of a single-language query to the lexical (BM25) search; dense retrieval does
not need it (LaBSE is cross-lingual). Bounded: queries longer than
TRANSLATION_MAX_QUERY_CHARS are not translated, output is capped at MAX_NEW_TOKENS, and
results are cached. Calls are serialized (the shared tokenizer's src_lang is mutable
state, and fast tokenizers are not thread-safe).
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.request
from functools import lru_cache
from typing import Any

from pgs_search.config import settings

logger = logging.getLogger(__name__)

NEPALI_LANG_CODE = "npi_Deva"
ENGLISH_LANG_CODE = "eng_Latn"
MAX_NEW_TOKENS = 64

_LANGUAGE_NAMES = {ENGLISH_LANG_CODE: "English", NEPALI_LANG_CODE: "Nepali (Devanagari script)"}

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
    if settings.translation_quantize:
        model = _quantize(model)
    return model, tokenizer


def _quantize(model: Any) -> Any:
    """int8 dynamic quantization of the Linear layers: ~2-3x faster on CPU, ~half the RAM.

    Slightly lower translation quality; falls back to the full model if the platform's
    torch build has no quantized engine.
    """
    import torch

    try:
        quantized = torch.quantization.quantize_dynamic(model, {torch.nn.Linear}, dtype=torch.qint8)
    except Exception:  # noqa: BLE001 -- unsupported engine/arch: keep serving fp32
        logger.warning("int8 quantization unavailable, using the full model", exc_info=True)
        return model
    logger.info("translation model quantized to int8")
    return quantized


def _translate_remote(text: str, source_lang_code: str, target_lang_code: str) -> str:
    """Translate through an OpenAI-compatible chat API; raises on any failure."""
    if not settings.translation_api_key:
        raise RuntimeError("TRANSLATION_API_KEY is not set")
    body = {
        "model": settings.translation_api_model,
        "temperature": 0,
        "max_tokens": MAX_NEW_TOKENS * 2,
        "messages": [
            {
                "role": "system",
                "content": (
                    f"Translate the user's search query from {_LANGUAGE_NAMES[source_lang_code]} "
                    f"to {_LANGUAGE_NAMES[target_lang_code]}. Keep proper nouns. "
                    "Reply with the translation only."
                ),
            },
            {"role": "user", "content": text},
        ],
    }
    request = urllib.request.Request(
        settings.translation_api_base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {settings.translation_api_key}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=settings.translation_api_timeout_seconds) as r:
        payload = json.load(r)
    return str(payload["choices"][0]["message"]["content"]).strip().strip("\"'")


@lru_cache(maxsize=4096)
def _translate(text: str, source_lang_code: str, target_lang_code: str) -> str:
    if settings.translation_backend == "remote":
        return _translate_remote(text, source_lang_code, target_lang_code)
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
