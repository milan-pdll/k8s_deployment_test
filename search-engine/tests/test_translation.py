"""Tests for the NLLB-backed translation helpers.

The cached loader is mocked for every test, so nothing here downloads or loads
the real facebook/nllb-200-distilled-600M model.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pgs_search.config import settings
from pgs_search.query import translation
from pgs_search.query.translation import (
    ENGLISH_LANG_CODE,
    NEPALI_LANG_CODE,
    get_model_and_tokenizer,
    translate_to_english,
    translate_to_nepali,
)

MODEL_NAME = settings.translation_model_name

# Real NLLB token ids for the two language codes this module uses.
_LANG_IDS = {"npi_Deva": 256130, "eng_Latn": 256047}


class _FakeTokenizer:
    """Minimal stand-in for the NLLB tokenizer.

    Deliberately a plain class rather than a MagicMock: it only exposes the
    attributes the implementation is allowed to use, so a call to something the
    installed transformers version does not provide (for example
    `lang_code_to_id`) raises AttributeError instead of silently succeeding.
    """

    def __init__(self, decoded: str = "काठमाडौं उपत्यक नेपालको राजधानी हो ।") -> None:
        self.src_lang = None
        self.src_lang_at_tokenize = None
        self.tokenized_text = None
        self.return_tensors = None
        self.queried_tokens = []
        self.decoded = decoded
        self.skip_special_tokens = None

    def __call__(self, text, return_tensors=None, **kwargs):
        self.tokenized_text = text
        self.return_tensors = return_tensors
        self.src_lang_at_tokenize = self.src_lang
        return {"input_ids": "encoded-ids", "attention_mask": "encoded-mask"}

    def convert_tokens_to_ids(self, token):
        self.queried_tokens.append(token)
        return _LANG_IDS[token]

    def batch_decode(self, output, skip_special_tokens=False):
        self.decoded_output = output
        self.skip_special_tokens = skip_special_tokens
        return [self.decoded]


class _FakeModel:
    def __init__(self) -> None:
        self.generate_kwargs = None
        self.eval_called = False

    def eval(self):
        self.eval_called = True
        return self

    def generate(self, **kwargs):
        self.generate_kwargs = kwargs
        return "generated-ids"


@pytest.fixture
def fake_nllb():
    """Patch the cached loader so no real model is downloaded or run."""
    translation._translate.cache_clear()  # results are cached per text
    with patch.object(translation, "get_model_and_tokenizer") as mock_loader:
        tokenizer = _FakeTokenizer()
        model = _FakeModel()
        mock_loader.return_value = (model, tokenizer)
        yield SimpleNamespace(model=model, tokenizer=tokenizer, loader=mock_loader)
    translation._translate.cache_clear()


def test_translate_to_nepali_uses_english_source_language(fake_nllb):
    translate_to_nepali("Kathmandu Valley is the capital of Nepal.")

    assert fake_nllb.tokenizer.src_lang == ENGLISH_LANG_CODE
    assert fake_nllb.tokenizer.src_lang_at_tokenize == ENGLISH_LANG_CODE


def test_translate_to_nepali_forces_nepali_target_token(fake_nllb):
    translate_to_nepali("Kathmandu Valley is the capital of Nepal.")

    assert fake_nllb.tokenizer.queried_tokens == [NEPALI_LANG_CODE]
    assert fake_nllb.model.generate_kwargs["forced_bos_token_id"] == _LANG_IDS["npi_Deva"]


def test_translate_to_english_uses_nepali_source_language(fake_nllb):
    translate_to_english("काठमाडौं उपत्यक नेपालको राजधानी हो।")

    assert fake_nllb.tokenizer.src_lang == NEPALI_LANG_CODE
    assert fake_nllb.tokenizer.src_lang_at_tokenize == NEPALI_LANG_CODE


def test_translate_to_english_forces_english_target_token(fake_nllb):
    translate_to_english("काठमाडौं उपत्यक नेपालको राजधानी हो।")

    assert fake_nllb.tokenizer.queried_tokens == [ENGLISH_LANG_CODE]
    assert fake_nllb.model.generate_kwargs["forced_bos_token_id"] == _LANG_IDS["eng_Latn"]


def test_translate_tokenizes_text_with_tensors(fake_nllb):
    translate_to_nepali("Kathmandu Valley is the capital of Nepal.")

    assert fake_nllb.tokenizer.tokenized_text == "Kathmandu Valley is the capital of Nepal."
    assert fake_nllb.tokenizer.return_tensors == "pt"


def test_translate_passes_encoded_inputs_to_generate(fake_nllb):
    translate_to_english("काठमाडौं")

    assert fake_nllb.model.generate_kwargs["input_ids"] == "encoded-ids"
    assert fake_nllb.model.generate_kwargs["attention_mask"] == "encoded-mask"


def test_translate_to_nepali_returns_decoded_text(fake_nllb):
    fake_nllb.tokenizer.decoded = "काठमाडौं उपत्यक नेपालको राजधानी हो ।"

    result = translate_to_nepali("Kathmandu Valley is the capital of Nepal.")

    assert result == "काठमाडौं उपत्यक नेपालको राजधानी हो ।"
    assert fake_nllb.tokenizer.skip_special_tokens is True


def test_translate_to_english_returns_decoded_text(fake_nllb):
    fake_nllb.tokenizer.decoded = "Kathmandu Valley is the capital of Nepal."

    result = translate_to_english("काठमाडौं उपत्यक नेपालको राजधानी हो।")

    assert result == "Kathmandu Valley is the capital of Nepal."
    assert fake_nllb.tokenizer.skip_special_tokens is True


def test_translate_strips_surrounding_whitespace_from_output(fake_nllb):
    fake_nllb.tokenizer.decoded = "  Kathmandu Valley  "

    assert translate_to_english("काठमाडौं") == "Kathmandu Valley"


def test_translation_does_not_rely_on_lang_code_to_id(fake_nllb):
    assert not hasattr(fake_nllb.tokenizer, "lang_code_to_id")

    translate_to_nepali("Namaste")
    translate_to_english("नमस्ते")


def test_get_model_and_tokenizer_loads_nllb_once_and_caches_it():
    assert MODEL_NAME == "facebook/nllb-200-distilled-600M"
    assert get_model_and_tokenizer.cache_info().maxsize == 1

    get_model_and_tokenizer.cache_clear()
    try:
        import transformers

        with (
            patch.object(transformers.AutoTokenizer, "from_pretrained") as mock_tokenizer_load,
            patch.object(transformers.AutoModelForSeq2SeqLM, "from_pretrained") as mock_model_load,
        ):
            mock_model = MagicMock()
            mock_model_load.return_value = mock_model
            mock_tokenizer = MagicMock()
            mock_tokenizer_load.return_value = mock_tokenizer

            first = get_model_and_tokenizer()
            second = get_model_and_tokenizer()

            assert first is second
            assert first == (mock_model, mock_tokenizer)
            revision = settings.translation_model_revision
            mock_tokenizer_load.assert_called_once_with(MODEL_NAME, revision=revision)
            mock_model_load.assert_called_once_with(MODEL_NAME, revision=revision)
            mock_model.eval.assert_called_once_with()
            assert get_model_and_tokenizer.cache_info().hits == 1
    finally:
        get_model_and_tokenizer.cache_clear()


def test_translations_are_cached(fake_nllb):
    translate_to_nepali("Budget notice")
    translate_to_nepali("Budget notice")
    assert fake_nllb.model.generate_kwargs is not None
    assert translation._translate.cache_info().hits == 1


def test_long_or_disabled_translation_is_skipped(fake_nllb, monkeypatch):
    long_query = "x" * (settings.translation_max_query_chars + 1)
    assert translate_to_nepali(long_query) == ""
    disabled = settings.model_copy(update={"translation_enabled": False})
    monkeypatch.setattr(translation, "settings", disabled)
    assert translate_to_nepali("Budget notice") == ""
    assert fake_nllb.model.generate_kwargs is None


def test_generation_is_bounded(fake_nllb):
    translate_to_nepali("Budget notice")
    assert fake_nllb.model.generate_kwargs["max_new_tokens"] == translation.MAX_NEW_TOKENS


class _FakeResponse:
    def __init__(self, content: str) -> None:
        self._body = ('{"choices": [{"message": {"content": %s}}]}' % __import__("json").dumps(content)).encode()

    def read(self, *_a):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def test_remote_backend_calls_chat_api_and_skips_local_model(monkeypatch):
    monkeypatch.setattr(settings, "translation_backend", "remote")
    monkeypatch.setattr(settings, "translation_api_key", "k")
    translation._translate.cache_clear()
    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["auth"] = request.get_header("Authorization")
        seen["body"] = __import__("json").loads(request.data)
        return _FakeResponse('"काठमाडौं"\n'.strip())

    with (
        patch.object(translation.urllib.request, "urlopen", fake_urlopen),
        patch.object(translation, "get_model_and_tokenizer") as load,
    ):
        assert translate_to_nepali("Kathmandu") == "काठमाडौं"
    load.assert_not_called()
    assert seen["url"].endswith("/chat/completions")
    assert seen["auth"] == "Bearer k"
    assert seen["body"]["messages"][1]["content"] == "Kathmandu"
    translation._translate.cache_clear()


def test_remote_backend_without_key_raises(monkeypatch):
    monkeypatch.setattr(settings, "translation_backend", "remote")
    monkeypatch.setattr(settings, "translation_api_key", None)
    translation._translate.cache_clear()
    with pytest.raises(RuntimeError):
        translate_to_english("काठमाडौं")
    translation._translate.cache_clear()
