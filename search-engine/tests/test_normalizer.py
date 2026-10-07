from unittest.mock import patch

import pytest

from pgs_search.query import normalizer
from pgs_search.query.normalizer import (
    detect_language,
    expand_query_terms,
    get_place_map,
    lemmatize,
    lemmatize_nepali_query,
    lemmatize_query,
    normalize_query,
)


def test_normalize_collapses_whitespace():
    assert normalize_query("  Hello   World  ") == "hello world"
    assert normalize_query("kathmandu    valley") == "kathmandu valley"


def test_normalize_collapses_tabs_and_newlines():
    assert normalize_query("Kathmandu\tPokhara") == "kathmandu pokhara"
    assert normalize_query("Kathmandu\nPokhara\nRiver") == "kathmandu pokhara river"


def test_normalize_strips_leading_and_trailing_whitespace():
    assert normalize_query("   kathmandu   ") == "kathmandu"


def test_normalize_lowercases_english():
    assert normalize_query("KATHMANDU") == "kathmandu"
    assert normalize_query("Pokhara Valley") == "pokhara valley"


def test_normalize_leaves_nepali_case_untouched():
    assert normalize_query("काठमाडौं") == "काठमाडौं"
    assert normalize_query("सगरमाथा") == "सगरमाथा"


def test_normalize_empty_query():
    assert normalize_query("") == ""
    assert normalize_query("   ") == ""
    assert normalize_query("\t\n") == ""


def test_normalize_mixed_language_preserves_devanagari():
    assert normalize_query("kathmandu नेपाल") == "kathmandu नेपाल"


def test_detect_language_english():
    assert detect_language("best trekking route") == "en"
    assert detect_language("himalayan trails") == "en"
    assert detect_language("tourist guide pokhara") == "en"


def test_detect_language_nepali():
    assert detect_language("काठमाडौं") == "ne"
    assert detect_language("सगरमाथा") == "ne"


def test_detect_language_romanized_nepali():
    assert detect_language("kathmandu") == "ne"
    assert detect_language("namaste") == "ne"
    assert detect_language("kathmandu pokhara") == "ne"
    assert detect_language("namche bazaar") == "ne"


def test_detect_language_mixed():
    assert detect_language("kathmandu नेपाल") == "mixed"
    assert detect_language("best trek नेपाल") == "mixed"


def test_detect_language_unknown():
    assert detect_language("") == "unknown"
    assert detect_language("привет") == "unknown"
    assert detect_language("你好") == "unknown"


def test_expand_query_terms_adds_nepali_place():
    terms = expand_query_terms("  Kathmandu ")
    assert "kathmandu" in terms
    assert "काठमाडौं" in terms


def test_get_place_map_loads_json():
    place_map = get_place_map()

    assert isinstance(place_map, dict)
    assert place_map
    assert place_map["kathmandu"] == "काठमाडौं"


def test_expand_nepali_place_adds_english():
    terms = expand_query_terms("काठमाडौं")
    assert "काठमाडौं" in terms
    assert "kathmandu" in terms


def test_expand_all_places_have_equivalents():
    place_map = get_place_map()
    for name in place_map:
        terms = expand_query_terms(name)
        assert place_map[name] in terms


def test_expand_query_terms_keeps_original_and_adds_stemmed_variant():
    terms = expand_query_terms("himalayan trails")
    assert terms[0] == "himalayan trails"
    assert len(terms) == 2


def test_expand_mixed_language_query():
    terms = expand_query_terms("kathmandu नेपाल")
    assert "kathmandu नेपाल" in terms
    assert "काठमाडौं" in terms


def test_expand_empty_query():
    assert expand_query_terms("") == []
    assert expand_query_terms("   ") == []


def test_lemmatize_stems_english_words():
    assert lemmatize_query("running") == "run"


def test_lemmatize_preserves_nepali_unchanged():
    assert lemmatize_query("काठमाडौं") == "काठमाडौं"


def test_expand_query_terms_includes_stemmed_variant():
    terms = expand_query_terms("running")
    assert "running" in terms
    assert "run" in terms


def test_lemmatize_nepali_empty_input_returns_empty_list():
    assert lemmatize_nepali_query("") == []
    assert lemmatize_nepali_query("   ") == []


def test_lemmatize_nepali_non_devnagari_input_returns_empty_list():
    assert lemmatize_nepali_query("running") == []
    assert lemmatize_nepali_query("hospital budget") == []


def test_lemmatize_nepali_returns_lemma_tokens():
    lemmas = lemmatize_nepali_query("काठमाडौंमा हामी जान्छौं")
    assert isinstance(lemmas, list)
    assert lemmas
    assert all(isinstance(token, str) for token in lemmas)
    assert "जानु" in lemmas


class _FakeNepaliPipeline:
    def transform(self, docs):
        return [["केटा", "खानु"]]


def test_lemmatize_nepali_uses_cached_joblib_pipeline():
    normalizer.get_nepali_pipeline.cache_clear()
    try:
        with patch("joblib.load", return_value=_FakeNepaliPipeline()) as mock_load:
            result = lemmatize_nepali_query("केटाहरुले खाए।")
            assert result == ["केटा", "खानु"]
            mock_load.assert_called_once_with(normalizer._NEPALI_PIPELINE_FILE)
    finally:
        normalizer.get_nepali_pipeline.cache_clear()


@pytest.mark.slow
def test_lemmatize_nepali_integration_with_real_pipeline():
    lemmas = lemmatize_nepali_query("केटाहरुले पोखरामा रातो स्याउ खाए।")
    assert isinstance(lemmas, list)
    assert lemmas
    assert all(isinstance(token, str) for token in lemmas)
    assert "केटा" in lemmas
    assert "खानु" in lemmas


def test_lemmatize_dispatches_devanagari_to_nepali():
    with patch(
        "pgs_search.query.normalizer.lemmatize_nepali_query",
        return_value=["केटा", "खानु"],
    ) as mock_nepali:
        result = lemmatize("केटाहरुले खाए।")
    assert result == ["केटा", "खानु"]
    mock_nepali.assert_called_once_with("केटाहरुले खाए।")


def test_lemmatize_dispatches_latin_to_porter():
    assert lemmatize("running") == ["run"]
    assert lemmatize("himalayan trails") == ["himalayan trail"]


def test_expand_query_terms_adds_nepali_lemmatized_variant():
    terms = expand_query_terms("काठमाडौंमा हामी जान्छौं")
    assert "काठमाडौंमा हामी जान्छौं" in terms
    assert "काठमाडौं जानु" in terms


def test_expand_query_terms_translates_english_query_to_nepali(stub_translation):
    stub_translation.to_nepali.return_value = "सगरमाथा पर्वत"

    terms = expand_query_terms("Best trekking route")

    stub_translation.to_nepali.assert_called_once_with("Best trekking route")
    stub_translation.to_english.assert_not_called()
    assert "सगरमाथा पर्वत" in terms


def test_expand_query_terms_translates_nepali_query_to_english(stub_translation):
    stub_translation.to_english.return_value = "Himalayan tourism"

    terms = expand_query_terms("हिमालय पर्यटन")

    stub_translation.to_english.assert_called_once_with("हिमालय पर्यटन")
    stub_translation.to_nepali.assert_not_called()
    assert "Himalayan tourism" in terms


def test_expand_query_terms_keeps_existing_terms_alongside_translation(stub_translation):
    stub_translation.to_nepali.return_value = "सगरमाथा पर्वत"

    terms = expand_query_terms("Kathmandu hospital")

    assert "kathmandu hospital" in terms
    assert "काठमाडौं" in terms
    assert "सगरमाथा पर्वत" in terms


def test_expand_query_terms_skips_translation_for_mixed_query(stub_translation):
    terms = expand_query_terms("kathmandu नेपाल")

    stub_translation.to_nepali.assert_not_called()
    stub_translation.to_english.assert_not_called()
    assert terms == ["kathmandu नेपाल", "काठमाडौं"]


def test_expand_query_terms_skips_translation_for_unknown_query(stub_translation):
    terms = expand_query_terms("привет")

    stub_translation.to_nepali.assert_not_called()
    stub_translation.to_english.assert_not_called()
    assert terms == ["привет"]


def test_expand_query_terms_skips_empty_translation_result(stub_translation):
    stub_translation.to_nepali.return_value = ""
    stub_translation.to_english.return_value = ""

    assert expand_query_terms("himalayan trails") == ["himalayan trails", "himalayan trail"]
    assert expand_query_terms("") == []


def test_expand_query_terms_deduplicates_translation_match(stub_translation):
    stub_translation.to_nepali.return_value = "kathmandu hospital"

    terms = expand_query_terms("Kathmandu hospital")

    assert terms.count("kathmandu hospital") == 1


def test_expand_query_terms_survives_nepali_translation_failure(stub_translation, caplog):
    stub_translation.to_nepali.side_effect = RuntimeError("model unavailable")

    with caplog.at_level("WARNING"):
        terms = expand_query_terms("Kathmandu hospital")

    assert terms == ["kathmandu hospital", "kathmandu hospit", "काठमाडौं"]
    assert "Nepali translation failed" in caplog.text


def test_expand_query_terms_survives_english_translation_failure(stub_translation, caplog):
    stub_translation.to_english.side_effect = RuntimeError("model unavailable")

    with caplog.at_level("WARNING"):
        terms = expand_query_terms("हिमालय पर्यटन")

    assert "हिमालय पर्यटन" in terms
    assert "English translation failed" in caplog.text
