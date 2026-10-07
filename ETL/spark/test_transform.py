import unicodedata
import unittest
from typing import ClassVar

from site_event import validate_event
from transform import (
    detect_language,
    extract_html_text,
    normalize_text,
    sha256_text,
    simhash_text,
    transform_document,
)

NEPALI = "काठमाडौं महानगरपालिका"


class ExtractionTests(unittest.TestCase):
    def test_extracts_html_text_without_script_style_or_title(self):
        html = """
        <html><head><title>Ignored title</title><script>ignoreMe()</script>
        <style>p { color: red }</style></head>
        <body><h1>Kathmandu Metropolitan City</h1><p>Public notice</p></body></html>
        """
        self.assertEqual(extract_html_text(html), "Kathmandu Metropolitan City Public notice")

    def test_block_elements_do_not_glue_words(self):
        self.assertEqual(
            extract_html_text("<ul><li>Kathmandu</li><li>Pokhara</li></ul>"), "Kathmandu Pokhara"
        )
        self.assertEqual(extract_html_text("line one<br>line two"), "line one line two")

    def test_entities_are_decoded(self):
        self.assertEqual(extract_html_text("<p>Tax &amp; fees &#2325;</p>"), "Tax & fees क")

    def test_site_chrome_is_dropped_when_the_page_has_content_of_its_own(self):
        body = " ".join(f"word{i}" for i in range(60))
        html = f"<nav>Home About Contact</nav><main><p>{body}</p></main><footer>Copyright</footer>"
        text = extract_html_text(html)
        self.assertNotIn("Home About Contact", text)
        self.assertNotIn("Copyright", text)
        self.assertIn("word0", text)

    def test_site_chrome_is_kept_on_a_page_that_is_mostly_chrome(self):
        html = "<nav>Ward office contact</nav><p>Phone 01-4200000</p>"
        self.assertEqual(extract_html_text(html), "Ward office contact Phone 01-4200000")

    def test_unclosed_tags_do_not_swallow_the_page(self):
        self.assertIn("Notice", extract_html_text("<div><p>Notice<p>Second paragraph"))


class NormalizationTests(unittest.TestCase):
    def test_nfc_makes_composed_and_decomposed_text_identical(self):
        composed = "क़"  # DEVANAGARI LETTER QA (precomposed)
        decomposed = unicodedata.normalize("NFD", composed)
        self.assertNotEqual(composed, decomposed)
        self.assertEqual(normalize_text(composed), normalize_text(decomposed))
        self.assertEqual(sha256_text(composed), sha256_text(decomposed))

    def test_control_characters_and_whitespace(self):
        self.assertEqual(normalize_text(" a\x00b\t\n c  "), "a b c")


class LanguageTests(unittest.TestCase):
    def test_detects_english_nepali_and_mixed_text(self):
        self.assertEqual(detect_language("Kathmandu notice"), "en")
        self.assertEqual(detect_language(NEPALI), "ne")
        self.assertEqual(detect_language(f"Kathmandu Metropolitan {NEPALI}"), "mixed")
        self.assertEqual(detect_language("123 456"), "unknown")

    def test_a_little_latin_does_not_make_a_nepali_page_mixed(self):
        self.assertEqual(detect_language(f"{NEPALI} {NEPALI} {NEPALI} {NEPALI} PDF"), "ne")


class RecordTests(unittest.TestCase):
    def test_transform_document_adds_hashes_and_language(self):
        result = transform_document(
            source_url="https://example.gov.np/notice/1",
            target_domain="kathmandu.gov.np",
            text="Kathmandu  Metropolitan City notice",
            title=" Notice ",
            description="",
        )
        self.assertEqual(result["language_detected"], "en")
        self.assertEqual(result["searchable_text"], "Kathmandu Metropolitan City notice")
        self.assertEqual(result["title"], "Notice")
        self.assertIsNone(result["description"])
        self.assertEqual(result["word_count"], 4)
        self.assertEqual(len(result["content_sha256"]), 64)
        self.assertEqual(len(result["simhash"]), 16)
        self.assertNotIn("geo_location", result)

    def test_transform_is_deterministic(self):
        first = transform_document(source_url="u", text="same text")
        second = transform_document(source_url="u", text="same text")
        self.assertEqual(first, second)

    def test_simhash_keeps_its_original_definition(self):
        # The low 64 bits of each token's SHA-256, as before the refactor.
        import hashlib

        tokens = ["pokhara", "notice"]
        weights = [0] * 64
        for token in tokens:
            digest = int(hashlib.sha256(token.encode()).hexdigest(), 16)
            for bit in range(64):
                weights[bit] += 1 if digest & (1 << bit) else -1
        expected = sum(1 << bit for bit, weight in enumerate(weights) if weight > 0)
        self.assertEqual(simhash_text("Pokhara notice"), expected)

    def test_near_duplicates_have_close_simhashes(self):
        base = "Pokhara city notice for tax payment payment payment"
        left = simhash_text(base)
        right = simhash_text(base + " update")
        self.assertLessEqual((left ^ right).bit_count(), 3)


class EventTests(unittest.TestCase):
    EVENT: ClassVar[dict] = {
        "event_type": "site_crawl_completed",
        "schema_version": 1,
        "crawl_run_id": 7,
        "target_domain": "ward.gov.np",
        "bucket": "crawled-pages",
        "key_prefix": "",
        "documents_prefix": "7/ward.gov.np/",
    }

    def test_accepts_a_site_event_with_or_without_schema_version(self):
        self.assertIs(validate_event(self.EVENT), self.EVENT)
        legacy = {k: v for k, v in self.EVENT.items() if k != "schema_version"}
        self.assertIs(validate_event(legacy), legacy)

    def test_rejects_malformed_or_unsafe_events(self):
        bad_events = [
            {},
            ["not", "a", "dict"],
            {**self.EVENT, "event_type": "file_ready"},
            {**self.EVENT, "schema_version": 2},
            {**self.EVENT, "bucket": ""},
            {**self.EVENT, "crawl_run_id": "7"},
            {**self.EVENT, "crawl_run_id": 0},
            {**self.EVENT, "documents_prefix": "7/ward.gov.np"},
            {**self.EVENT, "documents_prefix": "../other/"},
            {**self.EVENT, "key_prefix": "/etc"},
            {**self.EVENT, "target_domain": "ward.gov.np/../x"},
        ]
        for bad in bad_events:
            with self.subTest(event=bad), self.assertRaises(ValueError):
                validate_event(bad)


if __name__ == "__main__":
    unittest.main()
