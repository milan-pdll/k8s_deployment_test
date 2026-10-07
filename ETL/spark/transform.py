"""Text processing for one crawled page: HTML -> normalized text, language, hashes.

Runs inside the Spark executors (shipped with SparkContext.addPyFile), once per page.
Standard library only and deterministic: the same bytes always give the same record,
so re-running a site produces identical hashes and Silver can recognise unchanged
pages. Embeddings are computed afterwards, on the driver (embeddings.py).
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from html.parser import HTMLParser
from typing import Any

WORD_RE = re.compile(r"[\wऀ-ॿ]+", re.UNICODE)
SPACE_RE = re.compile(r"\s+")
# C0/C1 control characters other than whitespace; ZWJ/ZWNJ stay (Devanagari conjuncts).
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")

# Never page text.
_SKIP_TAGS = frozenset({"script", "style", "noscript", "template", "svg", "title"})
# Site chrome repeated on every page: menus, banners, footers. Dropping it keeps one
# site's pages from looking alike to the near-duplicate check and keeps menus out of
# the index, unless that leaves too little text (see extract_html_text).
_BOILERPLATE_TAGS = frozenset({"nav", "header", "footer", "aside"})
# Elements that end a run of text, so words of adjacent blocks are not glued together
# ("<li>Kathmandu</li><li>Pokhara</li>" must not become "KathmanduPokhara").
_BLOCK_TAGS = frozenset(
    {
        "address",
        "article",
        "blockquote",
        "br",
        "caption",
        "dd",
        "div",
        "dl",
        "dt",
        "figcaption",
        "form",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "li",
        "main",
        "ol",
        "option",
        "p",
        "pre",
        "section",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    }
)
# Below this many words without the site chrome, the chrome is kept: on very short
# pages it may be all the content there is.
MIN_MAIN_CONTENT_WORDS = 40

# Share of letters one script needs before a text counts as that language.
_DOMINANT_SCRIPT_SHARE = 0.8


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.all_parts: list[str] = []
        self.main_parts: list[str] = []
        self._skip_depth = 0
        self._boilerplate_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS:
            self._skip_depth += 1
        elif tag in _BOILERPLATE_TAGS:
            self._boilerplate_depth += 1
        if tag in _BLOCK_TAGS:
            self._separate()

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in _BLOCK_TAGS:
            self._separate()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1
        elif tag in _BOILERPLATE_TAGS and self._boilerplate_depth:
            self._boilerplate_depth -= 1
        if tag in _BLOCK_TAGS:
            self._separate()

    def handle_data(self, data: str) -> None:
        if self._skip_depth or not data.strip():
            return
        self.all_parts.append(data)
        if not self._boilerplate_depth:
            self.main_parts.append(data)

    def _separate(self) -> None:
        self.all_parts.append("\n")
        self.main_parts.append("\n")


def normalize_text(text: str) -> str:
    """NFC-normalize, drop control characters and collapse whitespace.

    NFC matters for Devanagari: the same word typed with a precomposed or a decomposed
    nukta must hash, tokenize and match identically.
    """
    text = unicodedata.normalize("NFC", text or "")
    text = CONTROL_RE.sub(" ", text)
    return SPACE_RE.sub(" ", text).strip()


def extract_html_text(raw_html: str) -> str:
    """Visible text of an HTML page, without scripts/styles and, when the page has
    enough content of its own, without the navigation/header/footer chrome."""
    parser = _HTMLTextExtractor()
    parser.feed(raw_html)
    parser.close()
    main = normalize_text("".join(parser.main_parts))
    if len(tokenize(main)) >= MIN_MAIN_CONTENT_WORDS:
        return main
    return normalize_text("".join(parser.all_parts))


def detect_language(text: str) -> str:
    """ne | en | mixed | unknown, from the share of Devanagari vs Latin letters.

    One English word (a URL, an acronym) in a Nepali page does not make it mixed:
    a script needs at least 80% of the letters to decide the language on its own.
    """
    devanagari = sum(1 for char in text if "ऀ" <= char <= "ॿ" and char.isalpha())
    latin = sum(1 for char in text if char.isascii() and char.isalpha())
    letters = devanagari + latin
    if not letters:
        return "unknown"
    if devanagari / letters >= _DOMINANT_SCRIPT_SHARE:
        return "ne"
    if latin / letters >= _DOMINANT_SCRIPT_SHARE:
        return "en"
    return "mixed"


def tokenize(text: str) -> list[str]:
    return [match.group(0).lower() for match in WORD_RE.finditer(text)]


def sha256_text(text: str) -> str:
    return hashlib.sha256(normalize_text(text).encode("utf-8")).hexdigest()


def simhash_text(text: str, bits: int = 64) -> int:
    """64-bit SimHash of the text's tokens (unsigned). Near-duplicate pages differ in
    only a few bits; Silver compares fingerprints when it saves a page."""
    tokens = tokenize(text)
    if not tokens:
        return 0
    weights = [0] * bits
    for token in tokens:
        # The low 64 bits of the token's SHA-256 (as int(hexdigest, 16) & (2**64 - 1)).
        digest = int.from_bytes(hashlib.sha256(token.encode("utf-8")).digest()[-8:], "big")
        for index in range(bits):
            weights[index] += 1 if digest & (1 << index) else -1
    fingerprint = 0
    for index, weight in enumerate(weights):
        if weight > 0:
            fingerprint |= 1 << index
    return fingerprint


def transform_document(
    *,
    source_url: str,
    text: str,
    title: str = "",
    description: str = "",
    target_domain: str = "",
    object_key: str = "",
) -> dict[str, Any]:
    """One page's record, in the shape pgs_db.etl.payload_from_transform reads."""
    normalized = normalize_text(text)
    return {
        "source_url": source_url,
        "object_key": object_key,
        "target_domain": target_domain,
        "title": normalize_text(title),
        "description": normalize_text(description) or None,
        "language_detected": detect_language(normalized),
        "searchable_text": normalized,
        "word_count": len(tokenize(normalized)),
        "char_count": len(normalized),
        "content_sha256": sha256_text(normalized),
        "simhash": f"{simhash_text(normalized):016x}",
    }
