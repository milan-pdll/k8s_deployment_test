import math
import os
import unittest

try:
    import numpy as np
except ImportError:  # the light test environment has no numpy
    np = None

from embeddings import Embedder, EmbeddingConfig

CONFIG = EmbeddingConfig(
    model_name="fake/model", revision="abc", dimensions=2, batch_size=8, max_chunks=2
)


class FakeTokenizer:
    """Whitespace 'tokenizer': one token per word."""

    def __call__(self, text, add_special_tokens, truncation):
        return {"input_ids": text.split()}

    def num_special_tokens_to_add(self, pair):
        return 2

    def decode(self, ids, skip_special_tokens):
        return " ".join(ids)


class FakeModel:
    max_seq_length = 5  # 3 text tokens per window
    tokenizer = FakeTokenizer()

    def __init__(self):
        self.encoded = []

    def get_sentence_embedding_dimension(self):
        return 2

    def encode(self, chunks, **kwargs):
        self.encoded.extend(chunks)
        # One axis per chunk position, so means are easy to check.
        return [
            np.array([1.0, 0.0]) if i % 2 == 0 else np.array([0.0, 1.0])
            for i, _ in enumerate(chunks)
        ]


@unittest.skipIf(np is None, "numpy is not installed")
class EmbedderTests(unittest.TestCase):
    def test_blank_texts_get_no_vector_and_no_model_call(self):
        model = FakeModel()
        self.assertEqual(Embedder(CONFIG, model).embed(["", "  "]), [None, None])
        self.assertEqual(model.encoded, [])

    def test_long_text_is_windowed_capped_and_mean_pooled(self):
        model = FakeModel()
        text = " ".join(f"w{i}" for i in range(10))  # 4 windows of 3, capped at 2
        [vector] = Embedder(CONFIG, model).embed([text])
        self.assertEqual(model.encoded, ["w0 w1 w2", "w3 w4 w5"])
        self.assertAlmostEqual(vector[0], 1 / math.sqrt(2))
        self.assertAlmostEqual(vector[1], 1 / math.sqrt(2))

    def test_vectors_keep_input_order(self):
        model = FakeModel()
        vectors = Embedder(CONFIG, model).embed(["a", "", "b"])
        self.assertIsNotNone(vectors[0])
        self.assertIsNone(vectors[1])
        self.assertIsNotNone(vectors[2])

    def test_wrong_model_dimension_is_a_configuration_error(self):
        import sys
        from types import ModuleType

        fake_module = ModuleType("sentence_transformers")
        fake_module.SentenceTransformer = lambda *args, **kwargs: FakeModel()
        sys.modules["sentence_transformers"] = fake_module
        try:
            config = EmbeddingConfig("fake/model", None, 768, 8, 2)
            with self.assertRaises(RuntimeError):
                Embedder(config).embed(["text"])
        finally:
            del sys.modules["sentence_transformers"]


@unittest.skipUnless(
    os.environ.get("RUN_LABSE_TESTS") == "1", "set RUN_LABSE_TESTS=1 (loads the real LaBSE)"
)
class LabseContractTests(unittest.TestCase):
    """The real pinned model: 768-d, normalized, and cross-lingual (what the search
    engine relies on when it compares an English query with Nepali pages)."""

    def test_labse_contract(self):
        embedder = Embedder(EmbeddingConfig.from_env())
        english, nepali, unrelated = embedder.embed(
            ["Kathmandu Metropolitan City", "काठमाडौं महानगरपालिका", "chocolate cake recipe"]
        )
        self.assertEqual(len(english), 768)
        self.assertAlmostEqual(sum(v * v for v in english), 1.0, places=4)
        same = sum(a * b for a, b in zip(english, nepali, strict=True))
        other = sum(a * b for a, b in zip(english, unrelated, strict=True))
        self.assertGreater(same, other + 0.2)


if __name__ == "__main__":
    unittest.main()
