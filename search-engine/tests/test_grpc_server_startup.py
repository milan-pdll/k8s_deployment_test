from __future__ import annotations

import logging

from pgs_search.grpc import server


def test_native_thread_environment_sets_defaults_without_overwriting(monkeypatch) -> None:
    for name in server.NATIVE_THREAD_ENV_DEFAULTS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OMP_NUM_THREADS", "8")

    server.configure_native_thread_environment()

    assert server.os.environ["OMP_NUM_THREADS"] == "8"
    assert server.os.environ["MKL_NUM_THREADS"] == "1"
    assert server.os.environ["OPENBLAS_NUM_THREADS"] == "1"
    assert server.os.environ["VECLIB_MAXIMUM_THREADS"] == "1"


def test_preload_order_lightgbm_before_torch_models(monkeypatch) -> None:
    events: list[str] = []
    monkeypatch.setattr(server, "preload_lightgbm_reranker", lambda: events.append("lightgbm"))
    monkeypatch.setattr(server, "configure_pytorch_threads", lambda: events.append("torch"))

    from pgs_search.query import embeddings, translation

    monkeypatch.setattr(translation, "get_model_and_tokenizer", lambda: events.append("nllb"))

    class FakeEmbedder:
        def load(self) -> None:
            events.append("labse")

    monkeypatch.setattr(embeddings, "get_query_embedder", lambda: FakeEmbedder())

    server.preload_search_runtime()

    assert events == ["lightgbm", "torch", "nllb", "labse"]


def test_translation_model_is_not_loaded_when_disabled(monkeypatch) -> None:
    events: list[str] = []
    monkeypatch.setattr(server, "preload_lightgbm_reranker", lambda: True)
    monkeypatch.setattr(server, "configure_pytorch_threads", lambda: None)
    monkeypatch.setattr(
        server, "settings", server.settings.model_copy(update={"translation_enabled": False})
    )

    from pgs_search.query import embeddings, translation

    monkeypatch.setattr(translation, "get_model_and_tokenizer", lambda: events.append("nllb"))

    class FakeEmbedder:
        def load(self) -> None:
            events.append("labse")

    monkeypatch.setattr(embeddings, "get_query_embedder", lambda: FakeEmbedder())

    server.preload_search_runtime()

    assert events == ["labse"]


def test_configure_pytorch_threads_logs_interop_warning(caplog) -> None:
    class FakeTorch:
        num_threads: int | None = None

        def set_num_threads(self, value: int) -> None:
            self.num_threads = value

        def set_num_interop_threads(self, value: int) -> None:
            raise RuntimeError("parallel work already started")

    fake_torch = FakeTorch()
    with caplog.at_level(logging.WARNING):
        server.configure_pytorch_threads(fake_torch)

    assert fake_torch.num_threads == 1
    assert "could not set PyTorch interop threads" in caplog.text


def test_missing_lightgbm_model_keeps_the_fusion_order(caplog) -> None:
    def missing_loader() -> None:
        raise FileNotFoundError("missing model")

    with caplog.at_level(logging.WARNING):
        ready = server.preload_lightgbm_reranker(missing_loader)

    assert ready is False
    assert "fusion order" in caplog.text
