"""The search engine's gRPC server (`python -m pgs_search.grpc.server`).

Startup: make sure the OpenSearch index exists (index_manager), load every model
(LightGBM first, then the PyTorch ones, in one thread), and only then open the port and
report SERVING on the standard grpc.health.v1 service -- an open port means ready.
SIGTERM: report NOT_SERVING, stop taking requests and give running ones a grace period.
Concurrency is bounded: SEARCH_GRPC_WORKERS threads, and requests beyond twice that are
refused with RESOURCE_EXHAUSTED instead of queueing without limit.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
from collections.abc import Callable
from concurrent import futures
from typing import Any

NATIVE_THREAD_ENV_DEFAULTS = {
    "OMP_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "VECLIB_MAXIMUM_THREADS": "1",
}


def configure_native_thread_environment() -> None:
    """Set conservative native thread defaults before ML libraries initialize: requests
    run in parallel threads, so each inference uses one core."""
    for name, value in NATIVE_THREAD_ENV_DEFAULTS.items():
        os.environ.setdefault(name, value)


configure_native_thread_environment()

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from pgs_search.config import settings
from pgs_search.grpc.generated import search_pb2_grpc

logger = logging.getLogger(__name__)

SERVICE_NAME = "search.engine.v1.SearchService"
SHUTDOWN_GRACE_SECONDS = 10


def preload_lightgbm_reranker(loader: Callable[[], Any] | None = None) -> bool:
    """Load LightGBM before Torch to avoid OpenMP runtime conflicts."""
    if loader is None:
        from pgs_search.ranking.lightgbm_reranker import get_model as loader

    try:
        loader()
    except (FileNotFoundError, ValueError) as exc:
        logger.warning("LightGBM reranker unavailable; results keep the fusion order: %s", exc)
        return False
    logger.info("LightGBM reranker ready")
    return True


def configure_pytorch_threads(torch_module: Any | None = None) -> None:
    """One PyTorch thread per inference (see configure_native_thread_environment)."""
    if torch_module is None:
        import torch as torch_module

    torch_module.set_num_threads(1)
    try:
        torch_module.set_num_interop_threads(1)
    except RuntimeError as exc:
        logger.warning("could not set PyTorch interop threads: %s", exc)


def preload_search_runtime() -> None:
    """Initialize the native ML runtimes and models in a fixed order, in one thread."""
    configure_native_thread_environment()
    preload_lightgbm_reranker()
    configure_pytorch_threads()
    if settings.translation_enabled and settings.translation_backend == "local":
        from pgs_search.query.translation import get_model_and_tokenizer

        get_model_and_tokenizer()
        logger.info("translation model ready")
    from pgs_search.query.embeddings import get_query_embedder

    get_query_embedder().load()
    logger.info("query embedding model ready")


def build_server(pipeline: Any, health_servicer: health.HealthServicer) -> grpc.Server:
    from pgs_search.grpc.service import SearchService

    max_message = settings.search_grpc_max_message_bytes
    server = grpc.server(
        futures.ThreadPoolExecutor(
            max_workers=settings.search_grpc_workers, thread_name_prefix="search"
        ),
        maximum_concurrent_rpcs=settings.search_grpc_workers * 2,
        options=[
            ("grpc.max_receive_message_length", 64 * 1024),  # a request is a query
            ("grpc.max_send_message_length", max_message),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.min_ping_interval_without_data_ms", 10_000),
        ],
    )
    search_pb2_grpc.add_SearchServiceServicer_to_server(SearchService(pipeline), server)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    return server


def _bind(server: grpc.Server, address: str) -> None:
    """TLS when a certificate and key are configured, else plaintext."""
    cert_file, key_file = settings.search_grpc_tls_cert_file, settings.search_grpc_tls_key_file
    if bool(cert_file) != bool(key_file):
        raise RuntimeError("set both SEARCH_GRPC_TLS_CERT_FILE and SEARCH_GRPC_TLS_KEY_FILE")
    if cert_file and key_file:
        with open(cert_file, "rb") as cert, open(key_file, "rb") as key:
            credentials = grpc.ssl_server_credentials([(key.read(), cert.read())])
        server.add_secure_port(address, credentials)
        logger.info("search gRPC server uses TLS")
    else:
        server.add_insecure_port(address)


def serve() -> None:
    from pgs_search.client.opensearch import get_opensearch_client
    from pgs_search.indexing.index_manager import ensure_index
    from pgs_search.pipeline import default_pipeline

    ensure_index(get_opensearch_client())
    preload_search_runtime()

    health_servicer = health.HealthServicer()
    server = build_server(default_pipeline(), health_servicer)
    address = f"{settings.search_grpc_host}:{settings.search_grpc_port}"
    _bind(server, address)
    server.start()
    for name in ("", SERVICE_NAME):
        health_servicer.set(name, health_pb2.HealthCheckResponse.SERVING)
    logger.info("search gRPC server listening on %s", address)

    stopped = threading.Event()

    def stop(signum: int, _frame: Any) -> None:
        logger.info("signal %d: draining for up to %d s", signum, SHUTDOWN_GRACE_SECONDS)
        health_servicer.enter_graceful_shutdown()
        server.stop(grace=SHUTDOWN_GRACE_SECONDS).wait()
        stopped.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.wait_for_termination()
    stopped.wait(timeout=SHUTDOWN_GRACE_SECONDS + 5)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    serve()


if __name__ == "__main__":
    main()
