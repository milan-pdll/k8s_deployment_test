"""Search engine settings: read once from the environment (no .env file).

docker-compose.yml sets them for the `search-engine` (gRPC server) and `search-indexer`
services; field names map to upper-case variables (opensearch_host -> OPENSEARCH_HOST).
PostgreSQL is reached through pgs_db, which reads DATABASE_URL (role pgs_search).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", frozen=True)

    # --- OpenSearch (the lexical index) -----------------------------------------------
    opensearch_host: str = "localhost"
    opensearch_port: int = 9200
    # https + credentials in production (the security plugin enabled); plain http only
    # for local development, where the plugin is disabled.
    opensearch_scheme: str = Field(default="http", pattern="^https?$")
    opensearch_username: str | None = None
    opensearch_password: str | None = None
    opensearch_verify_certs: bool = True
    opensearch_ca_certs: str | None = None
    opensearch_timeout_seconds: float = Field(default=5.0, gt=0)
    # The alias every reader and writer uses; it points at np_web_pages_v<N>.
    opensearch_index: str = "np_web_pages"
    opensearch_replicas: int = Field(default=0, ge=0)

    # --- Query embeddings: MUST be the model and revision the ETL embeds documents with
    embedding_model_name: str = "sentence-transformers/LaBSE"
    embedding_model_revision: str | None = None
    embedding_dimensions: int = Field(default=768, gt=0)

    # --- Query translation (NLLB), used for cross-language lexical expansion ----------
    translation_enabled: bool = True
    # "local" runs NLLB in this process; "remote" calls an OpenAI-compatible chat API
    # (OpenRouter ":free" models, Gemini's OpenAI endpoint, ...), so the server loads no model.
    translation_backend: Literal["local", "remote"] = "local"
    translation_api_base_url: str = "https://openrouter.ai/api/v1"
    translation_api_model: str = "meta-llama/llama-3.3-70b-instruct:free"
    translation_api_key: str | None = None
    translation_api_timeout_seconds: float = Field(default=3.0, gt=0)
    translation_model_name: str = "facebook/nllb-200-distilled-600M"
    translation_model_revision: str | None = None
    # int8-quantize the NLLB model (faster, smaller; slightly lower quality). For laptops.
    translation_quantize: bool = False
    # How long a search waits for the translated variant once the other work is done.
    # Slower translations finish in the background (and are cached for the next search).
    translation_wait_seconds: float = Field(default=0.3, ge=0)
    # Longer queries are not translated: their cost grows with length, and LaBSE's
    # cross-lingual vectors already cover them.
    translation_max_query_chars: int = Field(default=200, gt=0)

    # --- Request bounds -------------------------------------------------------------
    max_query_chars: int = Field(default=512, gt=0)
    max_limit: int = Field(default=100, gt=0)
    # page * limit may not exceed this: deep paging would mean unbounded retrieval.
    max_result_window: int = Field(default=500, gt=0)
    # Candidates each retriever returns at least (more when the page window needs it,
    # never more than max_result_window).
    candidate_pool: int = Field(default=100, gt=0)
    rerank_enabled: bool = True

    # --- gRPC server ----------------------------------------------------------------
    search_grpc_host: str = "0.0.0.0"
    search_grpc_port: int = 50051
    search_grpc_workers: int = Field(default=8, gt=0)
    # TLS for the gRPC port: both files set = TLS (clients need the CA); neither = plaintext
    # (private network only). Mount the PEM files into the container.
    search_grpc_tls_cert_file: str | None = None
    search_grpc_tls_key_file: str | None = None
    search_grpc_max_message_bytes: int = Field(default=4 * 1024 * 1024, gt=0)

    # --- Indexer (Silver -> OpenSearch) ---------------------------------------------
    indexer_batch_size: int = Field(default=100, gt=0)
    indexer_idle_seconds: float = Field(default=5.0, gt=0)
    # A page claimed by an indexer that died is handed out again after this long.
    indexer_stale_after_seconds: int = Field(default=900, gt=0)
    indexer_heartbeat_file: str | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
