from __future__ import annotations

from opensearchpy import OpenSearch

from pgs_search.config import Settings, settings


def get_opensearch_client(config: Settings = settings) -> OpenSearch:
    """An OpenSearch client with bounded timeouts. Credentials and TLS come from the
    environment (OPENSEARCH_USERNAME/PASSWORD, OPENSEARCH_SCHEME=https)."""
    auth = None
    if config.opensearch_username and config.opensearch_password:
        auth = (config.opensearch_username, config.opensearch_password)
    https = config.opensearch_scheme == "https"
    return OpenSearch(
        hosts=[
            {
                "host": config.opensearch_host,
                "port": config.opensearch_port,
                "scheme": config.opensearch_scheme,
            }
        ],
        http_auth=auth,
        use_ssl=https,
        verify_certs=https and config.opensearch_verify_certs,
        ca_certs=config.opensearch_ca_certs,
        timeout=config.opensearch_timeout_seconds,
        max_retries=1,
        retry_on_timeout=False,
        http_compress=True,
    )
