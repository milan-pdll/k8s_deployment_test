"""The scraper -> ETL hand-off event, validated the same way by its two consumers.

The Go worker publishes one ``site_crawl_completed`` event to Kafka
(``scraped_files_topic``, key = target_domain) when a website's crawl has finished and
every page of it is in the bucket (scraper/internal/storage/site_events.go). The
``etl_ingestion_pipeline`` DAG validates each message before it batches it, and
``site_pipeline.run_site_pipeline`` validates it again before it touches S3.

Standard library only: the DAG imports this module in Airflow's own environment.
"""

from __future__ import annotations

import posixpath
from typing import Any

SITE_EVENT_TYPE = "site_crawl_completed"
# Versions of the event this code understands. A missing schema_version is 1 (events
# published before the field existed).
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})
REQUIRED_FIELDS = ("crawl_run_id", "target_domain", "bucket", "documents_prefix")


def validate_event(event: object) -> dict[str, Any]:
    """Return the event if the ETL can run it; raise ValueError (permanent) if not."""
    if not isinstance(event, dict) or event.get("event_type") != SITE_EVENT_TYPE:
        raise ValueError(f"not a {SITE_EVENT_TYPE} event")
    version = event.get("schema_version", 1)
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported schema_version {version!r}")
    missing = [field for field in REQUIRED_FIELDS if not event.get(field)]
    if missing:
        raise ValueError(f"site event is missing {', '.join(missing)}")
    run_id = event["crawl_run_id"]
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id <= 0:
        raise ValueError("crawl_run_id must be a positive integer")
    prefix = str(event["documents_prefix"])
    if not prefix.endswith("/"):
        raise ValueError("documents_prefix must end with '/'")
    # The prefix and the keys built from key_prefix come from a message, not from us:
    # refuse anything that could escape the crawl's own part of the bucket.
    for name in ("documents_prefix", "key_prefix"):
        value = str(event.get(name) or "")
        if value.startswith("/") or ".." in value.split("/"):
            raise ValueError(f"{name} must be a relative key prefix")
    host = str(event["target_domain"])
    if "/" in host or host != host.strip() or posixpath.normpath(host) != host:
        raise ValueError("target_domain must be a bare host name")
    return event
