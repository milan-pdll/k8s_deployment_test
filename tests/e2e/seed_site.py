"""Put a small synthetic crawled website into the crawl bucket, the way the Go scraper
stores one (scraper/internal/storage: Document JSON per page under
<run>/<host>/, HTML under html/<host>/<content hash>.html), and print its
site_crawl_completed event (one JSON line) on stdout.

Runs in the ETL image (boto3, S3 settings from the etl-worker environment):
    docker compose run --rm --no-deps -T --entrypoint /opt/etl-venv/bin/python \
        -v ./tests/e2e:/e2e:ro etl-worker /e2e/seed_site.py <crawl_run_id>
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import UTC, datetime

import boto3

BUCKET = "crawled-pages"
# A seeded local-government site: its pages are geo-tagged through its local body.
HOST = "pokharamun.gov.np"

PAGES = [
    (
        f"https://{HOST}/notice/e2e-budget",
        "Pokhara budget notice",
        "<html><head><title>Budget</title></head><body><nav>Home About</nav>"
        "<main><h1>Pokhara Metropolitan City annual budget</h1>"
        "<p>The annual budget for fiscal year 2083/84 allocates funds for road maintenance, "
        "drinking water projects and ward office upgrades across all thirty-three wards. "
        "Citizens can submit feedback on the e2e-marker-zebra proposal at the ward office "
        "before the end of the month.</p></main></body></html>",
    ),
    (
        f"https://{HOST}/notice/e2e-nepali",
        "पोखरा महानगरपालिका सूचना",
        "<html><body><main><h1>पोखरा महानगरपालिकाको वार्षिक बजेट</h1>"
        "<p>आर्थिक वर्ष २०८३/८४ को बजेटमा सडक मर्मत, खानेपानी आयोजना र वडा कार्यालय "
        "सुधारका लागि रकम विनियोजन गरिएको छ। नागरिकहरूले वडा कार्यालयमा सुझाव "
        "दिन सक्नेछन्।</p></main></body></html>",
    ),
]


def main() -> None:
    run_id = int(sys.argv[1])
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT") or None,
        region_name=os.environ.get("AWS_REGION") or None,
    )
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    for url, title, html in PAGES:
        body = html.encode("utf-8")
        content_hash = hashlib.sha256(body).hexdigest()
        html_key = f"html/{HOST}/{content_hash}.html"
        s3.put_object(Bucket=BUCKET, Key=html_key, Body=body, ContentType="text/html")
        document = {
            "url": url,
            "normalized_url": url,
            "host": HOST,
            "category": "government",
            "title": title,
            "text": "",
            "links": [],
            "crawl_run_id": run_id,
            "html_key": html_key,
            "depth": 1,
            "status_code": 200,
            "content_type": "text/html; charset=utf-8",
            "content_hash": content_hash,
            "fetched_at": now,
            "fetch_duration_ms": 10,
        }
        key = f"{run_id}/{HOST}/{hashlib.sha256(url.encode()).hexdigest()}.json"
        body = json.dumps(document).encode()
        s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
    event = {
        "event_type": "site_crawl_completed",
        "schema_version": 1,
        "crawl_run_id": run_id,
        "workflow_id": f"e2e-{run_id}",
        "target_domain": HOST,
        "status": "completed",
        "pages_fetched": len(PAGES),
        "bucket": BUCKET,
        "key_prefix": "",
        "documents_prefix": f"{run_id}/{HOST}/",
        "completed_at": now,
    }
    print(json.dumps(event))


if __name__ == "__main__":
    main()
