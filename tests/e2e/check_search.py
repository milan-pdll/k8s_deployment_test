"""Exit 0 if the API's search response on stdin has a result whose URL contains argv[1]."""

import json
import sys

body = json.load(sys.stdin)
needle = sys.argv[1]
urls = [result["url"] for result in body["results"]]
total, degraded = body["total_hits"], body["degraded"]
print(f"   {total} hit(s), degraded={degraded}: {urls[:3]}")
sys.exit(0 if any(needle in url for url in urls) else 1)
