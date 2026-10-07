"""Spark Security Scanner - two layers of checks on a file before it
enters the pipeline:

  Layer 1: basic, metadata-only checks (extension, size, filename, hash).
  Layer 2: real content scanning via ClamAV (the clamd daemon).

Layer 1 alone cannot see inside a file - a file called invoice.pdf could
still contain a real payload. Layer 2 closes that gap using an actual
antivirus engine instead of us trying to write one.
"""

import hashlib
import io
import os
from typing import Any, TypedDict


class ScanResult(TypedDict):
    filename: str
    extension: str
    size_bytes: int
    sha256: str
    verdict: str
    reasons: list[str]


# Extensions we treat as risky to auto-run/auto-open.
SUSPICIOUS_EXTENSIONS = {
    "exe",
    "bat",
    "cmd",
    "com",
    "scr",
    "msi",
    "vbs",
    "js",
    "jar",
    "ps1",
    "sh",
}

# Extensions we expect to see routinely from the web crawler.
EXPECTED_EXTENSIONS = {
    "html",
    "htm",
    "pdf",
    "txt",
    "json",
    "png",
    "jpg",
    "jpeg",
    "gif",
    "docx",
    "csv",
}

MAX_SAFE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB
MAX_FILENAME_LENGTH = 100

# ClamAV (clamd) connection: CLAMD_HOST/CLAMD_PORT (clamav:3310 in docker-compose; the
# Spark workers inherit them from their container environment). The timeout bounds one
# scan; a slow or unreachable daemon is an ERROR, never a pass.
CLAMD_HOST = os.environ.get("CLAMD_HOST", "localhost")
CLAMD_PORT = int(os.environ.get("CLAMD_PORT", "3310"))
CLAMD_TIMEOUT_SECONDS = float(os.environ.get("CLAMD_TIMEOUT_SECONDS", "30"))


def get_extension(filename: str) -> str:
    name = filename.lstrip(".")  # ignore one leading dot, e.g. hidden files like .gitattributes
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1].lower()


def get_file_size(content: bytes) -> int:
    return len(content)


def get_sha256(content: bytes) -> str:
    """Return the SHA-256 hash of the file content as a hex string.

    Verified example: hashing the hello.txt sample content gives
    a3a8893ea3e12eab2e099103b9af1ebedd80e9b7b812a23909dc4d4d607e1a55
    """
    return hashlib.sha256(content).hexdigest()


def has_double_extension(filename: str) -> bool:
    parts = filename.split(".")
    return len(parts) > 2


def scan_file(filename: str, content: bytes) -> ScanResult:
    """Layer 1: fast, metadata-only checks. No network access, no ClamAV -
    kept pure and easy to unit test on its own."""
    suspicious_reasons: list[str] = []
    unknown_reasons: list[str] = []

    ext = get_extension(filename)
    if ext in SUSPICIOUS_EXTENSIONS:
        suspicious_reasons.append(f"'.{ext}' is a risky/executable extension")
    elif ext == "":
        unknown_reasons.append("file has no extension - can't identify its type")
    elif ext not in EXPECTED_EXTENSIONS:
        unknown_reasons.append(f"'.{ext}' is not a recognized extension")

    if has_double_extension(filename):
        suspicious_reasons.append("filename has multiple extensions (e.g. file.pdf.exe pattern)")

    size = get_file_size(content)
    if size == 0:
        suspicious_reasons.append("file is empty (0 bytes)")
    elif size > MAX_SAFE_SIZE_BYTES:
        suspicious_reasons.append(f"file is larger than {MAX_SAFE_SIZE_BYTES} bytes")

    if len(filename) > MAX_FILENAME_LENGTH:
        suspicious_reasons.append(f"filename is unusually long ({len(filename)} characters)")

    if suspicious_reasons:
        verdict = "SUSPICIOUS"
        reasons = suspicious_reasons
    elif unknown_reasons:
        verdict = "UNKNOWN"
        reasons = unknown_reasons
    else:
        verdict = "SAFE"
        reasons = ["no issues found"]

    return {
        "filename": filename,
        "extension": ext,
        "size_bytes": size,
        "sha256": get_sha256(content),
        "verdict": verdict,
        "reasons": reasons,
    }


def scan_with_clamav(content: bytes) -> dict[str, Any]:
    """Layer 2: send the actual file bytes to ClamAV (clamd) over the
    network and ask it to scan them. This is real antivirus scanning -
    we are not attempting to detect malware ourselves.

    Returns one of:
      {"status": "CLEAN",    "signature": None}
      {"status": "INFECTED", "signature": "<threat name>"}
      {"status": "ERROR",    "signature": None, "error": "<message>"}

    A connection failure or any unexpected response is always ERROR,
    never CLEAN - a security check that fails open is not a security
    check.
    """
    import clamd

    try:
        client = clamd.ClamdNetworkSocket(
            host=CLAMD_HOST, port=CLAMD_PORT, timeout=CLAMD_TIMEOUT_SECONDS
        )
        response = client.instream(io.BytesIO(content))
        status, signature = response["stream"]
    except Exception as exc:  # clamd down, connection refused, timeout, etc.
        return {"status": "ERROR", "signature": None, "error": str(exc)}

    if status == "OK":
        return {"status": "CLEAN", "signature": None}
    if status == "FOUND":
        return {"status": "INFECTED", "signature": signature}
    return {"status": "ERROR", "signature": None, "error": f"unexpected clamd status: {status!r}"}


def ping_clamav() -> None:
    """Raise if clamd cannot answer PING. The ETL checks this before a site's run, so
    an outage retries the run instead of failing page after page."""
    import clamd

    client = clamd.ClamdNetworkSocket(
        host=CLAMD_HOST, port=CLAMD_PORT, timeout=CLAMD_TIMEOUT_SECONDS
    )
    if client.ping() != "PONG":
        raise ConnectionError(f"clamd at {CLAMD_HOST}:{CLAMD_PORT} did not answer PING")


def inspect_bytes(filename: str, content: bytes) -> dict[str, Any]:
    """Both layers for content already in memory (an object read from S3).

    ``accepted`` is True only when Layer 1 found nothing AND ClamAV actively confirmed
    the content is clean. An empty or oversized payload is rejected by Layer 1 without
    being streamed to ClamAV (clamav_status "SKIPPED"); an unreachable or failing
    ClamAV gives clamav_status "ERROR", which the pipeline treats as "cannot scan, do
    not process" (it retries the site), never as a pass.
    """
    result = scan_file(filename, content)

    findings: list[str] = []
    extension = result["extension"]
    if extension in SUSPICIOUS_EXTENSIONS:
        findings.append("suspicious_extension")
    if extension not in EXPECTED_EXTENSIONS:
        findings.append("unexpected_extension")
    if result["size_bytes"] == 0:
        findings.append("empty_file")
    elif result["size_bytes"] > MAX_SAFE_SIZE_BYTES:
        findings.append("file_too_large")
    if len(filename) > MAX_FILENAME_LENGTH:
        findings.append("filename_too_long")
    if has_double_extension(filename):
        findings.append("multiple_extensions")

    if "empty_file" in findings or "file_too_large" in findings:
        clamav_result: dict[str, Any] = {"status": "SKIPPED", "signature": None}
    else:
        clamav_result = scan_with_clamav(content)
    clamav_status = clamav_result["status"]

    if clamav_status == "INFECTED":
        findings.append(f"clamav_infected:{clamav_result['signature']}")
        verdict = "INFECTED"
    elif clamav_status == "ERROR":
        findings.append("clamav_unavailable")
        verdict = "UNKNOWN"
    elif findings:
        verdict = "SUSPICIOUS"
    else:
        verdict = "SAFE"

    accepted = clamav_status == "CLEAN" and not findings

    return {
        "accepted": accepted,
        "filename": result["filename"],
        "extension": extension,
        "size_bytes": result["size_bytes"],
        "sha256": result["sha256"],
        "findings": findings,
        "verdict": verdict,
        "clamav_status": clamav_status,
        "clamav_signature": clamav_result.get("signature"),
        "clamav_error": clamav_result.get("error"),
    }
