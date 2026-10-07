import unittest
from unittest.mock import patch

from security_scanner import MAX_SAFE_SIZE_BYTES, inspect_bytes

CLEAN_RESULT = {"status": "CLEAN", "signature": None}
INFECTED_RESULT = {"status": "INFECTED", "signature": "Eicar-Test-Signature"}
ERROR_RESULT = {"status": "ERROR", "signature": None, "error": "connection refused"}


class SecurityScannerTests(unittest.TestCase):
    # -- Layer 1: rule checks (ClamAV mocked as CLEAN, so only the rules are tested) --

    @patch("security_scanner.scan_with_clamav", return_value=CLEAN_RESULT)
    def test_accepts_expected_document_type(self, clamav):
        result = inspect_bytes("notice.html", b"<h1>Notice</h1>")

        self.assertTrue(result["accepted"])
        self.assertEqual(result["extension"], "html")
        self.assertEqual(result["findings"], [])
        self.assertEqual(result["verdict"], "SAFE")
        self.assertEqual(len(result["sha256"]), 64)
        clamav.assert_called_once()

    @patch("security_scanner.scan_with_clamav", return_value=CLEAN_RESULT)
    def test_rejects_suspicious_extension(self, clamav):
        result = inspect_bytes("payload.exe", b"binary")

        self.assertFalse(result["accepted"])
        self.assertIn("suspicious_extension", result["findings"])
        self.assertIn("unexpected_extension", result["findings"])
        self.assertEqual(result["verdict"], "SUSPICIOUS")

    @patch("security_scanner.scan_with_clamav", return_value=CLEAN_RESULT)
    def test_oversized_and_empty_payloads_are_not_streamed_to_clamav(self, clamav):
        too_big = inspect_bytes("page.html", b"x" * (MAX_SAFE_SIZE_BYTES + 1))
        empty = inspect_bytes("page.html", b"")

        clamav.assert_not_called()
        self.assertFalse(too_big["accepted"])
        self.assertIn("file_too_large", too_big["findings"])
        self.assertEqual(too_big["clamav_status"], "SKIPPED")
        self.assertFalse(empty["accepted"])
        self.assertIn("empty_file", empty["findings"])

    # -- Layer 2: ClamAV (mocked; no live daemon needed) --

    @patch("security_scanner.scan_with_clamav", return_value=INFECTED_RESULT)
    def test_rejects_clamav_detected_threat(self, clamav):
        result = inspect_bytes("invoice.pdf", b"harmless-looking bytes, ClamAV mocked as FOUND")

        self.assertFalse(result["accepted"])
        self.assertEqual(result["verdict"], "INFECTED")
        self.assertEqual(result["clamav_signature"], "Eicar-Test-Signature")
        self.assertTrue(any(f.startswith("clamav_infected") for f in result["findings"]))

    @patch("security_scanner.scan_with_clamav", return_value=ERROR_RESULT)
    def test_unknown_when_clamav_unavailable(self, clamav):
        result = inspect_bytes("notice.html", b"<h1>Notice</h1>")

        # A ClamAV outage must NOT be silently treated as clean.
        self.assertFalse(result["accepted"])
        self.assertEqual(result["verdict"], "UNKNOWN")
        self.assertEqual(result["clamav_status"], "ERROR")
        self.assertIn("clamav_unavailable", result["findings"])


if __name__ == "__main__":
    unittest.main()
