import io
import json
import unittest
from unittest import mock

import site_pipeline
from site_pipeline import (
    DependencyUnavailable,
    ScannerUnavailable,
    finish_group,
    process_page,
    run_site_pipeline,
)

EVENT = {
    "event_type": "site_crawl_completed",
    "schema_version": 1,
    "crawl_run_id": 7,
    "target_domain": "ward.gov.np",
    "status": "completed",
    "bucket": "crawled-pages",
    "key_prefix": "dev",
    "documents_prefix": "dev/7/ward.gov.np/",
}

HTML = b"<html><body><h1>Ward Office</h1><p>Public notice for Kathmandu</p></body></html>"
DOCUMENT = {
    "url": "https://ward.gov.np/notice?utm_source=x",
    "normalized_url": "https://ward.gov.np/notice",
    "title": "Notice",
    "meta_description": "Ward notices",
    "meta_keywords": ["notice", "ward"],
    "category": "government",
    "html_key": "html/ward.gov.np/abc.html",
    "status_code": 200,
    "content_type": "text/html; charset=utf-8",
    "fetched_at": "2026-10-01T10:00:00Z",
    "open_graph": {"article:published_time": "2026-09-30T08:00:00Z"},
}


def client_error(code):
    from botocore.exceptions import ClientError

    return ClientError({"Error": {"Code": code, "Message": code}}, "GetObject")


class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.read = []
        self.put = {}

    def get_object(self, Bucket, Key):
        self.read.append(Key)
        if Key not in self.objects:
            raise client_error("NoSuchKey")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.put[Key] = Body
        self.objects[Key] = Body

    def get_paginator(self, name):
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket, Prefix):
                keys = sorted(k for k in objects if k.startswith(Prefix))
                return [{"Contents": [{"Key": k} for k in keys]}]

        return Paginator()


def scan(status="CLEAN", accepted=True, findings=None):
    return {
        "accepted": accepted,
        "clamav_status": status,
        "clamav_error": None if status != "ERROR" else "refused",
        "findings": findings if findings is not None else ([] if accepted else ["x"]),
        "verdict": "SAFE" if accepted else "INFECTED",
        "sha256": "0" * 64,
    }


class ProcessPageTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3(
            {
                "dev/7/ward.gov.np/1.json": json.dumps(DOCUMENT).encode(),
                "dev/html/ward.gov.np/abc.html": HTML,
            }
        )

    def page(self, key="dev/7/ward.gov.np/1.json"):
        return process_page(self.s3, EVENT, key)

    def test_scans_and_transforms_the_stored_html(self):
        with mock.patch("security_scanner.inspect_bytes", return_value=scan()) as inspect:
            result = self.page()
        inspect.assert_called_once_with("abc.html", HTML)
        self.assertEqual(result["status"], "transformed")
        record = result["record"]
        self.assertEqual(record["searchable_text"], "Ward Office Public notice for Kathmandu")
        self.assertEqual(record["source_url"], "https://ward.gov.np/notice")
        self.assertEqual(record["object_key"], "s3://crawled-pages/dev/html/ward.gov.np/abc.html")
        self.assertEqual(record["description"], "Ward notices")
        self.assertEqual(record["keywords"], ["notice", "ward"])
        self.assertEqual(record["category"], "government")
        self.assertEqual(record["published_at"], "2026-09-30T08:00:00+00:00")
        self.assertEqual((record["crawl_run_id"], record["target_domain"]), (7, "ward.gov.np"))
        self.assertEqual(result["document"]["normalized_url"], "https://ward.gov.np/notice")

    def test_rejected_page_is_not_transformed(self):
        with mock.patch("security_scanner.inspect_bytes", return_value=scan("INFECTED", False)):
            result = self.page()
        self.assertEqual(result["status"], "rejected")
        self.assertNotIn("record", result)

    def test_unreachable_clamav_fails_instead_of_rejecting(self):
        with (
            mock.patch("security_scanner.inspect_bytes", return_value=scan("ERROR", False)),
            self.assertRaises(ScannerUnavailable),
        ):
            self.page()

    def test_failed_fetches_are_skipped(self):
        self.s3.objects["dev/7/ward.gov.np/2.json"] = json.dumps(
            {**DOCUMENT, "status_code": 404, "error": "HTTP 404"}
        ).encode()
        self.assertEqual(self.page("dev/7/ward.gov.np/2.json")["status"], "skipped")

    def test_missing_or_corrupt_objects_fail_only_that_page(self):
        self.s3.objects["dev/7/ward.gov.np/3.json"] = b"{not json"
        self.s3.objects["dev/7/ward.gov.np/4.json"] = json.dumps(
            {**DOCUMENT, "html_key": "html/ward.gov.np/gone.html"}
        ).encode()
        self.assertEqual(self.page("dev/7/ward.gov.np/3.json")["status"], "failed")
        self.assertEqual(self.page("dev/7/ward.gov.np/4.json")["status"], "failed")
        self.assertEqual(self.page("dev/7/ward.gov.np/missing.json")["status"], "failed")

    def test_other_s3_errors_propagate(self):
        from botocore.exceptions import ClientError

        self.s3.get_object = mock.Mock(side_effect=client_error("SlowDown"))
        with self.assertRaises(ClientError):
            self.page()

    def test_html_is_decoded_with_its_declared_charset(self):
        latin = "<p>Café notice</p>".encode("latin-1")
        self.s3.objects["dev/html/ward.gov.np/abc.html"] = latin
        self.s3.objects["dev/7/ward.gov.np/1.json"] = json.dumps(
            {**DOCUMENT, "content_type": "text/html; charset=ISO-8859-1"}
        ).encode()
        with mock.patch("security_scanner.inspect_bytes", return_value=scan()):
            result = self.page()
        self.assertEqual(result["record"]["searchable_text"], "Café notice")

    def test_keys_from_the_event_cannot_escape_the_prefix(self):
        self.s3.objects["dev/7/ward.gov.np/5.json"] = json.dumps(
            {**DOCUMENT, "html_key": "../../secrets/x.html"}
        ).encode()
        with self.assertRaises(ValueError):
            self.page("dev/7/ward.gov.np/5.json")


class FakeWriter:
    def __init__(self, duplicate_urls=(), fail=None):
        self.saved = []
        self.errors = []
        self.rejections = []
        self.duplicate_urls = set(duplicate_urls)
        self.fail = fail
        self.checked = False

    def check(self):
        self.checked = True

    def save(self, record, document):
        if self.fail is not None:
            raise self.fail
        self.saved.append((record, document))
        return record["source_url"] in self.duplicate_urls

    def log_page_error(self, event, url, error, error_type):
        self.errors.append((url, error_type))

    def log_rejection(self, event, result):
        self.rejections.append(result["key"])


class FakeEmbedder:
    model_name = "sentence-transformers/LaBSE"

    def __init__(self):
        self.calls = []

    def embed(self, texts):
        self.calls.append(list(texts))
        return [[0.1] * 768 if text else None for text in texts]


def transformed(url):
    record = {"source_url": url, "searchable_text": f"text of {url}"}
    return {"status": "transformed", "key": url, "record": record, "document": {"url": url}}


class FinishGroupTests(unittest.TestCase):
    def test_embeds_once_per_group_and_saves_every_page(self):
        writer, embedder = FakeWriter(duplicate_urls={"b"}), FakeEmbedder()
        summary = site_pipeline._new_summary(EVENT, 5)
        results = [
            transformed("a"),
            transformed("b"),
            {"status": "skipped", "key": "c", "reason": "no text"},
            {"status": "failed", "key": "d", "error": "corrupt"},
            {
                "status": "rejected",
                "key": "e",
                "object_key": "s3://b/e",
                "source_url": "e",
                "security_scan": scan("INFECTED", False),
            },
        ]
        finish_group(results, EVENT, summary, writer, embedder)

        self.assertEqual(len(embedder.calls), 1)
        self.assertEqual([record["source_url"] for record, _ in writer.saved], ["a", "b"])
        record = writer.saved[0][0]
        self.assertEqual(record["embedding_model"], "sentence-transformers/LaBSE")
        self.assertEqual(record["embedding_dim"], 768)
        counts = ("saved", "duplicates", "skipped", "failed", "rejected", "infected")
        self.assertEqual(
            {k: summary[k] for k in counts},
            {"saved": 2, "duplicates": 1, "skipped": 1, "failed": 1, "rejected": 1, "infected": 1},
        )
        self.assertEqual(writer.rejections, ["e"])

    def test_a_page_with_bad_data_is_logged_and_skipped(self):
        writer = FakeWriter(fail=ValueError("simhash must be a 64-bit fingerprint"))
        summary = site_pipeline._new_summary(EVENT, 1)
        finish_group([transformed("a")], EVENT, summary, writer, FakeEmbedder())
        self.assertEqual(summary["failed"], 1)
        self.assertEqual(writer.errors, [("a", "ValueError")])

    def test_a_lost_database_connection_fails_the_run(self):
        from sqlalchemy.exc import OperationalError

        writer = FakeWriter(fail=OperationalError("SELECT 1", {}, Exception("server closed")))
        summary = site_pipeline._new_summary(EVENT, 1)
        with self.assertRaises(DependencyUnavailable):
            finish_group([transformed("a")], EVENT, summary, writer, FakeEmbedder())


class RunSiteTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3(
            {
                "dev/7/ward.gov.np/1.json": json.dumps(DOCUMENT).encode(),
                "dev/html/ward.gov.np/abc.html": HTML,
            }
        )

    def run_site(self, writer=None, **kwargs):
        def fake_group(spark, event, keys):
            return [process_page(self.s3, event, key) for key in keys]

        with (
            mock.patch("site_pipeline._run_group", side_effect=fake_group),
            mock.patch("security_scanner.inspect_bytes", return_value=scan()),
        ):
            return run_site_pipeline(
                EVENT,
                spark=None,
                writer=writer or FakeWriter(),
                embedder=FakeEmbedder(),
                s3=self.s3,
                check_scanner=False,
                **kwargs,
            )

    def test_runs_the_site_and_writes_its_marker(self):
        writer = FakeWriter()
        summary = self.run_site(writer)
        self.assertEqual((summary["documents"], summary["saved"]), (1, 1))
        self.assertTrue(writer.checked)
        marker = json.loads(self.s3.put["dev/_etl/7/ward.gov.np.json"])
        self.assertEqual(marker["pipeline_version"], site_pipeline.PIPELINE_VERSION)
        self.assertEqual(marker["summary"]["saved"], 1)

    def test_a_finished_site_is_skipped_on_rerun(self):
        self.run_site()
        writer = FakeWriter()
        summary = self.run_site(writer)
        self.assertTrue(summary["already_processed"])
        self.assertEqual(writer.saved, [])

    def test_a_marker_from_an_older_pipeline_version_is_ignored(self):
        self.s3.objects["dev/_etl/7/ward.gov.np.json"] = json.dumps(
            {"pipeline_version": 1, "summary": {}}
        ).encode()
        self.assertEqual(self.run_site()["saved"], 1)

    def test_scanner_outage_is_detected_before_any_work(self):
        with (
            mock.patch("security_scanner.ping_clamav", side_effect=ConnectionRefusedError()),
            self.assertRaises(ScannerUnavailable),
        ):
            run_site_pipeline(
                EVENT, spark=None, writer=FakeWriter(), embedder=FakeEmbedder(), s3=self.s3
            )

    def test_invalid_events_are_rejected(self):
        with self.assertRaises(ValueError):
            run_site_pipeline(
                {**EVENT, "bucket": ""},
                spark=None,
                writer=FakeWriter(),
                embedder=FakeEmbedder(),
                s3=self.s3,
            )


try:
    import pyspark  # noqa: F401

    HAVE_SPARK = True
except ImportError:
    HAVE_SPARK = False


@unittest.skipUnless(HAVE_SPARK, "pyspark is not installed")
class SparkGroupTests(unittest.TestCase):
    """_run_group on a real local Spark: the executor side ships and runs."""

    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.builder.master("local[2]").appName("etl-test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_pages_are_processed_in_spark_tasks(self):
        # Defined here, so Spark ships it to the executors by value.
        def fake_partition(event, keys):
            for key in keys:
                yield {"status": "skipped", "key": key, "reason": event["target_domain"]}

        results = site_pipeline._run_group(self.spark, EVENT, ["k1", "k2", "k3"], fake_partition)
        self.assertEqual(sorted(r["key"] for r in results), ["k1", "k2", "k3"])
        self.assertTrue(all(r["reason"] == "ward.gov.np" for r in results))


if __name__ == "__main__":
    unittest.main()
