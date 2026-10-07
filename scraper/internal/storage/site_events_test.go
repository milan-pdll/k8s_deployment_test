package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type fakeSiteEventWriter struct {
	messages []kafka.Message
}

func (w *fakeSiteEventWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	return nil
}

func (w *fakeSiteEventWriter) Close() error { return nil }

func TestKafkaSiteEventEmitterPublishesOneEventPerSite(t *testing.T) {
	writer := &fakeSiteEventWriter{}
	emitter := &KafkaSiteEventEmitter{w: writer, bucket: "crawled-pages", keyPrefix: "dev"}
	done := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	if err := emitter.EmitSiteCrawled(context.Background(), 7, "wf-1", "ward.gov.np", "completed", "", 12, done); err != nil {
		t.Fatalf("EmitSiteCrawled: %v", err)
	}
	if len(writer.messages) != 1 || string(writer.messages[0].Key) != "ward.gov.np" {
		t.Fatalf("messages = %+v, want one keyed by the site", writer.messages)
	}
	var ev SiteCrawledEvent
	if err := json.Unmarshal(writer.messages[0].Value, &ev); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	want := SiteCrawledEvent{
		EventType: SiteCrawledEventType, SchemaVersion: SiteCrawledSchemaVersion, CrawlRunID: 7, WorkflowID: "wf-1", TargetDomain: "ward.gov.np",
		Status: "completed", PagesFetched: 12, Bucket: "crawled-pages", KeyPrefix: "dev",
		DocumentsPrefix: "dev/7/ward.gov.np/", CompletedAt: done,
	}
	if ev != want {
		t.Fatalf("event = %+v\nwant    %+v", ev, want)
	}
	if !strings.Contains(string(writer.messages[0].Value), `"schema_version":1`) {
		t.Fatalf("event JSON lacks schema_version: %s", writer.messages[0].Value)
	}
}

func TestS3ObjectKeyIsUnderItsSitePrefix(t *testing.T) {
	key := S3ObjectKey(7, "https://Ward.gov.np/notices/1")
	if prefix := S3SiteDocumentsPrefix(7, "ward.gov.np"); !strings.HasPrefix(key, prefix) {
		t.Fatalf("S3ObjectKey = %q, want it under the site prefix %q", key, prefix)
	}
}
