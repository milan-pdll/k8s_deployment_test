package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// SiteCrawledEventType is the event_type of every SiteCrawledEvent.
const SiteCrawledEventType = "site_crawl_completed"

// SiteCrawledSchemaVersion is the version of the SiteCrawledEvent JSON. Bump it
// only for a change consumers must know about (a removed or retyped field);
// consumers treat a missing schema_version as 1 and skip versions they don't
// know (ETL/spark/site_event.py).
const SiteCrawledSchemaVersion = 1

// SiteCrawledEvent is the scraper -> ETL hand-off: one message per website
// (host) whose crawl has finished, published to Kafka after every page of
// that site is already in the bucket. The ETL (the etl_ingestion_pipeline
// Airflow DAG) then processes the whole site: it lists DocumentsPrefix, and
// each Document JSON there names its raw HTML (html_key, relative to
// KeyPrefix). There is no per-page or per-file message.
type SiteCrawledEvent struct {
	EventType     string `json:"event_type"`
	SchemaVersion int    `json:"schema_version"`
	CrawlRunID    int64  `json:"crawl_run_id"`
	WorkflowID    string `json:"workflow_id"`
	TargetDomain  string `json:"target_domain"`
	// Status is "completed", or "failed" when the crawl ended with an error;
	// the pages it did store are still under DocumentsPrefix.
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	// PagesFetched counts the site's fetch attempts (not only stored pages).
	PagesFetched int    `json:"pages_fetched"`
	Bucket       string `json:"bucket"`
	// KeyPrefix is the bucket-wide key prefix (--s3-prefix, may be empty);
	// html_key/page_record_key in a Document are relative to it.
	KeyPrefix string `json:"key_prefix"`
	// DocumentsPrefix holds this site's Document JSON for this run:
	// "<key_prefix>/<crawl_run_id>/<target_domain>/" (see S3ObjectKey).
	DocumentsPrefix string    `json:"documents_prefix"`
	CompletedAt     time.Time `json:"completed_at"`
}

// SiteEventEmitter publishes SiteCrawledEvents.
type SiteEventEmitter interface {
	EmitSiteCrawled(ctx context.Context, runID int64, workflowID, host, status, errMsg string, pagesFetched int, completedAt time.Time) error
}

// NoopSiteEventEmitter is used when no Kafka brokers are configured.
type NoopSiteEventEmitter struct{}

func (NoopSiteEventEmitter) EmitSiteCrawled(context.Context, int64, string, string, string, string, int, time.Time) error {
	return nil
}

type siteEventWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// KafkaSiteEventEmitter publishes SiteCrawledEvents for documents stored in
// one S3 bucket. Messages are keyed by target_domain, so every event for a
// site lands in the same partition, in order.
type KafkaSiteEventEmitter struct {
	w         siteEventWriter
	bucket    string
	keyPrefix string
}

// NewKafkaSiteEventEmitter returns an emitter for topic on brokers, for
// documents the S3 writer stores in bucket under keyPrefix (--s3-prefix).
func NewKafkaSiteEventEmitter(brokers []string, topic, bucket, keyPrefix string) (*KafkaSiteEventEmitter, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker address is required")
	}
	if topic == "" {
		return nil, fmt.Errorf("kafka: topic is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("kafka: site events need the S3 bucket the documents are stored in")
	}
	return &KafkaSiteEventEmitter{
		w: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
			// Acknowledged by every in-sync replica before the activity counts
			// the site as handed off: an event lost to a broker failover would
			// leave a crawled site unprocessed with nothing to retry it.
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
			// The broker creates the topic on the first event (no topic-init step).
			AllowAutoTopicCreation: true,
			Async:                  false, // block until acked, so the activity's retries see delivery failures
		},
		bucket:    bucket,
		keyPrefix: strings.Trim(keyPrefix, "/"),
	}, nil
}

// EmitSiteCrawled implements SiteEventEmitter.
func (e *KafkaSiteEventEmitter) EmitSiteCrawled(ctx context.Context, runID int64, workflowID, host, status, errMsg string, pagesFetched int, completedAt time.Time) error {
	if host == "" {
		return fmt.Errorf("site event requires a host")
	}
	docsPrefix := S3SiteDocumentsPrefix(runID, host)
	if e.keyPrefix != "" {
		docsPrefix = e.keyPrefix + "/" + docsPrefix
	}
	data, err := json.Marshal(SiteCrawledEvent{
		EventType:       SiteCrawledEventType,
		SchemaVersion:   SiteCrawledSchemaVersion,
		CrawlRunID:      runID,
		WorkflowID:      workflowID,
		TargetDomain:    host,
		Status:          status,
		Error:           errMsg,
		PagesFetched:    pagesFetched,
		Bucket:          e.bucket,
		KeyPrefix:       e.keyPrefix,
		DocumentsPrefix: docsPrefix,
		CompletedAt:     completedAt.UTC(),
	})
	if err != nil {
		return fmt.Errorf("marshal site event: %w", err)
	}
	if err := e.w.WriteMessages(ctx, kafka.Message{Key: []byte(host), Value: data}); err != nil {
		return fmt.Errorf("publish site event for %s (run %d): %w", host, runID, err)
	}
	return nil
}

// Close flushes and closes the underlying Kafka writer.
func (e *KafkaSiteEventEmitter) Close() error { return e.w.Close() }
