package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"search-engine-scraper/internal/fetcher"
	"search-engine-scraper/internal/model"
	"search-engine-scraper/internal/parser"
)

type orderedObjectStore struct {
	events  *[]string
	objects []recordedObject
}

func (s *orderedObjectStore) PutObject(_ context.Context, bucket, key string, body io.Reader, _ int64, contentType string) error {
	*s.events = append(*s.events, "dfs")
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.objects = append(s.objects, recordedObject{bucket: bucket, key: key, contentType: contentType, data: data})
	return nil
}

func TestFetchExtractWriteAndSignalIntegration(t *testing.T) {
	html := `<html><head><title>Ward Office</title>
<meta name="description" content="Official ward office">
<meta name="keywords" content="ward, kathmandu">
</head><body><a href="/contact">Contact</a><address>Kathmandu, Nepal</address></body></html>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(html))
	}))
	defer srv.Close()

	ctx := context.Background()
	f := fetcher.NewWithOptions(fetcher.Options{Timeout: time.Second, DomainRequestsPerSecond: 20, DomainBurst: 1, AllowPrivateNetworks: true})
	result, err := f.Get(ctx, srv.URL)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	u, _ := url.Parse(srv.URL)
	extracted, err := parser.ParseForDomain(result.Body, result.FinalURL, result.ContentType, u.Hostname())
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	sum := sha256.Sum256(result.Body)
	doc := &model.Document{
		URL: result.FinalURL, Host: u.Hostname(), NormalizedURL: result.FinalURL,
		StatusCode: result.StatusCode, ContentType: result.ContentType,
		ContentHash: hex.EncodeToString(sum[:]), FetchedAt: time.Now().UTC(),
	}
	extracted.ApplyTo(doc)
	payload, err := BuildDFSPayload(doc, result.Body, "Ward Office")
	if err != nil {
		t.Fatalf("payload: %v", err)
	}

	events := []string{}
	store := &orderedObjectStore{events: &events}
	dfs, _ := NewMinIOWriter(store, "crawler", "raw")
	emitter := &fakeEmitter{events: &events}
	handoff, _ := NewHandoffWriter(dfs, emitter)
	stored, err := handoff.WritePage(ctx, payload)
	if err != nil {
		t.Fatalf("handoff: %v", err)
	}
	if len(events) != 2 || events[0] != "dfs" || events[1] != "kafka" {
		t.Fatalf("events = %v", events)
	}
	if len(store.objects) != 1 || len(emitter.signals) != 1 {
		t.Fatalf("objects=%d signals=%d", len(store.objects), len(emitter.signals))
	}
	if emitter.signals[0].ObjectKey != stored.Key || extracted.Title != "Ward Office" {
		t.Fatalf("stored=%+v signal=%+v extracted=%+v", stored, emitter.signals[0], extracted)
	}
}
