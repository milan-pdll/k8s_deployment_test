package activities

import (
	"context"
	"errors"
	"testing"

	"search-engine-scraper/internal/model"
)

type flakyWriter struct {
	calls   int
	failFor int
}

func (w *flakyWriter) Write(*model.Document) error {
	w.calls++
	if w.calls <= w.failFor {
		return errors.New("kafka down")
	}
	return nil
}
func (w *flakyWriter) Close() error { return nil }

// A failed write must not be remembered as written: Temporal's retry has to
// write again, and only after a success are duplicates no-ops.
func TestWriteDocument_RetriesAfterFailedWrite(t *testing.T) {
	w := &flakyWriter{failFor: 1}
	a := New(nil, nil, w, nil, nil)
	in := WriteDocumentInput{Doc: model.Document{NormalizedURL: "https://a.np/", ContentHash: "h"}}

	if err := a.WriteDocument(context.Background(), in); err == nil {
		t.Fatal("first write: want the writer's error")
	}
	if err := a.WriteDocument(context.Background(), in); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if w.calls != 2 {
		t.Fatalf("writer calls after retry = %d, want 2", w.calls)
	}
	if err := a.WriteDocument(context.Background(), in); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	if w.calls != 2 {
		t.Errorf("duplicate after success wrote again: calls = %d, want 2", w.calls)
	}
}
