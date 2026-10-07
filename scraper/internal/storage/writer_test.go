package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"search-engine-scraper/internal/model"
)

// TestNDJSONWriter_WriteAppendsOneJSONLinePerDocument proves the basic
// contract: each Write call appends exactly one JSON line, and the file
// round-trips back to the documents written. This is the shape
// README.md's "Database" section and docs/RESILIENCE.md's crash-recovery
// claim both depend on -- verified here since no test previously covered
// NDJSONWriter at all. Person 4 checklist item 10: this writer must keep
// working unchanged now that S3 exists as an additional backend.
func TestNDJSONWriter_WriteAppendsOneJSONLinePerDocument(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "documents.ndjson")

	w, err := NewNDJSONWriter(path)
	if err != nil {
		t.Fatalf("NewNDJSONWriter: %v", err)
	}

	docs := []*model.Document{
		{URL: "https://example.com/a", NormalizedURL: "https://example.com/a", Title: "A"},
		{URL: "https://example.com/b", NormalizedURL: "https://example.com/b", Title: "B"},
	}
	for _, d := range docs {
		if err := w.Write(d); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer f.Close()

	var got []model.Document
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var d model.Document
		if err := json.Unmarshal(scanner.Bytes(), &d); err != nil {
			t.Fatalf("unmarshal line %q: %v", scanner.Text(), err)
		}
		got = append(got, d)
	}
	if len(got) != len(docs) {
		t.Fatalf("read %d lines, want %d", len(got), len(docs))
	}
	for i, d := range docs {
		if got[i].URL != d.URL || got[i].Title != d.Title {
			t.Errorf("line %d = %+v, want URL/Title matching %+v", i, got[i], d)
		}
	}
}

// TestNDJSONWriter_ReopenAppendsWithoutTruncating proves the specific
// behavior docs/RESILIENCE.md's crash-recovery claim depends on: opening
// an existing output file again (as happens when a crashed worker process
// restarts with the same --output path) appends to it rather than
// truncating it. NewNDJSONWriter's doc comment already states this is
// deliberate (os.O_APPEND, not os.O_TRUNC/os.O_CREATE-only); this is the
// regression test that would catch it if that ever silently changed.
func TestNDJSONWriter_ReopenAppendsWithoutTruncating(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "documents.ndjson")

	w1, err := NewNDJSONWriter(path)
	if err != nil {
		t.Fatalf("NewNDJSONWriter (first process): %v", err)
	}
	if err := w1.Write(&model.Document{URL: "https://example.com/pre-crash", NormalizedURL: "https://example.com/pre-crash"}); err != nil {
		t.Fatalf("Write (pre-crash): %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close (first process): %v", err)
	}

	// Simulates the worker restart in docs/RESILIENCE.md: same path, a
	// brand new NDJSONWriter, no cleanup of the existing file.
	w2, err := NewNDJSONWriter(path)
	if err != nil {
		t.Fatalf("NewNDJSONWriter (restarted process): %v", err)
	}
	if err := w2.Write(&model.Document{URL: "https://example.com/post-restart", NormalizedURL: "https://example.com/post-restart"}); err != nil {
		t.Fatalf("Write (post-restart): %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close (restarted process): %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	var lines []model.Document
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		var d model.Document
		if err := json.Unmarshal(scanner.Bytes(), &d); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		lines = append(lines, d)
	}

	if len(lines) != 2 {
		t.Fatalf("got %d lines after reopen, want 2 (pre-crash document must survive)", len(lines))
	}
	if lines[0].URL != "https://example.com/pre-crash" {
		t.Errorf("first line URL = %q, want the pre-crash document (it must not have been truncated away)", lines[0].URL)
	}
	if lines[1].URL != "https://example.com/post-restart" {
		t.Errorf("second line URL = %q, want the post-restart document", lines[1].URL)
	}
}

// TestNoopFreshnessChecker_And_NoopRunRecorder_AreWhatNDJSONUses documents
// (as a compile-time + behavioral check, not just a comment) that
// --storage=ndjson has no freshness/listing support: NDJSONWriter itself
// implements neither FreshnessChecker nor RunRecorder, so cmd/worker's
// runRecorder/freshnessChecker helpers fall back to the Noop
// implementations for it -- same limitation as before this branch, now
// against S3 as the alternative instead of Postgres.
func TestNoopFreshnessChecker_And_NoopRunRecorder_AreWhatNDJSONUses(t *testing.T) {
	dir := t.TempDir()
	w, err := NewNDJSONWriter(filepath.Join(dir, "documents.ndjson"))
	if err != nil {
		t.Fatalf("NewNDJSONWriter: %v", err)
	}
	defer func() { _ = w.Close() }() // test cleanup; the assertions ran already

	if _, ok := interface{}(w).(FreshnessChecker); ok {
		t.Error("NDJSONWriter unexpectedly implements FreshnessChecker -- cmd/worker's fallback-to-Noop logic would silently stop applying")
	}
	if _, ok := interface{}(w).(RunRecorder); ok {
		t.Error("NDJSONWriter unexpectedly implements RunRecorder -- cmd/worker's fallback-to-Noop logic would silently stop applying")
	}
}
