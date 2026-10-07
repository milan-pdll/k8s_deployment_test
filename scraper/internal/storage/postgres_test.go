package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"search-engine-scraper/internal/model"
)

// TestCrawledDocumentParams_AppliesContractConversions checks the conversions of
// database/docs/scraper-db-contract.md §1.
func TestCrawledDocumentParams_AppliesContractConversions(t *testing.T) {
	doc := &model.Document{
		URL: "https://ward.gov.np/a", NormalizedURL: "https://ward.gov.np/a", Host: "ward.gov.np",
		ContentHash: "h", FetchedAt: time.Now(), SimHash: ^uint64(0),
		Geo:         &model.GeoPoint{Lat: 27.7, Lng: 85.3},
		ContactInfo: model.ContactInfo{Emails: []string{"ward@gov.np"}},
	}
	p, err := crawledDocumentParams(doc)
	if err != nil {
		t.Fatal(err)
	}
	if p.CrawlRunID.Valid {
		t.Error("CrawlRunID 0 must be NULL, not a foreign key to run 0")
	}
	if p.FinalUrl.Valid || p.Error.Valid || p.Country.Valid {
		t.Error(`empty strings must be NULL, not ""`)
	}
	if !p.SimHash.Valid || uint64(p.SimHash.Int64) != doc.SimHash {
		t.Errorf("SimHash = %+v, want the uint64 bits as int64", p.SimHash)
	}
	if !p.GeoLat.Valid || p.GeoLng.Float64 != 85.3 || len(p.Emails) != 1 {
		t.Errorf("Geo/ContactInfo not flattened: %+v %+v %v", p.GeoLat, p.GeoLng, p.Emails)
	}
	if p.Headings != nil || p.OpenGraph != nil {
		t.Error("absent headings / open graph must be NULL")
	}
}

// TestPostgresWriter_AgainstMigratedSchema runs the real statements against a
// database migrated by database/ (Alembic): it proves the generated models and the
// queries still match the source of truth. Skipped unless
// PGS_TEST_DATABASE_URL is set, e.g.
// postgres://pgs:pgs@localhost:5432/pgs?sslmode=disable
func TestPostgresWriter_AgainstMigratedSchema(t *testing.T) {
	url := os.Getenv("PGS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("PGS_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	w, err := NewPostgresWriter(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }() // test cleanup; the assertions ran already

	runID, err := w.StartRun(ctx, StartRunInput{SeedCount: 1, MaxDepth: 2, MaxPages: 10})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	doc := &model.Document{
		URL: "https://go-test.example/" + time.Now().Format(time.RFC3339Nano), Host: "go-test.example",
		Title: "Go test", Text: "body", Links: []string{"https://go-test.example/b"},
		AnchorTexts: []string{"b"}, Headings: []model.Heading{{Level: 1, Text: "H"}},
		ContentHash: "go-test-hash", StatusCode: 200, ContentType: "text/html",
		FetchedAt: time.Now().UTC(), CrawlRunID: runID, SimHash: 1 << 63,
	}
	doc.NormalizedURL = doc.URL
	for i := 0; i < 2; i++ { // the second write is the retried activity: same row
		if err := w.Write(doc); err != nil {
			t.Fatalf("Write #%d: %v", i+1, err)
		}
	}
	params, _ := crawledDocumentParams(doc)
	row, err := w.queries.UpsertCrawledDocument(ctx, params)
	if err != nil || row.Inserted {
		t.Fatalf("third upsert = %+v, %v; want the existing row (inserted=false)", row, err)
	}
	fresh, err := w.FreshURLs(ctx, []string{doc.NormalizedURL}, time.Now().Add(-time.Hour))
	if err != nil || !fresh[doc.NormalizedURL] {
		t.Fatalf("FreshURLs = %v, %v; want the URL fresh", fresh, err)
	}
	if err := w.UpdateRunStats(ctx, runID, RunStats{Fetched: 1, Succeeded: 1}); err != nil {
		t.Fatalf("UpdateRunStats: %v", err)
	}
	if err := w.FinishRun(ctx, runID, "completed", RunStats{Fetched: 1, Succeeded: 1}, ""); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	run, err := w.queries.GetCrawlRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetCrawlRun: %v", err)
	}
	if run.Status != "COMPLETED" || run.FetchedCount != 1 || run.Error.Valid || !run.FinishedAt.Valid {
		t.Errorf("run = %+v, want COMPLETED with 1 fetched, no error, finished", run)
	}

	// A page on www.<domain> belongs to the seeded domain (stored without www.), and
	// through it to its local body: pokharamun.gov.np is Pokhara (database/data/domains.json).
	page := *doc
	page.URL = "https://www.pokharamun.gov.np/go-test-" + time.Now().Format(time.RFC3339Nano)
	page.NormalizedURL, page.Host = page.URL, "www.pokharamun.gov.np"
	if err := w.Write(&page); err != nil {
		t.Fatalf("Write www page: %v", err)
	}
	var domain, localBody string
	err = w.pool.QueryRow(ctx, `SELECT d.domain, coalesce(lb.code, '') FROM crawled_documents c
		JOIN domains d ON d.id = c.domain_id LEFT JOIN local_bodies lb ON lb.id = d.local_body_id
		WHERE c.normalized_url = $1`, page.NormalizedURL).Scan(&domain, &localBody)
	if err != nil || domain != "pokharamun.gov.np" || localBody != "MUN414" {
		t.Errorf("www page -> domain %q, local body %q, %v; want pokharamun.gov.np / MUN414", domain, localBody, err)
	}
}
