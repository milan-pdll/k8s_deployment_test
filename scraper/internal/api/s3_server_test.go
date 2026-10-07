package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"search-engine-scraper/internal/model"
	"search-engine-scraper/internal/storage"
)

// fakeS3ReadAPI is an in-memory object store (key -> body) that
// ListObjectsV2/GetObject/HeadBucket work against, so S3Server's routing
// and pagination logic can be tested end-to-end (via real net/http/httptest
// handling) without LocalStack. maxKeysPerPage, when > 0, forces multi-page
// listings so continuation-token handling is actually exercised rather
// than only ever hit on a single-page happy path.
type fakeS3ReadAPI struct {
	objects        map[string][]byte
	maxKeysPerPage int
	headBucketErr  error
	listErr        error
	getErrOnKey    map[string]error
}

func newFakeS3ReadAPI() *fakeS3ReadAPI {
	return &fakeS3ReadAPI{objects: map[string][]byte{}, getErrOnKey: map[string]error{}}
}

func (f *fakeS3ReadAPI) put(key string, doc *model.Document) {
	body, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	f.objects[key] = body
}

func (f *fakeS3ReadAPI) putManifest(key string, m storage.S3RunManifest) {
	body, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	f.objects[key] = body
}

func (f *fakeS3ReadAPI) HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if f.headBucketErr != nil {
		return nil, f.headBucketErr
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeS3ReadAPI) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	if err, ok := f.getErrOnKey[key]; ok {
		return nil, err
	}
	body, ok := f.objects[key]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "key does not exist"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
}

// ListObjectsV2 supports exactly what S3Server needs: an optional Prefix,
// an optional Delimiter ("/", for the crawl_run_id-enumeration path), and
// ContinuationToken-based pagination -- both encoded as a plain integer
// offset into the (always deterministically sorted) matching-key list, not
// a real S3 opaque token, since nothing here needs to decode one.
func (f *fakeS3ReadAPI) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	prefix := aws.ToString(in.Prefix)
	delim := aws.ToString(in.Delimiter)

	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	var items []string // either full keys, or common-prefix strings
	if delim == "" {
		items = keys
	} else {
		seen := map[string]bool{}
		for _, k := range keys {
			rest := strings.TrimPrefix(k, prefix)
			idx := strings.Index(rest, delim)
			if idx == -1 {
				continue
			}
			cp := prefix + rest[:idx+1]
			if !seen[cp] {
				seen[cp] = true
				items = append(items, cp)
			}
		}
		sort.Strings(items)
	}

	start := 0
	if in.ContinuationToken != nil {
		start, _ = strconv.Atoi(aws.ToString(in.ContinuationToken))
	}
	end := len(items)
	if f.maxKeysPerPage > 0 && start+f.maxKeysPerPage < end {
		end = start + f.maxKeysPerPage
	}
	if start > len(items) {
		start = len(items)
	}
	page := items[start:end]

	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(end < len(items))}
	if end < len(items) {
		out.NextContinuationToken = aws.String(strconv.Itoa(end))
	}
	if delim == "" {
		for _, k := range page {
			out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
		}
	} else {
		for _, cp := range page {
			out.CommonPrefixes = append(out.CommonPrefixes, s3types.CommonPrefix{Prefix: aws.String(cp)})
		}
	}
	return out, nil
}

func doGet(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	srv := httptest.NewServer(handler)
	defer srv.Close()

	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestS3Server_HandleListDocuments_RequiresCategory(t *testing.T) {
	fake := newFakeS3ReadAPI()
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?crawl_run_id=1")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestS3Server_HandleListDocuments_RequiresCrawlRunID(t *testing.T) {
	fake := newFakeS3ReadAPI()
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?category=tech")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "crawl_run_id") {
		t.Errorf("error message = %q, want it to explain crawl_run_id is required", rec.Body.String())
	}
}

func TestS3Server_HandleListDocuments_FiltersByCategoryAndCountry(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.put(storage.S3ObjectKey(1, "https://a.example/tech-np"), &model.Document{
		URL: "https://a.example/tech-np", NormalizedURL: "https://a.example/tech-np", Category: "tech", Country: "NP",
	})
	fake.put(storage.S3ObjectKey(1, "https://a.example/tech-in"), &model.Document{
		URL: "https://a.example/tech-in", NormalizedURL: "https://a.example/tech-in", Category: "tech", Country: "IN",
	})
	fake.put(storage.S3ObjectKey(1, "https://a.example/news-np"), &model.Document{
		URL: "https://a.example/news-np", NormalizedURL: "https://a.example/news-np", Category: "news", Country: "NP",
	})
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?category=tech&crawl_run_id=1&country=np")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Count int              `json:"count"`
		Items []model.Document `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Count != 1 || len(resp.Items) != 1 {
		t.Fatalf("count = %d, items = %d, want 1 (only the tech+NP document)", resp.Count, len(resp.Items))
	}
	if resp.Items[0].URL != "https://a.example/tech-np" {
		t.Errorf("item = %+v, want the tech-np document", resp.Items[0])
	}
}

func TestS3Server_HandleListDocuments_ExcludesOtherRuns(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.put(storage.S3ObjectKey(1, "https://a.example/run1"), &model.Document{URL: "https://a.example/run1", NormalizedURL: "https://a.example/run1", Category: "tech"})
	fake.put(storage.S3ObjectKey(2, "https://a.example/run2"), &model.Document{URL: "https://a.example/run2", NormalizedURL: "https://a.example/run2", Category: "tech"})
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?category=tech&crawl_run_id=1")
	var resp struct {
		Items []model.Document `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].URL != "https://a.example/run1" {
		t.Errorf("items = %+v, want only run1's document", resp.Items)
	}
}

func TestS3Server_HandleListDocuments_ExcludesRunManifest(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.put(storage.S3ObjectKey(1, "https://a.example/x"), &model.Document{URL: "https://a.example/x", NormalizedURL: "https://a.example/x", Category: "tech"})
	fake.putManifest(storage.S3RunManifestKey(1), storage.S3RunManifest{RunID: 1, Status: "running"})
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?category=tech&crawl_run_id=1")
	var resp struct {
		Items []model.Document `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items = %+v, want exactly 1 (the manifest object must not be unmarshalled as a Document)", resp.Items)
	}
}

func TestS3Server_HandleListDocuments_PaginatesAndHandlesContinuationTokens(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.maxKeysPerPage = 2 // forces ListObjectsV2 to paginate across several calls
	for i := 0; i < 5; i++ {
		url := "https://a.example/" + strconv.Itoa(i)
		fake.put(storage.S3ObjectKey(1, url), &model.Document{URL: url, NormalizedURL: url, Category: "tech"})
	}
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?category=tech&crawl_run_id=1&limit=2&offset=0")
	var page1 struct {
		Count int              `json:"count"`
		Items []model.Document `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page1); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if page1.Count != 2 {
		t.Fatalf("page1 count = %d, want 2 (limit=2, but all 5 underlying objects must have been listed across multiple ListObjectsV2 pages first)", page1.Count)
	}

	rec2 := doGet(t, s.Routes(), "/api/v1/documents?category=tech&crawl_run_id=1&limit=2&offset=4")
	var page2 struct {
		Count int              `json:"count"`
		Items []model.Document `json:"items"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &page2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if page2.Count != 1 {
		t.Fatalf("page2 (offset=4) count = %d, want 1 (only the 5th document remains)", page2.Count)
	}
}

func TestS3Server_HandleListDocuments_ListErrorReturns500(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.listErr = errors.New("throttled")
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/documents?category=tech&crawl_run_id=1")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestS3Server_HandleListCrawlRuns_SkipsLatestPrefixAndSortsNewestFirst(t *testing.T) {
	fake := newFakeS3ReadAPI()
	now := time.Now()
	fake.putManifest(storage.S3RunManifestKey(1), storage.S3RunManifest{RunID: 1, Status: "completed", StartedAt: now.Add(-2 * time.Hour)})
	fake.putManifest(storage.S3RunManifestKey(2), storage.S3RunManifest{RunID: 2, Status: "completed", StartedAt: now.Add(-1 * time.Hour)})
	fake.put(storage.S3LatestObjectKey("https://a.example/x"), &model.Document{URL: "https://a.example/x"}) // lives under latest/, must not be treated as a run

	s := NewS3Server(fake, "docs-bucket")
	rec := doGet(t, s.Routes(), "/api/v1/crawl-runs")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Count int                `json:"count"`
		Items []crawlRunResponse `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 2 {
		t.Fatalf("count = %d, want 2 (latest/ must not be counted as a run)", resp.Count)
	}
	if resp.Items[0].ID != 2 {
		t.Errorf("items[0].ID = %d, want 2 (newest-started-first)", resp.Items[0].ID)
	}
}

func TestS3Server_HandleListCrawlRuns_TransientErrorReturns500NotEmptyList(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.putManifest(storage.S3RunManifestKey(1), storage.S3RunManifest{RunID: 1, Status: "running"})
	fake.getErrOnKey[storage.S3RunManifestKey(1)] = errors.New("throttled")

	s := NewS3Server(fake, "docs-bucket")
	rec := doGet(t, s.Routes(), "/api/v1/crawl-runs")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (a transient GetObject error must not be swallowed as an empty list)", rec.Code)
	}
}

func TestS3Server_HandleListCrawlRuns_MissingManifestIsSilentlySkipped(t *testing.T) {
	fake := newFakeS3ReadAPI()
	// A document exists under run 1's prefix, but no _run.json -- a run
	// whose StartCrawlRun never completed.
	fake.put(storage.S3ObjectKey(1, "https://a.example/x"), &model.Document{URL: "https://a.example/x"})

	s := NewS3Server(fake, "docs-bucket")
	rec := doGet(t, s.Routes(), "/api/v1/crawl-runs")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (a missing manifest should be skipped, not fail the request)", rec.Code)
	}
	var resp struct {
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 0 {
		t.Errorf("count = %d, want 0", resp.Count)
	}
}

func TestS3Server_HandleGetCrawlRun_Found(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.putManifest(storage.S3RunManifestKey(42), storage.S3RunManifest{RunID: 42, Status: "completed", Fetched: 10, Succeeded: 9, Failed: 1})

	s := NewS3Server(fake, "docs-bucket")
	rec := doGet(t, s.Routes(), "/api/v1/crawl-runs/42")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got crawlRunResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != 42 || got.Fetched != 10 {
		t.Errorf("got = %+v, want ID=42 Fetched=10", got)
	}
}

func TestS3Server_HandleGetCrawlRun_NotFound(t *testing.T) {
	fake := newFakeS3ReadAPI()
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/api/v1/crawl-runs/999")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestS3Server_HandleGetCrawlRun_FinishedAtSetOnlyForTerminalStatus(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.putManifest(storage.S3RunManifestKey(1), storage.S3RunManifest{RunID: 1, Status: "running"})
	fake.putManifest(storage.S3RunManifestKey(2), storage.S3RunManifest{RunID: 2, Status: "completed"})

	s := NewS3Server(fake, "docs-bucket")

	rec1 := doGet(t, s.Routes(), "/api/v1/crawl-runs/1")
	var run1 crawlRunResponse
	if err := json.Unmarshal(rec1.Body.Bytes(), &run1); err != nil {
		t.Fatalf("decode run1: %v", err)
	}
	if run1.FinishedAt != nil {
		t.Errorf("run1 (status=running) FinishedAt = %v, want nil", run1.FinishedAt)
	}

	rec2 := doGet(t, s.Routes(), "/api/v1/crawl-runs/2")
	var run2 crawlRunResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &run2); err != nil {
		t.Fatalf("decode run2: %v", err)
	}
	if run2.FinishedAt == nil {
		t.Error("run2 (status=completed) FinishedAt = nil, want non-nil")
	}
}

func TestS3Server_HandleHealthz_ReportsBucketReachability(t *testing.T) {
	fake := newFakeS3ReadAPI()
	s := NewS3Server(fake, "docs-bucket")

	rec := doGet(t, s.Routes(), "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	fake.headBucketErr = errors.New("bucket unreachable")
	rec2 := doGet(t, s.Routes(), "/healthz")
	if rec2.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 once HeadBucket fails", rec2.Code)
	}
}

func TestS3Server_AggregateEndpoints_Return501(t *testing.T) {
	fake := newFakeS3ReadAPI()
	s := NewS3Server(fake, "docs-bucket")

	for _, path := range []string{"/api/v1/documents/count", "/api/v1/documents/categories"} {
		rec := doGet(t, s.Routes(), path)
		if rec.Code != http.StatusNotImplemented {
			t.Errorf("%s status = %d, want 501", path, rec.Code)
		}
	}
}

func TestS3Server_KeyPrefixIsRespected(t *testing.T) {
	fake := newFakeS3ReadAPI()
	fake.putManifest("staging/"+storage.S3RunManifestKey(1), storage.S3RunManifest{RunID: 1, Status: "completed"})

	s := NewS3Server(fake, "docs-bucket", WithS3ServerKeyPrefix("staging"))
	rec := doGet(t, s.Routes(), "/api/v1/crawl-runs/1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (should look under the staging/ prefix)", rec.Code)
	}
}
