package artifact

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is a path-style object store that checks what a real one checks:
// a SigV4 Authorization header and the payload hash.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	headers map[string]http.Header
	puts    int
	down    bool
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=AKIATEST/20261110/eu-west-1/s3/aws4_request") ||
		!strings.Contains(auth, "SignedHeaders=") || !strings.Contains(auth, "x-amz-content-sha256") || r.Header.Get("x-amz-date") == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	key := r.URL.Path
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if hexSHA256(body) != r.Header.Get("x-amz-content-sha256") {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.objects[key], f.headers[key] = body, r.Header.Clone()
		f.puts++
	case http.MethodGet, http.MethodHead:
		b, ok := f.objects[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == http.MethodGet {
			_, _ = w.Write(b)
		}
	case http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	}
}

func newS3(t *testing.T) (*S3, *fakeS3) {
	f := &fakeS3{objects: map[string][]byte{}, headers: map[string]http.Header{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIATEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	s, err := NewS3(S3Config{Endpoint: srv.URL, Region: "eu-west-1", Bucket: "harness", Prefix: "prod", Encryption: "aws:kms",
		KMSKeyID: "alias/harness", RetentionClass: "incident",
		Now: func() time.Time { return time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	return s, f
}

// PR-20: content-addressed, tenant-scoped, encrypted, verified.
func TestS3StoreRoundTripIsolationAndIntegrity(t *testing.T) {
	s, f := newS3(t)
	ctx := context.Background()
	ref, hash, err := s.Put(ctx, "acme", "evidence", []byte("p99 is 2.9s"))
	if err != nil || !strings.HasPrefix(ref, "s3://acme/evidence/") || !strings.HasPrefix(hash, "sha256:") {
		t.Fatalf("put: %q %q %v", ref, hash, err)
	}
	key := "/harness/prod/" + strings.TrimPrefix(ref, "s3://")
	h := f.headers[key]
	if h.Get("x-amz-server-side-encryption") != "aws:kms" || h.Get("x-amz-server-side-encryption-aws-kms-key-id") != "alias/harness" ||
		!strings.Contains(h.Get("x-amz-tagging"), "retention=incident") {
		t.Fatalf("the object must be written encrypted and tagged: %v", h)
	}
	if _, _, err := s.Put(ctx, "acme", "evidence", []byte("p99 is 2.9s")); err != nil || f.puts != 1 {
		t.Fatalf("the same content must not be written twice: puts=%d %v", f.puts, err)
	}
	got, err := s.Get(ctx, "acme", ref)
	if err != nil || string(got) != "p99 is 2.9s" {
		t.Fatalf("get: %q %v", got, err)
	}
	if _, err := s.Get(ctx, "other", ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another tenant must not read the ref: %v", err)
	}
	if _, err := s.Get(ctx, "acme", "s3://acme/../other/evidence/x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("path traversal: %v", err)
	}
	// Tampering: the bytes no longer match the hash the ref names.
	f.mu.Lock()
	f.objects[key] = []byte("p99 is fine")
	f.mu.Unlock()
	if _, err := s.Get(ctx, "acme", ref); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a changed object must be refused: %v", err)
	}
	// Unavailable: the write fails, so no step can claim success without
	// its artifact; the error names no endpoint.
	f.mu.Lock()
	f.down = true
	f.mu.Unlock()
	if _, _, err := s.Put(ctx, "acme", "evidence", []byte("new")); err == nil || strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("put on an unavailable store: %v", err)
	}
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	if _, _, err := s.Put(ctx, "acme", "evidence", []byte("x")); err == nil {
		t.Fatal("a store without credentials must fail closed")
	}
}

// Refs written to the filesystem store stay readable after the move to S3.
func TestMuxKeepsOldRefsReadable(t *testing.T) {
	s, _ := newS3(t)
	ctx := context.Background()
	fs := &FS{Root: t.TempDir()}
	old, _, err := fs.Put(ctx, "acme", "result", []byte("before the migration"))
	if err != nil {
		t.Fatal(err)
	}
	m := &Mux{Primary: s, Readers: map[string]Store{"s3": s, "fs": fs}}
	got, err := m.Get(ctx, "acme", old)
	if err != nil || string(got) != "before the migration" {
		t.Fatalf("old ref: %q %v", got, err)
	}
	ref, _, err := m.Put(ctx, "acme", "result", []byte("after"))
	if err != nil || !strings.HasPrefix(ref, "s3://") {
		t.Fatalf("new artifacts go to the primary: %q %v", ref, err)
	}
	if got, err := m.Get(ctx, "acme", ref); err != nil || string(got) != "after" {
		t.Fatalf("new ref: %q %v", got, err)
	}
	if _, err := m.Get(ctx, "acme", "gs://acme/result/x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown scheme: %v", err)
	}
}
