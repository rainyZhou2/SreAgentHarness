package memory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestTokenizeCJK(t *testing.T) {
	got := Tokenize("数据库连接池耗尽 db-pool timeout")
	want := map[string]bool{"数据": true, "据库": true, "连接": true, "池耗": true, "耗尽": true, "timeout": true}
	have := map[string]bool{}
	for _, g := range got {
		have[g] = true
	}
	for w := range want {
		if !have[w] {
			t.Fatalf("missing %q in %v", w, got)
		}
	}
	if g := Tokenize("库"); len(g) != 1 || g[0] != "库" {
		t.Fatalf("single char: %v", g)
	}
	if terms := SearchTerms("连接池", "连接池 ok"); terms != "连接 接池" {
		t.Fatalf("terms: %q", terms)
	}
	if SearchTerms("plain english only") != "" {
		t.Fatal("no CJK, no terms")
	}
	e := HashingEmbedder{Dims: 64}
	if Cosine(e.Embed("数据库连接池耗尽"), e.Embed("连接池耗尽了")) < 0.3 {
		t.Fatal("hashing embedder must see shared CJK bigrams")
	}
}

func TestHTTPEmbedder(t *testing.T) {
	var calls, fail atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "bad", 400)
			return
		}
		if fail.Load() == 1 {
			http.Error(w, "down", 503)
			return
		}
		var in struct {
			Model, Input string
			Dimensions   int
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Model != "m" || in.Dimensions != 3 {
			http.Error(w, "bad body", 400)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.1,0.2,0.3]}]}`))
	}))
	defer srv.Close()
	e := &HTTPEmbedder{BaseURL: srv.URL + "/v1/", ModelName: "m", APIKey: "k", Dimensions: 3}
	if e.Model() != "http:m-3" {
		t.Fatal(e.Model())
	}
	if v := e.Embed("hello"); len(v) != 3 {
		t.Fatalf("embed: %v", v)
	}
	e.Embed("hello")
	if calls.Load() != 1 {
		t.Fatal("repeated text must be cached")
	}
	fail.Store(1)
	if v := e.Embed("other"); v != nil {
		t.Fatal("failure must return nil")
	}
	e.Dimensions = 4
	fail.Store(0)
	if v := e.Embed("third"); v != nil {
		t.Fatal("dimension mismatch must return nil")
	}
	env := map[string]string{"HARNESS_EMBEDDING_BASE_URL": "http://x", "HARNESS_EMBEDDING_MODEL": "m",
		"HARNESS_EMBEDDING_API_KEY_ENV": "MY_KEY", "MY_KEY": "s", "HARNESS_EMBEDDING_DIMENSIONS": "8"}
	got := EmbedderFromEnv(func(k string) string { return env[k] }, nil)
	if got == nil || got.APIKey != "s" || got.Dimensions != 8 {
		t.Fatalf("from env: %+v", got)
	}
	if EmbedderFromEnv(func(string) string { return "" }, nil) != nil {
		t.Fatal("unset env must give nil")
	}
}

type fakeIndex struct {
	items []IndexItem
	put   map[string]string
}

func (f *fakeIndex) MemoriesToIndex(_ context.Context, model string, limit int) ([]IndexItem, error) {
	return f.items, nil
}
func (f *fakeIndex) PutMemoryIndex(_ context.Context, it IndexItem, model string, emb []float32, terms string) error {
	f.put[it.ID] = model + "|" + terms + "|" + strings.Repeat("x", len(emb))
	return nil
}

type nilEmbedder struct{}

func (nilEmbedder) Model() string          { return "dead" }
func (nilEmbedder) Embed(string) []float32 { return nil }

func TestIndexer(t *testing.T) {
	f := &fakeIndex{put: map[string]string{}, items: []IndexItem{
		{ID: "a", Title: "连接池", NeedTerms: true, NeedEmbedding: true},
		{ID: "b", Title: "x", NeedEmbedding: true},
	}}
	n, err := (&Indexer{Store: f, Embedder: HashingEmbedder{Dims: 4}}).Tick(context.Background())
	if err != nil || n != 2 || f.put["a"] != "hash-uni-bigram-v2-4|连接 接池|xxxx" {
		t.Fatalf("%d %v %v", n, err, f.put)
	}
	f.put = map[string]string{}
	n, _ = (&Indexer{Store: f, Embedder: nilEmbedder{}}).Tick(context.Background())
	if n != 1 || f.put["a"] != "dead|连接 接池|" {
		t.Fatalf("failing embedder: still store terms, stop on embedding-only: %d %v", n, f.put)
	}
	if n, err := (*Indexer)(nil).Tick(context.Background()); n != 0 || err != nil {
		t.Fatal("nil indexer")
	}
}
