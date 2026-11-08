package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// HTTPEmbedder calls an OpenAI-compatible embeddings endpoint
// (POST {base}/embeddings with {"model","input"[,"dimensions"]}), which
// OpenAI, Azure OpenAI, vLLM, Ollama, LiteLLM and most gateways serve. The
// Embedder interface has no error: on failure Embed returns nil, retrieval
// then runs without its vector branch, and the indexer retries later.
type HTTPEmbedder struct {
	BaseURL    string // e.g. https://api.openai.com/v1
	ModelName  string
	APIKey     string
	Dimensions int // optional; sent as "dimensions" when > 0
	Timeout    time.Duration
	MaxChars   int // input is cut to this many characters (default 8000)
	Client     *http.Client
	Log        *slog.Logger

	mu    sync.Mutex
	cache map[string][]float32 // recent texts (queries repeat across steps)
	order []string
}

const httpEmbedCacheSize = 256

// Model names the embedding space: provider model and dimensions.
func (h *HTTPEmbedder) Model() string {
	m := "http:" + h.ModelName
	if h.Dimensions > 0 {
		m += fmt.Sprintf("-%d", h.Dimensions)
	}
	return m
}

func (h *HTTPEmbedder) Embed(text string) []float32 {
	max := h.MaxChars
	if max <= 0 {
		max = 8000
	}
	if utf8.RuneCountInString(text) > max {
		text = string([]rune(text)[:max])
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}
	h.mu.Lock()
	if v, ok := h.cache[text]; ok {
		h.mu.Unlock()
		return v
	}
	h.mu.Unlock()
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	v, err := h.call(ctx, text)
	if err != nil {
		if h.Log != nil {
			h.Log.Warn("embedding request failed", "model", h.ModelName, "err", err)
		}
		return nil
	}
	h.mu.Lock()
	if h.cache == nil {
		h.cache = map[string][]float32{}
	}
	if _, ok := h.cache[text]; !ok {
		h.cache[text] = v
		h.order = append(h.order, text)
		if len(h.order) > httpEmbedCacheSize {
			delete(h.cache, h.order[0])
			h.order = h.order[1:]
		}
	}
	h.mu.Unlock()
	return v
}

func (h *HTTPEmbedder) call(ctx context.Context, text string) ([]float32, error) {
	body := map[string]any{"model": h.ModelName, "input": text}
	if h.Dimensions > 0 {
		body["dimensions"] = h.Dimensions
	}
	data, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(h.BaseURL, "/")+"/embeddings", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.APIKey)
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, clip(string(raw), 200))
	}
	var out struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(out.Data) == 0 || len(out.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("no embedding in response")
	}
	v := out.Data[0].Embedding
	if h.Dimensions > 0 && len(v) != h.Dimensions {
		return nil, fmt.Errorf("embedding has %d dimensions, want %d", len(v), h.Dimensions)
	}
	return v, nil
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---------- background indexing ----------

// IndexItem is a memory whose search terms or embedding (for the current
// embedding model) are missing.
type IndexItem struct {
	TenantID      string
	ID            string
	Version       int64
	Title         string
	Content       string
	Subject       string
	NeedTerms     bool
	NeedEmbedding bool
}

// IndexStore is implemented by stores that keep memory search indexes.
type IndexStore interface {
	MemoriesToIndex(ctx context.Context, embeddingModel string, limit int) ([]IndexItem, error)
	PutMemoryIndex(ctx context.Context, it IndexItem, embeddingModel string, embedding []float32, terms string) error
}

// Indexer fills in what older memories lack: CJK search terms, and
// embeddings for the embedding model in use (after switching models,
// vectors of the old model are never compared with new ones).
type Indexer struct {
	Store    IndexStore
	Embedder Embedder
	Batch    int // per tick (default 50)
	Log      *slog.Logger
}

// Tick indexes one batch and returns how many memories it updated.
func (x *Indexer) Tick(ctx context.Context) (int, error) {
	if x == nil || x.Store == nil {
		return 0, nil
	}
	n := x.Batch
	if n <= 0 {
		n = 50
	}
	model := ""
	if x.Embedder != nil {
		model = x.Embedder.Model()
	}
	items, err := x.Store.MemoriesToIndex(ctx, model, n)
	if err != nil {
		return 0, err
	}
	done := 0
	for _, it := range items {
		if ctx.Err() != nil {
			break
		}
		var emb []float32
		if it.NeedEmbedding && x.Embedder != nil {
			if emb = x.Embedder.Embed(it.Title + "\n" + it.Content); emb == nil && !it.NeedTerms {
				break // the embedding service is failing: retry next tick
			}
		}
		terms := ""
		if it.NeedTerms {
			terms = SearchTerms(it.Title, it.Content, it.Subject)
		}
		if err := x.Store.PutMemoryIndex(ctx, it, model, emb, terms); err != nil {
			return done, err
		}
		done++
	}
	if done > 0 && x.Log != nil {
		x.Log.Info("memory index updated", "memories", done, "embedding_model", model)
	}
	return done, nil
}

// EmbedderFromEnv returns an HTTPEmbedder when HARNESS_EMBEDDING_BASE_URL
// and HARNESS_EMBEDDING_MODEL are set (the API key is read from the
// variable named by HARNESS_EMBEDDING_API_KEY_ENV, default
// HARNESS_EMBEDDING_API_KEY), or nil.
func EmbedderFromEnv(getenv func(string) string, log *slog.Logger) *HTTPEmbedder {
	base, model := strings.TrimSpace(getenv("HARNESS_EMBEDDING_BASE_URL")), strings.TrimSpace(getenv("HARNESS_EMBEDDING_MODEL"))
	if base == "" || model == "" {
		return nil
	}
	keyEnv := getenv("HARNESS_EMBEDDING_API_KEY_ENV")
	if keyEnv == "" {
		keyEnv = "HARNESS_EMBEDDING_API_KEY"
	}
	dims := 0
	fmt.Sscanf(getenv("HARNESS_EMBEDDING_DIMENSIONS"), "%d", &dims)
	return &HTTPEmbedder{BaseURL: base, ModelName: model, APIKey: getenv(keyEnv), Dimensions: dims, Log: log}
}
