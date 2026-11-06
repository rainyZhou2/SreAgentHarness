package mcpexec

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go-sre-harness/internal/domain"
)

// swapHandler lets a test replace the server behind a fixed URL, which
// drops every session the old one knew (like a process restart).
type swapHandler struct {
	mu sync.Mutex
	h  http.Handler
}

func (s *swapHandler) set(h http.Handler) { s.mu.Lock(); s.h = h; s.mu.Unlock() }
func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	h := s.h
	s.mu.Unlock()
	h.ServeHTTP(w, r)
}

func pagingServer(calls *atomic.Int32) http.Handler {
	srv := mcp.NewServer(&mcp.Implementation{Name: "ops", Version: "0"}, nil)
	srv.AddTool(&mcp.Tool{Name: "page", InputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			calls.Add(1)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "paged"}}}, nil
		})
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)
}

// A restarted server forgets the harness's session; the next call must
// reconnect rather than fail with an unknown outcome.
func TestSideEffectCallSurvivesServerRestart(t *testing.T) {
	var calls atomic.Int32
	sw := &swapHandler{h: pagingServer(&calls)}
	hs := httptest.NewServer(sw)
	defer hs.Close()
	s, err := New(ServerConfig{Name: "ops", URL: hs.URL, Allow: map[string]ToolPolicy{"page": {Risk: domain.RiskR2}}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	ts, _, _, err := s.Discover(dctx)
	cancel()
	if err != nil || len(ts) != 1 {
		t.Fatalf("discover: %v %d", err, len(ts))
	}
	page := ts[0]
	if _, err := page.Invoke(context.Background(), "k1", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("first call: %v", err)
	}

	sw.set(pagingServer(&calls)) // restart: old sessions are unknown now
	hs.CloseClientConnections()
	time.Sleep(200 * time.Millisecond)

	out, err := page.Invoke(context.Background(), "k2", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("call after restart: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2 (%s)", calls.Load(), out)
	}
}
