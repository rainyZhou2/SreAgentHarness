// Command mcp-demo is a small streamable-HTTP MCP server for trying the
// harness's tenant MCP extensions locally:
//
//	go run ./examples/mcp-demo            # listens on 127.0.0.1:8090/mcp
//
// Add it from the desktop client (Customize → MCP servers) with the URL
// http://127.0.0.1:8090/mcp, or with the API:
//
//	PUT /v1/extensions/mcp-servers/demo
//	{"spec":{"url":"http://127.0.0.1:8090/mcp","allow":{"runbook_search":{"risk":"R0"}}}}
//
// It exposes two read-only tools (runbook_search, oncall_lookup) and one
// side-effecting tool (page_oncall) to exercise risk classes and approvals,
// plus POST /deploys, a plain JSON endpoint to try HTTP tools with.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var runbooks = map[string]string{
	"db-pool-exhaustion": "Database connection pool exhausted: check active connections, look for long-running " +
		"transactions, raise pool size only after confirming no leak; restart the service if connections are stuck.",
	"high-latency": "High p99 latency: compare with the last deploy, check downstream dependencies and saturation " +
		"(CPU, GC, queue depth); roll back if the regression started with a release.",
	"disk-full":   "Disk full: identify the largest directories, rotate or ship logs, expand the volume if growth is organic.",
	"error-spike": "5xx spike: group errors by endpoint and version, check feature flags and recent config changes.",
}

var oncall = map[string]string{"checkout": "alice", "payments": "bob", "search": "carol"}

func text(s string) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: s}}}
}

func args(req *mcp.CallToolRequest) map[string]string {
	var a map[string]any
	_ = json.Unmarshal(req.Params.Arguments, &a)
	out := map[string]string{}
	for k, v := range a {
		out[k] = fmt.Sprint(v)
	}
	return out
}

func newServer() *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "sre-demo", Version: "0.1.0"}, nil)
	ro, side := true, false
	s.AddTool(&mcp.Tool{Name: "runbook_search", Description: "Search the SRE runbooks by keywords.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: ro},
		InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"type":"string","description":"keywords"}},"required":["query"]}`)},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			q := strings.ToLower(args(req)["query"])
			var hits []string
			for name, body := range runbooks {
				for _, w := range strings.Fields(q) {
					if strings.Contains(name, w) || strings.Contains(strings.ToLower(body), w) {
						hits = append(hits, "## "+name+"\n"+body)
						break
					}
				}
			}
			sort.Strings(hits)
			if len(hits) == 0 {
				return text("No runbook matches " + q), nil
			}
			return text(strings.Join(hits, "\n\n")), nil
		})
	s.AddTool(&mcp.Tool{Name: "oncall_lookup", Description: "Who is on call for a service.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: ro},
		InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"}},"required":["service"]}`)},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			svc := args(req)["service"]
			if who, ok := oncall[svc]; ok {
				return text(svc + " on-call: " + who), nil
			}
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "unknown service " + svc}}}, nil
		})
	s.AddTool(&mcp.Tool{Name: "page_oncall", Description: "Page the on-call engineer of a service (side effect).",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &side},
		InputSchema: json.RawMessage(`{"type":"object","properties":{"service":{"type":"string"},"message":{"type":"string"}},"required":["service","message"]}`)},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			a := args(req)
			log.Printf("PAGE %s: %s", a["service"], a["message"])
			return text("paged on-call for " + a["service"]), nil
		})
	return s
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "listen address")
	flag.Parse()
	srv := newServer()
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	// A plain HTTP endpoint for trying HTTP tools (Customize → HTTP tools,
	// URL http://127.0.0.1:8090/deploys): POST {"service": "..."}.
	mux.HandleFunc("POST /deploys", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Service string `json:"service"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"service": in.Service, "deploys": []map[string]string{
			{"version": "v2.14.1", "at": "2026-11-05T13:42:00Z", "by": "ci", "change": "connection pool max 50 -> 20"},
			{"version": "v2.14.0", "at": "2026-11-04T09:10:00Z", "by": "ci", "change": "upgrade http client"},
		}})
	})
	log.Printf("mcp-demo listening on http://%s/mcp (and POST /deploys)", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
