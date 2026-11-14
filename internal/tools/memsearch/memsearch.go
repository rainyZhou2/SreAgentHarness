// Package memsearch is the builtin tool that searches long-term memory in
// the middle of a run. The first reasoning step only sees memories
// retrieved for the goal; as hypotheses, services and error signatures
// emerge, the agent searches again with what it has learned.
package memsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/tools"
)

// Name is the tool name (domain.MemorySearchToolName).
const Name = domain.MemorySearchToolName

// MaxContentRunes bounds each hit's content in the observation.
const MaxContentRunes = 1500

// Tool searches the tenant's ACTIVE memories with the retrieval pipeline.
type Tool struct {
	Retriever *memory.Retriever
}

func (Tool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: Name, Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
		Description: "Search long-term memory (past incidents, remediations, reasoning patterns) with a focused query: a hypothesis, symptom, error signature or component. Results are past experience to verify, not evidence; cite a hit by its ref.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{
"query":{"type":"string","description":"What to look for, e.g. a hypothesis or an error message."},
"service":{"type":"string","description":"Optional service to scope the search to (its dependencies are included)."},
"top_k":{"type":"integer","minimum":1,"maximum":10}},"required":["query"]}`)}
}

// Hit is one result as the agent sees it.
type Hit struct {
	Ref           string                `json:"ref"`
	MemoryID      string                `json:"memory_id"`
	Version       int64                 `json:"version"`
	Kind          string                `json:"kind"`
	Scope         string                `json:"scope"`
	Trust         string                `json:"trust"`
	Score         float64               `json:"score"`
	Branches      []string              `json:"branches,omitempty"`
	Title         string                `json:"title"`
	Content       string                `json:"content"`
	SourceRefs    []string              `json:"source_refs,omitempty"`
	EvidenceRefs  []string              `json:"evidence_refs,omitempty"`
	Applicability *memory.Applicability `json:"applicability,omitempty"`
	Verification  memory.Verification   `json:"verification"`
	ExpiresAt     *time.Time            `json:"expires_at,omitempty"`
}

// Result is the tool output.
type Result struct {
	Query     string              `json:"query"`
	Service   string              `json:"service,omitempty"`
	Hits      []Hit               `json:"hits"`
	Conflicts map[string][]string `json:"conflicts,omitempty"`
	Note      string              `json:"note"`
}

func (t Tool) Invoke(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	if t.Retriever == nil {
		return nil, errors.New("memory search is not configured")
	}
	var in struct {
		Query   string `json:"query"`
		Service string `json:"service"`
		TopK    int    `json:"top_k"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("arguments: %w", err)
	}
	in.Query = strings.TrimSpace(in.Query)
	if in.Query == "" {
		return nil, errors.New("query is required")
	}
	if in.TopK <= 0 || in.TopK > 10 {
		in.TopK = 5
	}
	info, _ := tools.CallInfoFrom(ctx)
	if info.TenantID == "" {
		return nil, errors.New("no tenant in call context")
	}
	var extra []string
	if in.Service != "" {
		extra = []string{in.Service}
	}
	res, err := t.Retriever.Search(ctx, info.TenantID, info.RunID, in.Query, extra, in.TopK)
	if err != nil {
		return nil, err
	}
	out := Result{Query: in.Query, Service: res.Service, Hits: []Hit{}, Conflicts: res.Conflicts,
		Note: "Past experience, not evidence about the current incident: check it with a tool before relying on it. Cite a hit by its ref."}
	for _, m := range res.Kept {
		content := m.Content
		if utf8.RuneCountInString(content) > MaxContentRunes {
			content = string([]rune(content)[:MaxContentRunes]) + "…"
		}
		out.Hits = append(out.Hits, Hit{Ref: fmt.Sprintf("memory:%s@%d", m.ID, m.Version), MemoryID: m.ID, Version: m.Version,
			Kind: m.Type, Scope: m.Scope + ":" + m.ScopeID, Trust: m.Trust, Score: res.Scores[m.ID],
			Branches: res.Branches[m.ID], Title: m.Title, Content: content,
			SourceRefs: m.SourceRefs, EvidenceRefs: m.EvidenceRefs, Applicability: m.Applicability,
			Verification: m.Verification, ExpiresAt: m.ExpiresAt})
	}
	return json.Marshal(out)
}

// Recalled reads the memories a memory_search observation returned.
func Recalled(content json.RawMessage) []Hit {
	var r Result
	_ = json.Unmarshal(content, &r)
	return r.Hits
}
