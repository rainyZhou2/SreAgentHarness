// Package runsearchtool is the builtin tool that searches past runs. Memory
// holds what someone chose (or the consolidator chose) to keep; this tool
// reaches the record behind it: how earlier investigations were asked and
// how they ended, including ones nobody promoted to a memory.
package runsearchtool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/runsearch"
	"go-sre-harness/internal/tools"
)

// Name is the tool name.
const Name = domain.RunSearchToolName

// RunReader resolves the calling run, to know whose chats it may see.
type RunReader interface {
	GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error)
}

// Tool searches the tenant's finished runs.
type Tool struct {
	Search *runsearch.Searcher
	Runs   RunReader
}

func (Tool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: Name, Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
		Description: "Search past investigations (finished runs): what was asked, the conclusion and the hypotheses. Use a focused query: a symptom, error text, service or component. Results are history, not evidence about the current incident: verify with a tool before relying on one. Complements " + domain.MemorySearchToolName + ", which holds only what was kept as memory.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{
"query":{"type":"string","description":"What to look for, e.g. an error message, symptom or component."},
"service":{"type":"string","description":"Optional: only runs that mention this service."},
"status":{"type":"string","enum":["COMPLETED","FAILED","CANCELLED","any"],"description":"Default COMPLETED."},
"limit":{"type":"integer","minimum":1,"maximum":10}},"required":["query"]}`)}
}

// Output is the tool result.
type Output struct {
	Query   string          `json:"query"`
	Hits    []runsearch.Hit `json:"hits"`
	Scanned int             `json:"scanned_runs"`
	Partial bool            `json:"partial,omitempty"`
	Note    string          `json:"note"`
}

func (t Tool) Invoke(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	if t.Search == nil || t.Runs == nil {
		return nil, errors.New("run search is not configured")
	}
	var in struct {
		Query   string `json:"query"`
		Service string `json:"service"`
		Status  string `json:"status"`
		Limit   int    `json:"limit"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("arguments: %w", err)
	}
	in.Query = strings.TrimSpace(in.Query)
	if in.Query == "" {
		return nil, errors.New("query is required")
	}
	info, _ := tools.CallInfoFrom(ctx)
	if info.TenantID == "" {
		return nil, errors.New("no tenant in call context")
	}
	caller := runsearch.Caller{RunID: info.RunID}
	if run, err := t.Runs.GetRun(ctx, info.TenantID, info.RunID); err == nil {
		caller.InitiatorType = run.InitiatorType
		if run.InitiatorID != nil {
			caller.InitiatorID = *run.InitiatorID
		}
	} // an unknown caller sees only non-user runs
	res, err := t.Search.Search(ctx, runsearch.Query{TenantID: info.TenantID, Text: in.Query, Service: in.Service,
		Status: in.Status, Limit: in.Limit, Caller: caller})
	if err != nil {
		return nil, err
	}
	note := "Past runs are history, not evidence about the current incident: their conclusions can be wrong or out of date. Check against live data before relying on one."
	if res.Partial {
		note += " Only the most recent runs were searched."
	}
	return json.Marshal(Output{Query: in.Query, Hits: res.Hits, Scanned: res.Scanned, Partial: res.Partial, Note: note})
}
