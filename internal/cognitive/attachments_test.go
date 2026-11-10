package cognitive

import (
	"encoding/json"
	"testing"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
)

func TestConclusionAttachmentsParsedLeniently(t *testing.T) {
	args := `{"answer":"ok","reason_summary":"r","attachments":[
	 {"type":"table","title":"Pods","columns":["name","restarts"],"rows":[["a",3],["b",null,true]]},
	 "garbage",
	 {"type":"link","url":"https://example.com/d","title":"Dash"}]}`
	d, err := decisionFromResponse(gw.Response{ToolCalls: []gw.ToolCall{{Name: ConclusionTool, Args: json.RawMessage(args)}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.Attachments) != 2 {
		t.Fatalf("want 2 attachments, got %+v", d.Attachments)
	}
	if got := d.Attachments[0].Rows[1]; len(got) != 3 || got[0] != "b" || got[1] != "" || got[2] != "true" {
		t.Fatalf("cells: %#v", got)
	}
	// A conclusion without attachments is unchanged (backward compatible).
	d, err = decisionFromResponse(gw.Response{ToolCalls: []gw.ToolCall{{Name: ConclusionTool, Args: json.RawMessage(`{"answer":"a","reason_summary":"r"}`)}}})
	if err != nil || d.Attachments != nil {
		t.Fatalf("plain conclusion: %v %+v", err, d.Attachments)
	}
}

func TestResolveAttachments(t *testing.T) {
	handle := "mem://t1/tool-output/abc"
	in := ReasoningInput{Observations: []domain.Observation{
		{StepSeq: 2, Tool: "logs", Artifact: &domain.ObservationArtifact{Handle: handle, Bytes: 4096, ContentType: "application/json"}},
		{StepSeq: 3, Tool: "small"},
	}}
	got := ResolveAttachments("t1", in, []domain.Attachment{
		{Type: "link", URL: "javascript:alert(1)"},              // dropped
		{Type: "link", URL: "file:///etc/passwd"},               // dropped
		{Type: "link", URL: "https://u:p@example.com/"},         // dropped (credentials)
		{Type: "image", URL: "data:image/png;base64,AAAA"},      // dropped
		{Type: "file", Ref: "obs:step2", Title: "logs.json"},    // resolved
		{Type: "file", Ref: "obs:step3"},                        // not offloaded: dropped
		{Type: "file", Ref: "mem://t2/tool-output/x"},           // other tenant: dropped
		{Type: "image", Ref: handle, MimeType: "image/svg+xml"}, // stored SVG: becomes a file
		{Type: "html", URL: "https://example.com"},              // unknown type
		{Type: "table", Columns: []string{"a"}, Rows: [][]string{{"1", "2"}, {"3"}}},
		{Type: "link", URL: " https://example.com/x ", Title: "X"},
	})
	if len(got) != 4 {
		t.Fatalf("want 4, got %d: %+v", len(got), got)
	}
	if f := got[0]; f.Ref != handle || f.SizeBytes != 4096 || f.MimeType != "application/json" {
		t.Fatalf("file: %+v", f)
	}
	if got[1].Type != domain.AttachFile {
		t.Fatalf("svg image should be a file: %+v", got[1])
	}
	if tb := got[2]; len(tb.Columns) != 2 || tb.Columns[1] != "2" || len(tb.Rows[1]) != 2 {
		t.Fatalf("table: %+v", tb)
	}
	if got[3].URL != "https://example.com/x" {
		t.Fatalf("link: %+v", got[3])
	}
}

func TestResolveAttachmentsCaps(t *testing.T) {
	var list []domain.Attachment
	for range 30 {
		list = append(list, domain.Attachment{Type: "link", URL: "https://example.com"})
	}
	if got := ResolveAttachments("t", ReasoningInput{}, list); len(got) != maxAttachments {
		t.Fatalf("cap: %d", len(got))
	}
	rows := make([][]string, 500)
	for i := range rows {
		rows[i] = []string{"x"}
	}
	got := ResolveAttachments("t", ReasoningInput{}, []domain.Attachment{{Type: "table", Columns: []string{"c"}, Rows: rows}})
	if len(got[0].Rows) != maxTableRows || got[0].Caption == "" {
		t.Fatalf("rows: %d caption %q", len(got[0].Rows), got[0].Caption)
	}
}
