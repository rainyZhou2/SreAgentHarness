package workclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func wireEvent(seq int, versions string) string {
	return fmt.Sprintf(`{%s"event_id":"event-%d","sequence":%d,"occurred_at":"2026-11-14T00:00:00Z","tenant_id":"t","work_id":"w","type":"new.additive.event","payload":{"new_field":true}}`, versions, seq, seq)
}

const modernVersions = `"schema_version":1,"contract_version":1,"minimum_reader_version":1,`

func TestContractReadersCheckVersions(t *testing.T) {
	for _, tc := range []struct {
		name, versions string
		bad            bool
	}{
		{"legacy", `"schema_version":1,`, false},
		{"current", modernVersions, false},
		{"future_schema", `"schema_version":2,"contract_version":1,"minimum_reader_version":1,`, true},
		{"future_contract", `"schema_version":1,"contract_version":2,"minimum_reader_version":1,`, true},
		{"future_reader", `"schema_version":1,"contract_version":1,"minimum_reader_version":2,`, true},
		{"partial", `"schema_version":1,"contract_version":1,`, true},
		{"negative", `"schema_version":-1,`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, kind := range []string{"view", "empty_page", "page", "item"} {
				t.Run(kind, func(t *testing.T) {
					body := ""
					switch kind {
					case "view":
						versions := tc.versions
						if tc.name == "legacy" {
							versions = ""
						}
						body = `{` + versions + `"work":{"id":"w"},"last_sequence":0,"unknown_additive_field":true}`
					case "empty_page":
						body = `{` + tc.versions + `"items":[],"next_after_seq":0}`
					case "page":
						body = `{` + tc.versions + `"items":[` + wireEvent(1, modernVersions) + `],"next_after_seq":1}`
					case "item":
						body = `{` + modernVersions + `"items":[` + wireEvent(1, modernVersions) + `,` + wireEvent(2, tc.versions) + `],"next_after_seq":2}`
					}
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
					defer srv.Close()
					c := New(srv.URL, "")
					var err error
					if kind == "view" {
						_, err = c.View(context.Background(), "w")
					} else {
						var events []Event
						events, err = c.ContractEvents(context.Background(), "w", 0, 10)
						if err != nil && len(events) > 0 {
							t.Fatal("partially returned incompatible page")
						}
					}
					if err != nil {
						var pe *ProtocolError
						if !errors.As(err, &pe) {
							t.Fatalf("expected typed protocol error, got %T: %v", err, err)
						}
					}
					if (err != nil) != tc.bad {
						t.Fatalf("bad=%v err=%v body=%s", tc.bad, err, body)
					}
				})
			}
		})
	}
}

func TestStreamRejectsInvalidFrameBeforeAdvancingCursor(t *testing.T) {
	for _, tc := range []struct{ name, frame string }{
		{"malformed", `{"schema_version":1,`},
		{"future_schema", wireEvent(2, `"schema_version":2,"contract_version":1,"minimum_reader_version":1,`)},
		{"future_contract", wireEvent(2, `"schema_version":1,"contract_version":2,"minimum_reader_version":1,`)},
		{"future_reader", wireEvent(2, `"schema_version":1,"contract_version":1,"minimum_reader_version":2,`)},
		{"missing_identity", `{"schema_version":1,"sequence":2}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: %s\n\ndata: %s\n\ndata: %s\n\n", wireEvent(1, modernVersions), tc.frame, strings.Replace(wireEvent(3, modernVersions), "new.additive.event", "work.completed", 1))
			}))
			defer srv.Close()
			c := New(srv.URL, "")
			last := int64(0)
			out := make(chan Event, 4)
			done, err := c.stream(context.Background(), "w", &last, out)
			var pe *ProtocolError
			if !errors.As(err, &pe) {
				t.Fatalf("expected typed permanent error: %T %v", err, err)
			}
			if err == nil || done || last != 1 || len(out) != 1 {
				t.Fatalf("bad frame was skipped/delivered: done=%v err=%v cursor=%d delivered=%d", done, err, last, len(out))
			}
		})
	}
}

func TestSubscribeDoesNotRetryIncompatibleData(t *testing.T) {
	for _, frame := range []string{wireEvent(1, `"schema_version":2,"contract_version":1,"minimum_reader_version":1,`), `{"broken":`} {
		t.Run(frame, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/view") {
					fmt.Fprint(w, `{"work":{"id":"w","status":"ACTIVE"},"last_sequence":0}`)
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", frame, strings.Replace(wireEvent(2, modernVersions), "new.additive.event", "work.completed", 1))
			}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			events, errs := New(srv.URL, "").Subscribe(ctx, "w", 0)
			for ev := range events {
				t.Errorf("invalid subscription delivered %+v", ev)
			}
			var pe *ProtocolError
			if err := <-errs; !errors.As(err, &pe) {
				t.Fatalf("invalid subscription did not fail with typed error: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatalf("invalid stream retried %d times", calls.Load())
			}
		})
	}
}

func TestStreamRejectsFutureHeaderOnEmptyStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Harness-Event-Schema", "2")
	}))
	defer srv.Close()
	last := int64(4)
	_, err := New(srv.URL, "").stream(context.Background(), "w", &last, make(chan Event, 1))
	if err == nil || last != 4 {
		t.Fatalf("incompatible empty stream accepted: %v cursor=%d", err, last)
	}
}

func TestLegacyAndUnknownAdditiveFieldsDecode(t *testing.T) {
	var old struct {
		SchemaVersion int    `json:"schema_version"`
		Sequence      int64  `json:"sequence"`
		Type          string `json:"type"`
	}
	if err := json.Unmarshal([]byte(wireEvent(1, modernVersions)), &old); err != nil || old.SchemaVersion != 1 || old.Sequence != 1 {
		t.Fatalf("new data broke old shape: %+v %v", old, err)
	}
}

func TestStreamKeepsLegacyAndEphemeralSemantics(t *testing.T) {
	legacy := wireEvent(1, `"schema_version":1,`)
	ephemeral := `{` + modernVersions + `"type":"model.delta","ephemeral":true,"tenant_id":"t","work_id":"w","occurred_at":"2026-11-14T00:00:00Z","payload":{"text":"hi"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Harness-Event-Schema", "1")
		fmt.Fprintf(w, "data: %s\n\ndata: %s\n\ndata: %s\n\n", legacy, ephemeral, wireEvent(2, modernVersions))
	}))
	defer srv.Close()
	out := make(chan Event, 4)
	last := int64(1)
	done, err := New(srv.URL, "").stream(context.Background(), "w", &last, out)
	if err != nil || done || last != 2 || len(out) != 2 {
		t.Fatalf("legacy/duplicate/ephemeral handling: done=%v err=%v cursor=%d events=%d", done, err, last, len(out))
	}
	if ev := <-out; !ev.Ephemeral || ev.Sequence != 0 {
		t.Fatalf("ephemeral: %+v", ev)
	}
	if ev := <-out; ev.Type != "new.additive.event" {
		t.Fatalf("unknown additive type dropped: %+v", ev)
	}
}

func TestSubscribeRetriesDisconnectAtLastDeliveredCursor(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/view") {
			status, last := "ACTIVE", 0
			if calls.Load() >= 2 {
				status, last = "CANCELLED", 2
			}
			fmt.Fprintf(w, `{"work":{"id":"w","status":%q},"last_sequence":%d}`, status, last)
			return
		}
		call := calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		if call == 1 {
			fmt.Fprintf(w, "data: %s\n\ndata: {", wireEvent(1, modernVersions))
			return
		}
		if got := r.Header.Get("Last-Event-ID"); got != "1" {
			t.Errorf("resume cursor=%q", got)
		}
		fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", wireEvent(1, modernVersions), strings.Replace(wireEvent(2, modernVersions), "new.additive.event", "work.completed", 1))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, errs := New(srv.URL, "").Subscribe(ctx, "w", 0)
	var seqs []int64
	for ev := range events {
		seqs = append(seqs, ev.Sequence)
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(seqs) != "[1 2]" || calls.Load() != 2 {
		t.Fatalf("resume=%v calls=%d", seqs, calls.Load())
	}
}

func TestStreamRejectsOversizedFrame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", strings.Repeat("x", 4<<20))
	}))
	defer srv.Close()
	last := int64(0)
	_, err := New(srv.URL, "").stream(context.Background(), "w", &last, make(chan Event, 1))
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != "invalid_frame" || last != 0 {
		t.Fatalf("oversized frame: cursor=%d err=%v", last, err)
	}
}

func TestStreamRejectsSequenceGap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", wireEvent(1, modernVersions), wireEvent(3, modernVersions))
	}))
	defer srv.Close()
	last := int64(0)
	out := make(chan Event, 3)
	_, err := New(srv.URL, "").stream(context.Background(), "w", &last, out)
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != "invalid_frame" || last != 1 || len(out) != 1 {
		t.Fatalf("gap advanced cursor: last=%d delivered=%d err=%v", last, len(out), err)
	}
}

func TestContractPageChecksScopeAndCursor(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		after      int64
	}{
		{"different_work", `{` + modernVersions + `"items":[` + strings.Replace(wireEvent(1, modernVersions), `"work_id":"w"`, `"work_id":"other"`, 1) + `],"next_after_seq":1}`, 0},
		{"first_gap", `{` + modernVersions + `"items":[` + wireEvent(2, modernVersions) + `],"next_after_seq":2}`, 0},
		{"middle_gap", `{` + modernVersions + `"items":[` + wireEvent(1, modernVersions) + `,` + wireEvent(3, modernVersions) + `],"next_after_seq":3}`, 0},
		{"wrong_next", `{` + modernVersions + `"items":[` + wireEvent(1, modernVersions) + `],"next_after_seq":99}`, 0},
		{"empty_jump", `{` + modernVersions + `"items":[],"next_after_seq":99}`, 7},
		{"duplicate", `{` + modernVersions + `"items":[` + wireEvent(1, modernVersions) + `,` + wireEvent(1, modernVersions) + `],"next_after_seq":1}`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, tc.body) }))
			defer srv.Close()
			events, err := New(srv.URL, "").ContractEvents(context.Background(), "w", tc.after, 10)
			var pe *ProtocolError
			if !errors.As(err, &pe) || pe.Code != "invalid_frame" || len(events) != 0 {
				t.Fatalf("invalid page returned: %+v err=%v", events, err)
			}
		})
	}
}

func TestViewRejectsAnotherWork(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{`+modernVersions+`"work":{"id":"other"},"last_sequence":0}`)
	}))
	defer srv.Close()
	view, err := New(srv.URL, "").View(context.Background(), "w")
	var pe *ProtocolError
	if !errors.As(err, &pe) || pe.Code != "invalid_frame" || view.Work.ID != "" {
		t.Fatalf("foreign snapshot returned: %+v err=%v", view, err)
	}
}

type unrelatedResponse struct {
	Value string `json:"value"`
}

func (*unrelatedResponse) Validate() error {
	return fmt.Errorf("unrelated domain validator must not run")
}
func TestHTTPDecodeDoesNotInvokeUnrelatedDomainValidation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"value":"ok"}`) }))
	defer srv.Close()
	var out unrelatedResponse
	_, err := New(srv.URL, "").do(context.Background(), http.MethodGet, "/domain", nil, nil, &out)
	if err != nil || out.Value != "ok" {
		t.Fatalf("unrelated validator invoked: %+v %v", out, err)
	}
}

func TestSubscribeLegacyTerminalBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		after                         int64
		active, missing, failSnapshot bool
	}{
		{name: "completed then disconnected", active: true},
		{name: "already terminal"},
		{name: "at terminal watermark", after: 3},
		{name: "missing tail", active: true, missing: true},
		{name: "snapshot failure", active: true, failSnapshot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var streams, views atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/view"):
					call := views.Add(1)
					if tc.failSnapshot && call > 1 {
						http.Error(w, "missing", 404)
						return
					}
					status := "CANCELLED"
					if tc.active && call == 1 {
						status = "ACTIVE"
					}
					fmt.Fprintf(w, `{"work":{"id":"w","status":%q},"last_sequence":3}`, status)
				case strings.HasSuffix(r.URL.Path, "/stream"):
					streams.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "data: %s\n\ndata: %s\n\ndata: {", wireEvent(1, `"schema_version":1,`), strings.Replace(wireEvent(2, `"schema_version":1,`), "new.additive.event", "work.completed", 1))
				case strings.HasSuffix(r.URL.Path, "/events"):
					after, _ := strconv.Atoi(r.URL.Query().Get("after_seq"))
					items := []string{}
					if !tc.missing {
						for seq := after + 1; seq <= 3; seq++ {
							items = append(items, wireEvent(seq, `"schema_version":1,`))
						}
					}
					fmt.Fprintf(w, `{"schema_version":1,"items":[%s],"next_after_seq":%d}`, strings.Join(items, ","), after+len(items))
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			events, errs := New(srv.URL, "").Subscribe(ctx, "w", tc.after)
			last := tc.after
			for ev := range events {
				if ev.Sequence != last+1 {
					t.Errorf("gap at %d", ev.Sequence)
				}
				last = ev.Sequence
			}
			err := <-errs
			if tc.missing || tc.failSnapshot {
				if err == nil {
					t.Fatal("incomplete terminal snapshot ended cleanly")
				}
				return
			}
			if err != nil || ctx.Err() != nil || last != 3 {
				t.Fatalf("last=%d err=%v context=%v", last, err, ctx.Err())
			}
			if !tc.active && streams.Load() != 0 {
				t.Fatalf("terminal snapshot opened %d SSE streams", streams.Load())
			}
		})
	}
}

func TestSubscribeRetainsTerminalWatermarkAcrossDrainRetry(t *testing.T) {
	var views, pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/view") {
			call := views.Add(1)
			// A later audit must not silently move this subscription's boundary.
			fmt.Fprintf(w, `{"work":{"id":"w","status":"CANCELLED"},"last_sequence":%d}`, 200+call)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/events") {
			t.Errorf("unexpected path %s", r.URL.Path)
			http.Error(w, "unexpected", 400)
			return
		}
		call := pages.Add(1)
		after, _ := strconv.Atoi(r.URL.Query().Get("after_seq"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if (after == 0 && limit != 200) || (after == 200 && limit != 1) {
			t.Errorf("cursor=%d limit=%d", after, limit)
		}
		if call == 2 {
			http.Error(w, "temporary disconnect", http.StatusServiceUnavailable)
			return
		}
		items := make([]string, 0, limit)
		for seq := after + 1; seq <= after+limit; seq++ {
			items = append(items, wireEvent(seq, modernVersions))
		}
		fmt.Fprintf(w, `{%s"items":[%s],"next_after_seq":%d}`, modernVersions, strings.Join(items, ","), after+limit)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events, errs := New(srv.URL, "").Subscribe(ctx, "w", 0)
	last := int64(0)
	for ev := range events {
		if ev.Sequence != last+1 {
			t.Fatalf("gap at %d", ev.Sequence)
		}
		last = ev.Sequence
	}
	if err := <-errs; err != nil || ctx.Err() != nil || last != 201 || views.Load() != 1 || pages.Load() != 3 {
		t.Fatalf("last=%d views=%d pages=%d err=%v context=%v", last, views.Load(), pages.Load(), err, ctx.Err())
	}
}

func TestSubscribeRejectsPageBeyondTerminalWatermark(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/view") {
			fmt.Fprint(w, `{"work":{"id":"w","status":"CANCELLED"},"last_sequence":1}`)
			return
		}
		fmt.Fprintf(w, `{%s"items":[%s,%s],"next_after_seq":2}`, modernVersions, wireEvent(1, modernVersions), wireEvent(2, modernVersions))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, errs := New(srv.URL, "").Subscribe(ctx, "w", 0)
	for ev := range events {
		t.Errorf("over-boundary page delivered %+v", ev)
	}
	var pe *ProtocolError
	if err := <-errs; !errors.As(err, &pe) || pe.Code != "invalid_frame" {
		t.Fatalf("expected invalid frame: %v", err)
	}
}
