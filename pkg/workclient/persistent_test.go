package workclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go-sre-harness/pkg/eventcursor"
)

func TestSubscribePersistentResumesAfterProcessReopen(t *testing.T) {
	var pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/works/w/view":
			fmt.Fprint(w, `{"work":{"id":"w","status":"CANCELLED"},"last_sequence":2}`)
		case "/api/works/w/events":
			pages.Add(1)
			if r.URL.Query().Get("after_seq") != "0" {
				fmt.Fprint(w, `{"items":[],"next_after_seq":2}`)
				return
			}
			fmt.Fprintf(w, `{%s"items":[%s,%s],"next_after_seq":2}`, modernVersions, wireEvent(1, modernVersions), wireEvent(2, modernVersions))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "t", SubjectID: "subject"}
	consume := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		events, errs := New(srv.URL, "").SubscribePersistent(ctx, "w", store, identity)
		var seqs []int64
		for ev := range events {
			seqs = append(seqs, ev.Sequence)
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if len(seqs) > 0 && fmt.Sprint(seqs) != "[1 2]" {
			t.Fatalf("sequences=%v", seqs)
		}
	}
	consume()
	consume()
	if pages.Load() != 1 {
		t.Fatalf("persistent cursor did not skip delivered pages: %d", pages.Load())
	}
}

type failingCursorStore struct{ err error }

func (s failingCursorStore) Load(context.Context, eventcursor.Key) (eventcursor.Cursor, bool, error) {
	return eventcursor.Cursor{}, false, nil
}
func (s failingCursorStore) Advance(context.Context, eventcursor.Key, int64, string) error {
	return s.err
}

func TestSubscribePersistentCommitFailureIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/works/w/view" {
			fmt.Fprint(w, `{"work":{"id":"w","status":"CANCELLED"},"last_sequence":1}`)
			return
		}
		fmt.Fprintf(w, `{%s"items":[%s],"next_after_seq":1}`, modernVersions, wireEvent(1, modernVersions))
	}))
	defer srv.Close()
	commitErr := errors.New("disk unavailable")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events, errs := New(srv.URL, "").SubscribePersistent(ctx, "w", failingCursorStore{commitErr}, SubscriptionIdentity{ServerURL: srv.URL, TenantID: "t", SubjectID: "s"})
	for range events {
	}
	if err := <-errs; !errors.Is(err, commitErr) {
		t.Fatalf("commit error=%v", err)
	}
}

func TestSubscribePersistentCommitsSSEBeforeReconnect(t *testing.T) {
	var streams atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/works/w/view":
			if streams.Load() == 0 {
				fmt.Fprint(w, `{"work":{"id":"w","status":"ACTIVE"},"last_sequence":0}`)
				return
			}
			fmt.Fprint(w, `{"work":{"id":"w","status":"CANCELLED"},"last_sequence":2}`)
		case "/api/works/w/events/stream":
			streams.Add(1)
			if got := r.Header.Get("Last-Event-ID"); got != "0" {
				t.Errorf("initial cursor=%q", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			completed := fmt.Sprintf(`{%s"event_id":"event-2","sequence":2,"occurred_at":"2026-11-14T00:00:00Z","tenant_id":"t","work_id":"w","type":"work.completed","payload":{}}`, modernVersions)
			fmt.Fprintf(w, "data: %s\n\ndata: %s\n\n", wireEvent(1, modernVersions), completed)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "t", SubjectID: "s"}
	consume := func() []int64 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		events, errs := New(srv.URL, "").SubscribePersistent(ctx, "w", store, identity)
		var sequences []int64
		for ev := range events {
			sequences = append(sequences, ev.Sequence)
		}
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		return sequences
	}
	if got := fmt.Sprint(consume()); got != "[1 2]" {
		t.Fatalf("sequences=%s", got)
	}
	if got := consume(); len(got) != 0 {
		t.Fatalf("events after recovery=%v", got)
	}
	if streams.Load() != 1 {
		t.Fatalf("SSE connections=%d, want 1", streams.Load())
	}
	cursor, ok, err := store.Load(context.Background(), eventcursor.Key{ServerURL: srv.URL, TenantID: "t", SubjectID: "s", Stream: "work", ResourceID: "w", Purpose: "contract-v1"})
	if err != nil || !ok || cursor.Sequence != 2 || cursor.EventID != "event-2" {
		t.Fatalf("cursor=%+v ok=%v err=%v", cursor, ok, err)
	}
}
