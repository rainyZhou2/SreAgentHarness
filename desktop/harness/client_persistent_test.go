package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go-sre-harness/pkg/eventcursor"
)

func TestStreamEventsPersistentResumesWithoutSnapshotRegression(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cursors = append(cursors, r.Header.Get("Last-Event-ID"))
		request := len(cursors)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"event_seq\":%d,\"event_type\":\"run.updated\",\"payload\":{}}\n\n", request)
		fmt.Fprint(w, "event: stream.end\ndata: {}\n\n")
	}))
	defer srv.Close()
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject"}
	for want := int64(1); want <= 2; want++ {
		client, err := New(srv.URL, Credentials{Mode: "bearer", Token: "token"})
		if err != nil {
			t.Fatal(err)
		}
		var got []int64
		if err := client.StreamEventsPersistent(context.Background(), "run", 0, store, identity, func(e Event) {
			got = append(got, e.Seq)
		}); err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0] != want {
			t.Fatalf("follow %d events=%v", want, got)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 2 || cursors[0] != "0" || cursors[1] != "1" {
		t.Fatalf("request cursors=%v", cursors)
	}
}

func TestStreamEventsPersistentRejectsSequenceGap(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"event_seq\":2,\"event_type\":\"run.updated\",\"payload\":{}}\n\n")
	}))
	defer srv.Close()
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(srv.URL, Credentials{Mode: "bearer", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = client.StreamEventsPersistent(ctx, "run", 0, store, identity, func(Event) {})
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gap error=%v", err)
	}
}

type desktopFailingCursorStore struct{ err error }

func (s desktopFailingCursorStore) Load(context.Context, eventcursor.Key) (eventcursor.Cursor, bool, error) {
	return eventcursor.Cursor{}, false, nil
}
func (s desktopFailingCursorStore) Advance(context.Context, eventcursor.Key, int64, string) error {
	return s.err
}

func TestStreamEventsPersistentCommitFailureIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"event_seq\":1,\"event_type\":\"run.updated\",\"payload\":{}}\n\n")
	}))
	defer srv.Close()
	client, err := New(srv.URL, Credentials{Mode: "bearer", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("disk unavailable")
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = client.StreamEventsPersistent(ctx, "run", 0, desktopFailingCursorStore{commitErr}, identity, func(Event) {})
	if !errors.Is(err, commitErr) {
		t.Fatalf("commit error=%v", err)
	}
}
