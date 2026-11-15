package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/pkg/eventcursor"
)

func TestFollowPersistentResumesCommittedRunEvent(t *testing.T) {
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
		client, err := NewClient(srv.URL, "token")
		if err != nil {
			t.Fatal(err)
		}
		var got []int64
		if err := client.FollowPersistent(context.Background(), "run", store, identity, func(e Event) {
			got = append(got, e.Seq)
		}, nil); err != nil {
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

func TestFollowPersistentRejectsSequenceGapWithoutCommit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"event_seq\":2,\"event_type\":\"run.updated\",\"payload\":{}}\n\n")
	}))
	defer srv.Close()
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(srv.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = client.FollowPersistent(ctx, "run", store, identity, func(Event) {}, nil)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gap error=%v", err)
	}
	key := eventcursor.Key{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject", Stream: "run", ResourceID: "run", Purpose: "events-v1"}
	if _, ok, loadErr := store.Load(context.Background(), key); loadErr != nil || ok {
		t.Fatalf("cursor committed after gap: ok=%v err=%v", ok, loadErr)
	}
}

type cliFailingCursorStore struct{ err error }

func (s cliFailingCursorStore) Load(context.Context, eventcursor.Key) (eventcursor.Cursor, bool, error) {
	return eventcursor.Cursor{}, false, nil
}
func (s cliFailingCursorStore) Advance(context.Context, eventcursor.Key, int64, string) error {
	return s.err
}

func TestFollowPersistentCommitFailureIsTerminal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"event_seq\":1,\"event_type\":\"run.updated\",\"payload\":{}}\n\n")
	}))
	defer srv.Close()
	client, err := NewClient(srv.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	commitErr := errors.New("disk unavailable")
	identity := SubscriptionIdentity{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = client.FollowPersistent(ctx, "run", cliFailingCursorStore{commitErr}, identity, func(Event) {}, nil)
	if !errors.Is(err, commitErr) {
		t.Fatalf("commit error=%v", err)
	}
}

func TestChatUsesPersistentCursorStore(t *testing.T) {
	f := &fakeServer{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	client, err := NewClient(srv.URL, "token")
	if err != nil {
		t.Fatal(err)
	}
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	chat := &Chat{C: client, In: strings.NewReader("restart checkout\n"), Out: io.Discard, AutoYes: true, CursorStore: store}
	if err := chat.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	key := (SubscriptionIdentity{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "me"}).cursorKey("r1")
	cursor, ok, err := store.Load(context.Background(), key)
	if err != nil || !ok || cursor.Sequence != 2 {
		t.Fatalf("cursor=%+v ok=%v err=%v", cursor, ok, err)
	}
}

func TestPersistentCursorIncludesRunIdentity(t *testing.T) {
	identity := SubscriptionIdentity{ServerURL: "https://example.test/", TenantID: "tenant", SubjectID: "subject"}
	key := identity.cursorKey("run")
	if key.Stream != "run" || key.ResourceID != "run" || key.Purpose != "events-v1" {
		t.Fatalf("key=%+v", key)
	}
	if key.ServerURL != "https://example.test" {
		t.Fatalf("server URL was not normalized by the store contract: %s", strconv.Quote(key.ServerURL))
	}
}
