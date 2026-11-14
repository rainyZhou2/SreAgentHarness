package workapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/runbook"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workapi"
	"go-sre-harness/internal/world"
	"go-sre-harness/pkg/workclient"
	_ "go-sre-harness/plugins/observability"
)

type v3env struct {
	t     *testing.T
	srv   *httptest.Server
	store *cpsql.Store
	api   *workapi.API
}

func newV3(t *testing.T) *v3env {
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat, err := orchestration.NewCatalog(nil)
	if err != nil {
		t.Fatal(err)
	}
	router, _ := eventing.NewRouter(nil)
	ws := &world.Service{Store: s}
	limits := orchestration.DefaultQueueLimits()
	bl := limits[work.QueueBatch]
	bl.MaxBacklog = 2
	limits[work.QueueBatch] = bl
	a := &workapi.API{Store: s, Service: &orchestration.Service{Store: s, Catalog: cat, Log: quiet, Queues: limits},
		Ingestor: &orchestration.Ingestor{Store: s, Router: router, Catalog: cat, Log: quiet},
		World:    ws, Log: quiet, Postmortems: &postmortem.Builder{Ledger: s}, StreamPoll: 10 * time.Millisecond,
		Broker: &observability.Broker{Catalog: s, Audit: s, Endpoints: map[string]observability.Endpoint{"prom-prod": {URL: "http://127.0.0.1:1"}},
			Credentials: observability.Credentials{}, Local: map[string]observability.Factory{"ledger": impact.Ledger(s)}},
		Attestation: func() governance.Attestation {
			return governance.Evaluate(governance.Input{Tier: governance.TierLocal, Flags: governance.Flags{Engine: true}})
		}}
	mux := http.NewServeMux()
	a.Register(mux)
	roles := a.RouteRoles()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := auth.Principal{Tenant: r.Header.Get("X-Tenant"), Subject: r.Header.Get("X-User"), Method: "token"}
		for _, x := range strings.Split(r.Header.Get("X-Roles"), ",") {
			p.Roles = append(p.Roles, auth.Role(x))
		}
		_, pattern := mux.Handler(r)
		if need, ok := roles[pattern]; ok && !p.Has(need) {
			http.Error(w, `{"error":"forbidden","message":"missing role"}`, http.StatusForbidden)
			return
		}
		mux.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &v3env{t: t, srv: srv, store: s, api: a}
}

func (e *v3env) client(tenant, roles string) *workclient.Client {
	c := workclient.New(e.srv.URL, "")
	c.Header = http.Header{"X-Tenant": {tenant}, "X-User": {"u-" + roles}, "X-Roles": {roles}}
	return c
}

func status(err error) int {
	var apiErr *workclient.Error
	if errors.As(err, &apiErr) {
		return apiErr.Status
	}
	return 0
}

var goal = workclient.Goal{Intent: "restore search latency", SubjectRefs: []string{"service://prod/search-api"}}

// Every v3 route has a role; none is reachable without one.
func TestV3RoutesHaveRoles(t *testing.T) {
	e := newV3(t)
	roles := e.api.RouteRoles()
	for _, route := range []string{"GET /v1/work-graphs/{id}", "POST /v1/works/{id}/children", "GET /v1/observability/sources",
		"POST /v1/observability/sources", "POST /v1/observability/sources/{id}/probe", "POST /v1/observability/query",
		"GET /v1/observability/evidence/{id}", "POST /v1/work-templates/{id}/dry-run", "GET /v1/governance/attestation", "GET /v1/quotas"} {
		if _, ok := roles[route]; !ok {
			t.Errorf("route %s has no role", route)
		}
	}
	if roles["POST /v1/observability/sources"] != auth.Admin || roles["POST /v1/actions/{id}/break-glass/review"] != auth.Approver {
		t.Fatal("source changes need admin, break-glass reviews an approver")
	}
}

func TestV3GraphViewAndSDK(t *testing.T) {
	e := newV3(t)
	ctx := context.Background()
	op, viewer, other := e.client("t1", "operator"), e.client("t1", "viewer"), e.client("t2", "operator")
	parent, err := op.Works().Create(ctx, "incident-1", workclient.CreateWork{Kind: "incident", Title: "search p99", Goal: goal})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := viewer.CreateChild(ctx, parent.ID, "c0", workclient.CreateChild{}); status(err) != http.StatusForbidden {
		t.Fatalf("a viewer cannot create children: %v", err)
	}
	rca, err := op.CreateChild(ctx, parent.ID, "c1", workclient.CreateChild{Title: "RCA", Goal: workclient.Goal{Intent: "find the root cause"}})
	if err != nil || rca.ParentID != parent.ID || len(rca.Goal.SubjectRefs) != 1 {
		t.Fatalf("child: %+v %v", rca, err)
	}
	again, err := op.CreateChild(ctx, parent.ID, "c1", workclient.CreateChild{Title: "RCA", Goal: workclient.Goal{Intent: "find the root cause"}})
	if err != nil || again.ID != rca.ID {
		t.Fatalf("child creation must be idempotent: %v", err)
	}
	if _, err := op.CreateChild(ctx, parent.ID, "c2", workclient.CreateChild{Goal: workclient.Goal{Intent: "x",
		SubjectRefs: []string{"service://prod/billing"}}}); status(err) != http.StatusBadRequest {
		t.Fatalf("a child outside the parent's subjects: %v", err)
	}
	if _, err := other.CreateChild(ctx, parent.ID, "c3", workclient.CreateChild{}); status(err) != http.StatusNotFound {
		t.Fatalf("another tenant must not see the parent: %v", err)
	}
	review, err := op.CreateChild(ctx, parent.ID, "c4", workclient.CreateChild{Title: "Review", Goal: workclient.Goal{Intent: "review"},
		DependsOn: []orchestration.ChildDependency{{WorkID: rca.ID}}})
	if err != nil || review.WakeAt != nil {
		t.Fatalf("a dependent child must be held: %+v %v", review.WakeAt, err)
	}
	g, err := viewer.Graph(ctx, review.ID)
	if err != nil || g.RootID != parent.ID || len(g.Nodes) != 3 || len(g.Edges) != 1 {
		t.Fatalf("graph: %+v %v", g, err)
	}
	if _, err := other.Graph(ctx, parent.ID); status(err) != http.StatusNotFound {
		t.Fatalf("graph across tenants: %v", err)
	}
	v, err := viewer.View(ctx, review.ID)
	if err != nil || v.SchemaVersion != protocol.SchemaVersion || v.ContractVersion != protocol.ContractVersion || v.MinimumReaderVersion != protocol.MinimumReaderVersion || v.WaitingOn != "another Work" || len(v.Stages) == 0 || len(v.Controls) == 0 {
		t.Fatalf("view: %+v %v", v, err)
	}
	pv, _ := viewer.View(ctx, parent.ID)
	if len(pv.Children) != 2 {
		t.Fatalf("parent view children: %d", len(pv.Children))
	}
}

// §13.2: a client that reconnects with the last sequence it saw gets every
// persisted event exactly once, in order, in the stable contract.
func TestSubscribeResumesWithoutGapsOrDuplicates(t *testing.T) {
	e := newV3(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	op := e.client("t1", "operator,approver")
	w, err := op.Works().Create(ctx, "sub-1", workclient.CreateWork{Kind: "incident", Title: "x", Goal: goal})
	if err != nil {
		t.Fatal(err)
	}
	note := func(n int) {
		for i := 0; i < n; i++ {
			if err := e.store.NoteWork(ctx, "t1", w.ID, "work.related_event", "system", "test", map[string]any{"i": i}); err != nil {
				t.Fatal(err)
			}
		}
	}
	note(5)
	first, cancelFirst := context.WithCancel(ctx)
	events, _ := op.Works().Subscribe(first, w.ID, 0)
	var seen []int64
	for ev := range events {
		if ev.SchemaVersion != protocol.SchemaVersion || ev.ContractVersion != protocol.ContractVersion || ev.MinimumReaderVersion != protocol.MinimumReaderVersion || ev.TenantID != "t1" || ev.WorkID != w.ID {
			t.Fatalf("event: %+v", ev)
		}
		seen = append(seen, ev.Sequence)
		if len(seen) == 4 {
			cancelFirst() // the connection drops mid-stream
			break
		}
	}
	cancelFirst()
	note(3)
	if _, err := op.Command(ctx, w.ID, "cancel-1", work.CmdCancel, nil, "done", 0); err != nil {
		t.Fatal(err)
	}
	events, errs := op.Works().Subscribe(ctx, w.ID, seen[len(seen)-1])
	var last workclient.Event
	for ev := range events {
		seen = append(seen, ev.Sequence)
		last = ev
	}
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	for i, s := range seen {
		if s != int64(i+1) {
			t.Fatalf("sequence must be gap-free and without duplicates: %v", seen)
		}
	}
	snapshot, err := op.View(ctx, w.ID)
	if err != nil || last.Sequence != snapshot.LastSeq {
		t.Fatalf("the stream must drain the terminal transaction: last=%d durable=%d error=%v", last.Sequence, snapshot.LastSeq, err)
	}
	page, err := op.ContractEvents(ctx, w.ID, 0, 3)
	if err != nil || len(page) != 3 || page[0].Type != protocol.WorkCreated {
		t.Fatalf("contract page: %+v %v", page, err)
	}
	// Clients that ask for nothing keep the previous shape.
	old, err := op.Events(ctx, w.ID, 0, 1)
	if err != nil || len(old) != 1 || old[0].Type != "work.created" || old[0].Seq != 1 {
		t.Fatalf("legacy events changed: %+v %v", old, err)
	}
}

func TestV3SourcesQuotasIntentsDryRunRunbooks(t *testing.T) {
	e := newV3(t)
	ctx := context.Background()
	admin, op, viewer := e.client("t1", "admin"), e.client("t1", "operator"), e.client("t1", "viewer")
	src := workclient.Source{ID: "prom", Kind: observability.SignalMetrics, Backend: "prometheus", Environment: "prod",
		EndpointRef: "prom-prod", AllowedScope: []string{"service://prod"}, State: observability.StateActive,
		Queries: map[string]string{"error_rate": `rate(errors{service="${service}"}[5m])`}}
	if _, err := op.PutSource(ctx, src, 0, ""); status(err) != http.StatusForbidden {
		t.Fatalf("only admins register sources: %v", err)
	}
	bad := src
	bad.EndpointRef = "http-evil"
	if _, err := admin.PutSource(ctx, bad, 0, ""); status(err) != http.StatusBadRequest {
		t.Fatalf("an endpoint the server does not know: %v", err)
	}
	stored, err := admin.PutSource(ctx, src, 0, "first")
	if err != nil || stored.Version != 1 || stored.TenantID != "t1" || !stored.QueryCaps.ReadOnly {
		t.Fatalf("put source: %+v %v", stored, err)
	}
	if _, err := admin.PutSource(ctx, src, 7, ""); status(err) != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match: %v", err)
	}
	if list, err := e.client("t2", "viewer").Sources(ctx); err != nil || len(list) != 0 {
		t.Fatalf("another tenant sees the source: %v %v", list, err)
	}
	// The endpoint is closed: the probe reports it, the source degrades, and
	// neither the result nor the health carries the address.
	h, err := op.ProbeSource(ctx, "prom")
	if err != nil || h.OK || strings.Contains(h.Error, "127.0.0.1") {
		t.Fatalf("probe: %+v %v", h, err)
	}
	res, _, err := op.Observe(ctx, "", workclient.Query{SourceID: "prom", SubjectRef: "service://prod/search-api", QueryRef: "error_rate"})
	if err != nil || res.Status != observability.StatusUnavailable || res.Healthy() {
		t.Fatalf("query against a dead source: %+v %v", res, err)
	}
	res, _, _ = op.Observe(ctx, "", workclient.Query{SourceID: "prom", SubjectRef: "service://prod/x", Expression: "up"})
	if res.Status != observability.StatusDenied {
		t.Fatalf("an operator cannot send raw expressions: %s", res.Status)
	}

	att, err := viewer.Attestation(ctx)
	if err != nil || att.Stage != governance.G2 || att.Tier != "local" {
		t.Fatalf("attestation: %+v %v", att, err)
	}
	if _, err := viewer.Quotas(ctx); err != nil {
		t.Fatal(err)
	}
	in, err := viewer.RouteIntent(ctx, "排查 Redis 超时")
	if err != nil || !in.NeedsWork || in.Kind != work.KindIncident || in.Writes != work.WritesForbidden {
		t.Fatalf("intent: %+v %v", in, err)
	}
	plan, err := op.DryRun(ctx, "incident", goal)
	if err != nil || len(plan.Steps) == 0 || !plan.OK || !strings.Contains(plan.Closes, "human confirmation") {
		t.Fatalf("dry run: %+v %v", plan, err)
	}
	plan, err = op.DryRun(ctx, work.TemplateFollowUp, goal)
	if err != nil || plan.OK || !strings.Contains(strings.Join(plan.Problems, " "), "action runtime is off") {
		t.Fatalf("a template with an action stage while the runtime is off must name the problem: %+v %v", plan.Problems, err)
	}
	if ws, _ := op.ListWorks(ctx, workclient.ListFilter{}); len(ws) != 0 {
		t.Fatalf("a dry run created %d Work(s)", len(ws))
	}

	// Load shedding: the batch queue refuses new Works past its backlog;
	// an incident is always admitted.
	for i := 0; i < 2; i++ {
		if _, err := op.Works().Create(ctx, "batch-"+cpkit.NewID(), workclient.CreateWork{Kind: "routine", Title: "eval", Goal: goal,
			Queue: work.QueueBatch}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := op.Works().Create(ctx, "batch-over", workclient.CreateWork{Kind: "routine", Title: "eval", Goal: goal,
		Queue: work.QueueBatch}); status(err) != http.StatusTooManyRequests {
		t.Fatalf("the batch queue must shed: %v", err)
	}
	if _, err := op.Works().Create(ctx, "inc-over", workclient.CreateWork{Kind: "incident", Title: "p99", Goal: goal}); err != nil {
		t.Fatalf("an incident is never shed: %v", err)
	}

	if _, err := e.store.PutRunbook(ctx, "t1", runbook.Spec{ID: "incident.search-latency", Version: 3, Owner: "team-search",
		Safety: runbook.Safety{HumanApproval: true, RehearsalRequired: true}, Lifecycle: runbook.Lifecycle{TemplateID: "incident"}},
		runbook.Active, 0); err != nil {
		t.Fatal(err)
	}
	rb, err := e.store.GetRunbook(ctx, "t1", "incident.search-latency", 0)
	if err != nil || len(rb.Problems("", time.Now())) != 1 {
		t.Fatalf("an unrehearsed runbook reports it: %+v %v", rb.Problems("", time.Now()), err)
	}
}

func TestViewFailsWhenSnapshotSourcesFail(t *testing.T) {
	for _, source := range []string{"outcomes", "children"} {
		t.Run(source, func(t *testing.T) {
			e := newV3(t)
			ctx := context.Background()
			c := e.client("t1", "operator")
			parent, err := c.CreateWork(ctx, "snapshot-parent", workclient.CreateWork{Kind: "incident", Goal: goal})
			if err != nil {
				t.Fatal(err)
			}
			if source == "outcomes" {
				_, err = e.store.DB().ExecContext(ctx, `DROP TABLE work_outcomes`)
			} else {
				child, xerr := c.CreateChild(ctx, parent.ID, "snapshot-child", workclient.CreateChild{Goal: workclient.Goal{Intent: "investigate"}})
				if xerr != nil {
					t.Fatal(xerr)
				}
				_, err = e.store.DB().ExecContext(ctx, `UPDATE agent_works SET goal='{' WHERE id=?`, child.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.View(ctx, parent.ID); status(err) != http.StatusInternalServerError {
				t.Fatalf("partial snapshot reported success: %v", err)
			}
		})
	}
}

func TestProtocolMetadataOnEmptyPageAndStream(t *testing.T) {
	e := newV3(t)
	ctx := context.Background()
	c := e.client("t1", "operator")
	w, err := c.CreateWork(ctx, "metadata", workclient.CreateWork{Kind: "incident", Goal: goal})
	if err != nil {
		t.Fatal(err)
	}
	request := func(path string) *http.Response {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = c.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp := request("/v1/works/" + w.ID + "/events?protocol=1&after_seq=9999")
	var page protocol.EventPage
	err = json.NewDecoder(resp.Body).Decode(&page)
	resp.Body.Close()
	if err != nil || page.SchemaVersion != 1 || page.ContractVersion != 1 || page.MinimumReaderVersion != 1 || len(page.Items) != 0 {
		t.Fatalf("empty page metadata: %+v %v", page, err)
	}
	resp = request("/v1/works/" + w.ID + "/events/stream?protocol=1")
	for _, header := range []string{protocol.HeaderSchemaVersion, protocol.HeaderContractVersion, protocol.HeaderMinimumReaderVersion} {
		if resp.Header.Get(header) != "1" {
			t.Errorf("header %s=%q", header, resp.Header.Get(header))
		}
	}
	resp.Body.Close()
	resp = request("/v1/works/" + w.ID + "/events")
	var legacy map[string]json.RawMessage
	err = json.NewDecoder(resp.Body).Decode(&legacy)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := legacy["schema_version"]; ok {
		t.Fatal("legacy event page shape changed")
	}
}

// A terminal status event is not the transaction's final durable event.
func TestSubscribeDrainsTerminalTransaction(t *testing.T) {
	for _, terminal := range []work.Status{work.StatusCancelled, work.StatusSucceeded} {
		t.Run(string(terminal), func(t *testing.T) {
			e := newV3(t)
			ctx := context.Background()
			c := e.client("t1", "operator")
			w, err := c.Works().Create(ctx, "terminal-tail", workclient.CreateWork{Kind: "incident", Title: "tail", Goal: goal})
			if err != nil {
				t.Fatal(err)
			}
			liveCtx, liveCancel := context.WithTimeout(ctx, 3*time.Second)
			defer liveCancel()
			liveEvents, liveErrors := c.Subscribe(liveCtx, w.ID, 0)
			first, ok := <-liveEvents
			if !ok || first.Sequence != 1 {
				t.Fatalf("subscription failed to start: %+v", first)
			}
			if terminal == work.StatusCancelled {
				if _, err = c.Command(ctx, w.ID, "cancel-tail", work.CmdCancel, nil, "done", 0); err != nil {
					t.Fatal(err)
				}
			} else {
				for _, st := range []work.Status{work.StatusActive, work.StatusVerifying} {
					if _, err = e.store.ApplyWork(ctx, work.Mutation{TenantID: "t1", WorkID: w.ID, ActorType: "system", Status: &st}); err != nil {
						t.Fatal(err)
					}
				}
				now := time.Now().UTC()
				if _, err = e.store.ApplyWork(ctx, work.Mutation{TenantID: "t1", WorkID: w.ID, ActorType: "system", Status: &terminal, Now: now, Events: []work.NewEvent{{Type: "outcome.satisfied", Payload: map[string]any{"verdict": "SATISFIED"}}},
					Outcome: &outcome.Evaluation{SpecHash: "human", Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: now}}); err != nil {
					t.Fatal(err)
				}
			}
			snapshot, err := c.View(ctx, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			liveLast := first.Sequence
			for ev := range liveEvents {
				if ev.Sequence != liveLast+1 {
					t.Fatalf("live gap: %d -> %d", liveLast, ev.Sequence)
				}
				liveLast = ev.Sequence
			}
			if err := <-liveErrors; err != nil || liveCtx.Err() != nil || liveLast != snapshot.LastSeq {
				t.Fatalf("live last=%d durable=%d error=%v context=%v", liveLast, snapshot.LastSeq, err, liveCtx.Err())
			}
			for _, after := range []int64{0, snapshot.LastSeq} {
				subctx, cancel := context.WithTimeout(ctx, time.Second)
				events, errs := c.Subscribe(subctx, w.ID, after)
				last := after
				for ev := range events {
					if ev.Sequence != last+1 {
						t.Errorf("gap: %d -> %d", last, ev.Sequence)
					}
					last = ev.Sequence
				}
				suberr := <-errs
				expired := subctx.Err()
				cancel()
				if suberr != nil || expired != nil || last != snapshot.LastSeq {
					t.Fatalf("after=%d last=%d durable=%d error=%v deadline=%v", after, last, snapshot.LastSeq, suberr, expired)
				}
			}
		})
	}
}
