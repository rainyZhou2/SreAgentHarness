package kernel

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/verify"
)

// Performance defaults.
const (
	DefaultArtifactCacheBytes = 64 << 20
	maxCachedArtifactBytes    = 1 << 20
	DefaultToolTopK           = 30
	DefaultCompactTokens      = 16000
	speculationTTL            = 2 * time.Minute
	maxSpeculations           = 256
)

// ---------- artifact cache ----------

// cachedStore keeps recently written and read artifacts in memory. Refs are
// immutable, so a hit is always the stored content; entries are keyed by
// tenant, so another tenant's ref still goes to the store and is refused.
// It turns the per-step history rebuild from one read per earlier step into
// reads of new steps only.
type cachedStore struct {
	artifact.Store
	mu   sync.Mutex
	max  int64
	size int64
	ll   *list.List
	m    map[string]*list.Element
	obs  func(event string)
}

type cacheEntry struct {
	key  string
	data []byte
}

func newCachedStore(s artifact.Store, max int64, obs func(string)) artifact.Store {
	if max < 0 || s == nil {
		return s
	}
	if max == 0 {
		max = DefaultArtifactCacheBytes
	}
	return &cachedStore{Store: s, max: max, ll: list.New(), m: map[string]*list.Element{}, obs: obs}
}

func (c *cachedStore) Put(ctx context.Context, tenantID, kind string, data []byte) (string, string, error) {
	ref, hash, err := c.Store.Put(ctx, tenantID, kind, data)
	if err == nil {
		c.add(tenantID+"\x00"+ref, data)
	}
	return ref, hash, err
}

func (c *cachedStore) Get(ctx context.Context, tenantID, ref string) ([]byte, error) {
	key := tenantID + "\x00" + ref
	c.mu.Lock()
	if el, ok := c.m[key]; ok {
		c.ll.MoveToFront(el)
		data := bytes.Clone(el.Value.(*cacheEntry).data)
		c.mu.Unlock()
		c.obs("artifact_cache_hit")
		return data, nil
	}
	c.mu.Unlock()
	c.obs("artifact_cache_miss")
	data, err := c.Store.Get(ctx, tenantID, ref)
	if err == nil {
		c.add(key, data)
	}
	return data, err
}

func (c *cachedStore) add(key string, data []byte) {
	n := int64(len(data))
	if n > maxCachedArtifactBytes || n > c.max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	c.m[key] = c.ll.PushFront(&cacheEntry{key: key, data: bytes.Clone(data)})
	c.size += n
	for c.size > c.max {
		el := c.ll.Back()
		e := el.Value.(*cacheEntry)
		c.ll.Remove(el)
		delete(c.m, e.key)
		c.size -= int64(len(e.data))
	}
}

// ---------- phase timing ----------

// LoopObserver is the optional part of Observer for the agent loop: time
// per step phase (queue, context, model, review, tool, commit) and loop
// events (repairs, guards, batches, cache and speculation hits, routing).
type LoopObserver interface {
	ObservePhase(kind, phase string, d time.Duration)
	ObserveLoop(event string)
}

type phases struct {
	mu sync.Mutex
	d  map[string]time.Duration
}

func (p *phases) add(phase string, d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.d == nil {
		p.d = map[string]time.Duration{}
	}
	p.d[phase] += d
}

// since records the time from t to now under phase and returns now.
func (p *phases) since(phase string, t time.Time) time.Time {
	now := time.Now()
	p.add(phase, now.Sub(t))
	return now
}

func (p *phases) millis() map[string]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int64, len(p.d))
	for k, v := range p.d {
		out[k] = v.Milliseconds()
	}
	return out
}

func (w *Worker) observePhases(kind string, p *phases) {
	lo, ok := w.cfg.Observer.(LoopObserver)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.d {
		lo.ObservePhase(kind, k, v)
	}
}

func (w *Worker) loopEvent(event string) {
	if lo, ok := w.cfg.Observer.(LoopObserver); ok {
		lo.ObserveLoop(event)
	}
}

// ---------- speculative read-only calls ----------

// speculation holds read-only tool calls started while the model was still
// streaming its response. The tool step that runs the same call (same tool
// version, risk and canonical arguments) takes the result instead of
// calling again; an unused result expires. Only read-only calls the policy
// allows without approval are started, so an unused one has no effect.
type speculation struct {
	mu sync.Mutex
	m  map[string]*specEntry
}

type specEntry struct {
	done    chan struct{}
	out     json.RawMessage
	err     error
	version string
	risk    domain.RiskClass
	at      time.Time
}

func specKey(tenantID, runID, name string, args json.RawMessage) string {
	return tenantID + "\x00" + runID + "\x00" + callKey(name, args)
}

// start runs fn unless the same call is already held. It reports whether it
// started.
func (s *speculation) start(key, version string, risk domain.RiskClass, fn func() (json.RawMessage, error)) bool {
	s.mu.Lock()
	if s.m == nil {
		s.m = map[string]*specEntry{}
	}
	now := time.Now()
	for k, e := range s.m {
		if now.Sub(e.at) > speculationTTL {
			delete(s.m, k)
		}
	}
	if _, ok := s.m[key]; ok || len(s.m) >= maxSpeculations {
		s.mu.Unlock()
		return false
	}
	e := &specEntry{done: make(chan struct{}), version: version, risk: risk, at: now}
	s.m[key] = e
	s.mu.Unlock()
	go func() {
		defer close(e.done)
		e.out, e.err = fn()
	}()
	return true
}

// take returns a speculative result for the call, waiting for one still in
// flight. It reports false when there is none, it failed, or the tool
// changed since it was started.
func (s *speculation) take(ctx context.Context, key, version string, risk domain.RiskClass) (json.RawMessage, bool) {
	s.mu.Lock()
	e, ok := s.m[key]
	if ok {
		delete(s.m, key)
	}
	s.mu.Unlock()
	if !ok {
		return nil, false
	}
	select {
	case <-e.done:
	case <-ctx.Done():
		return nil, false
	}
	if e.err != nil || e.version != version || e.risk != risk || time.Since(e.at) > speculationTTL {
		return nil, false
	}
	return e.out, true
}

// speculator returns the callback that starts eligible calls of a streaming
// response, or nil when speculation is off.
func (w *Worker) speculator(ctx context.Context, lease domain.Lease, rules profile.Rules, in cognitive.ReasoningInput,
	visible []tools.Descriptor, ls loopState, l loopLimits) func(string, json.RawMessage) {
	if !w.cfg.SpeculativeTools || in.ForceConclude {
		return nil
	}
	return func(name string, args json.RawMessage) {
		if name == cognitive.ConclusionTool || name == BatchToolName || name == LoadToolsName ||
			name == verify.ReviewToolName || !visibleTool(visible, name) || !json.Valid(args) {
			return
		}
		t, ok := w.tools.LookupFor(lease.TenantID, name)
		if !ok {
			return
		}
		raw := t.Descriptor()
		args = fillTimeArgs(&raw, args, in.Time)
		call := domain.ToolCall{Name: name, Args: args}
		if ls.refuse(call, l.maxRepeats) != "" {
			return
		}
		pd, eff := rules.DecideCall(ctx, w.policy, profile.Call{TenantID: lease.TenantID, RunID: lease.RunID,
			Name: name, Args: args}, &raw)
		if eff == nil || pd.Verdict != policy.Allow || eff.Risk.HasSideEffects() {
			return
		}
		desc := *eff
		key := specKey(lease.TenantID, lease.RunID, name, args)
		started := w.spec.start(key, desc.Version, desc.Risk, func() (json.RawMessage, error) {
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.StepTimeout)
			defer cancel()
			select {
			case w.toolSem <- struct{}{}:
			case <-sctx.Done():
				return nil, sctx.Err()
			}
			defer func() { <-w.toolSem }()
			sctx = tools.WithCallInfo(sctx, tools.CallInfo{TenantID: lease.TenantID, RunID: lease.RunID, StepID: lease.StepID,
				AttemptID: lease.AttemptID, FenceToken: lease.FenceToken, ContextSnapshotID: lease.ContextSnapshotID,
				InputRef: lease.InputRef, Deadline: time.Now().Add(w.cfg.StepTimeout)})
			out, err := t.Invoke(sctx, "spec:"+artifact.Hash([]byte(key)), args)
			outcome := "speculative_ok"
			if err != nil {
				outcome = "speculative_error"
			}
			w.cfg.Observer.ObserveTool(desc.Source, string(desc.Risk), outcome)
			return out, err
		})
		if started {
			w.loopEvent("speculation_started")
		}
	}
}

// takeSpeculative returns the result of a call already started while the
// model streamed, if any.
func (w *Worker) takeSpeculative(ctx context.Context, lease domain.Lease, desc tools.Descriptor, name string, args json.RawMessage) (json.RawMessage, bool) {
	if !w.cfg.SpeculativeTools || desc.Risk.HasSideEffects() {
		return nil, false
	}
	out, ok := w.spec.take(ctx, specKey(lease.TenantID, lease.RunID, name, args), desc.Version, desc.Risk)
	if ok {
		w.loopEvent("speculation_hit")
	}
	return out, ok
}

// ---------- tool selection ----------

// LoadToolsName is the builtin that adds tools listed under [more tools]
// to the schemas the model sees.
const LoadToolsName = tools.BuiltinPrefix + "load_tools"

// LoadToolsTool reports which of the requested tools exist; the kernel
// shows the schemas of the loaded ones from the next step on.
type LoadToolsTool struct{ Registry *tools.Registry }

func (LoadToolsTool) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: LoadToolsName, Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
		Description: "Load the schemas of tools listed under [more tools] so you can call them from the next step. Pass the exact names.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"names":{"type":"array","items":{"type":"string"},"maxItems":20}},"required":["names"]}`)}
}

func (l LoadToolsTool) Invoke(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Names []string `json:"names"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("arguments: %w", err)
	}
	info, _ := tools.CallInfoFrom(ctx)
	out := map[string][]string{"loaded": {}, "unknown": {}}
	for _, n := range in.Names {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		if _, ok := l.Registry.LookupFor(info.TenantID, n); ok && !tools.IsBuiltin(n) {
			out["loaded"] = append(out["loaded"], n)
		} else {
			out["unknown"] = append(out["unknown"], n)
		}
	}
	return json.Marshal(out)
}

// loadedTools reads the names a load_tools observation loaded.
func loadedTools(content json.RawMessage) []string {
	var r struct {
		Loaded []string `json:"loaded"`
	}
	_ = json.Unmarshal(content, &r)
	return r.Loaded
}

// selectTools picks the tools whose schemas the model sees when there are
// more than k. The choice depends only on the goal, the user's messages and
// the tools used or loaded in this run, so it is stable from step to step
// and keeps the prompt-cache prefix warm. Tools used or loaded, builtins
// and tools named in the request always stay; the rest are ranked by word
// overlap with the request. The others are listed by name and can be
// loaded with harness.load_tools.
func selectTools(visible []tools.Descriptor, request string, keep map[string]bool, k int) ([]tools.Descriptor, []cognitive.ToolHint) {
	hasLoader := false
	for _, d := range visible {
		if d.Name == LoadToolsName {
			hasLoader = true
		}
	}
	if k <= 0 || len(visible) <= k || !hasLoader {
		out := make([]tools.Descriptor, 0, len(visible))
		for _, d := range visible {
			if d.Name != LoadToolsName {
				out = append(out, d)
			}
		}
		return out, nil
	}
	req := strings.ToLower(request)
	words := cognitive.Tokens(request)
	type ranked struct {
		d      tools.Descriptor
		pinned bool
		score  int
	}
	rs := make([]ranked, 0, len(visible))
	for _, d := range visible {
		r := ranked{d: d}
		switch {
		case tools.IsBuiltin(d.Name), keep[d.Name], strings.Contains(req, strings.ToLower(d.Name)):
			r.pinned = true
		}
		for t := range cognitive.Tokens(d.Name) {
			if words[t] {
				r.score += 3
			}
		}
		for t := range cognitive.Tokens(d.Description) {
			if words[t] {
				r.score++
			}
		}
		rs = append(rs, r)
	}
	sort.SliceStable(rs, func(i, j int) bool {
		if rs[i].pinned != rs[j].pinned {
			return rs[i].pinned
		}
		if rs[i].score != rs[j].score {
			return rs[i].score > rs[j].score
		}
		return rs[i].d.Name < rs[j].d.Name
	})
	var shown []tools.Descriptor
	var more []cognitive.ToolHint
	for _, r := range rs {
		if r.pinned || len(shown) < k {
			shown = append(shown, r.d)
			continue
		}
		more = append(more, cognitive.ToolHint{Name: r.d.Name, Summary: firstSentence(r.d.Description, 90)})
	}
	if len(more) == 0 {
		return selectTools(visible, request, keep, 0)
	}
	sort.Slice(shown, func(i, j int) bool { return shown[i].Name < shown[j].Name })
	sort.Slice(more, func(i, j int) bool { return more[i].Name < more[j].Name })
	return shown, more
}

func firstSentence(s string, n int) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, ".\n。"); i > 0 {
		s = s[:i]
	}
	if r := []rune(s); len(r) > n {
		s = string(r[:n]) + "…"
	}
	return s
}

// requestText is what tool selection ranks against.
func requestText(in cognitive.ReasoningInput) string {
	parts := []string{in.Goal}
	for _, m := range in.Conversation {
		if m.Role == "user" {
			parts = append(parts, m.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// ---------- convergence and routing ----------

// convergence reports when the ledger has settled: a VERIFIED leading
// hypothesis with high confidence and no competing hypothesis left open.
func convergence(ledger []cognitive.LedgerEntry) (string, bool) {
	var lead *cognitive.LedgerEntry
	for i := range ledger {
		e := &ledger[i]
		if e.Status != "REFUTED" && (lead == nil || e.Confidence > lead.Confidence) {
			lead = e
		}
	}
	if lead == nil || lead.Status != "VERIFIED" || lead.Confidence < 0.8 {
		return "", false
	}
	for _, e := range ledger {
		if e.ID != lead.ID && e.Status != "REFUTED" && e.Status != "VERIFIED" && e.Confidence >= 0.2 {
			return "", false
		}
	}
	return lead.ID, true
}

// modelAlias picks the model alias of this reasoning call: the escalated
// alias for the steps that decide the outcome (first analysis, likely or
// forced conclusion, a stall, after a rejected conclusion, a repair), the
// routine alias otherwise. "" keeps the strategy's own alias.
func (w *Worker) modelAlias(in cognitive.ReasoningInput, stalled, converged bool) (string, string) {
	routine, escalated := w.cfg.RoutineModelAlias, w.cfg.EscalatedModelAlias
	if routine == "" && escalated == "" {
		return "", ""
	}
	reason := ""
	switch {
	case in.Repair != "":
		reason = "repair"
	case !in.HasToolSteps:
		reason = "first_step"
	case in.ForceConclude:
		reason = "conclude"
	case converged:
		reason = "conclusion_likely"
	case stalled:
		reason = "stalled"
	case len(in.Observations) > 0 && in.Observations[len(in.Observations)-1].Tool == verify.ReviewToolName:
		reason = "backtrack"
	}
	if reason != "" && escalated != "" {
		return escalated, "escalated:" + reason
	}
	if routine != "" {
		return routine, "routine"
	}
	return "", ""
}

// withAlias returns the strategy with another model alias when it is a
// model strategy.
func withAlias(s cognitive.Strategy, alias string) cognitive.Strategy {
	if alias == "" {
		return s
	}
	if llm, ok := s.(*cognitive.LLM); ok && llm.Alias != alias {
		c := *llm
		c.Alias = alias
		return &c
	}
	return s
}

// ---------- token-budget compaction ----------

// compactTokens is the estimated size of uncompacted tool results above
// which history is compacted regardless of the step count.
func (w *Worker) compactTokens() int {
	switch {
	case w.cfg.CompactTokens < 0:
		return 0
	case w.cfg.CompactTokens == 0:
		return DefaultCompactTokens
	}
	return w.cfg.CompactTokens
}

// overTokenBudget reports whether the tool results since the last
// compaction, plus pending bytes, exceed the compaction token budget.
func (w *Worker) overTokenBudget(ctx context.Context, lease domain.Lease, pending int) bool {
	limit := w.compactTokens()
	if limit <= 0 {
		return false
	}
	hist, err := w.store.History(ctx, lease.TenantID, lease.RunID, lease.StepSeq)
	if err != nil {
		return false
	}
	total := pending
	for i := len(hist) - 1; i >= 0 && total/3 <= limit; i-- {
		s := hist[i]
		if s.Kind == domain.StepCompact && s.Status == domain.StepSucceeded {
			break
		}
		if s.Kind != domain.StepTool || s.OutputRef == nil {
			continue
		}
		if data, err := w.artifacts.Get(ctx, lease.TenantID, *s.OutputRef); err == nil {
			total += len(data)
		}
	}
	return total/3 > limit // bytes ≈ 3 × tokens, as in the assembler
}

// withTokenCompaction puts a compaction ahead of the next reason step when
// the uncompacted results are over the token budget.
func (w *Worker) withTokenCompaction(ctx context.Context, lease domain.Lease, plan []domain.PlannedStep, pending int) []domain.PlannedStep {
	for _, p := range plan {
		if p.Kind == domain.StepCompact || p.Kind != domain.StepReason && p.Status != domain.StepSkipped {
			return plan // already compacting, or a follow-up step comes first
		}
	}
	if !w.overTokenBudget(ctx, lease, pending) {
		return plan
	}
	out := make([]domain.PlannedStep, 0, len(plan)+1)
	for _, p := range plan {
		if p.Kind == domain.StepReason {
			out = append(out, domain.PlannedStep{Kind: domain.StepCompact, Executor: "compactor", InputRef: p.InputRef,
				InputHash: p.InputHash, Status: domain.StepPending})
		}
		out = append(out, p)
	}
	w.loopEvent("compaction_tokens")
	return out
}
