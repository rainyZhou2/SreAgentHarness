// Package runsearch searches the tenant's past runs: their goals, conclusions
// and hypotheses. Transcripts are the raw record behind every memory; letting
// an agent search them lets it ask "have we seen this before, and how did it
// end?" without anyone having promoted that experience to a memory first.
//
// It needs no schema change. It scans the most recent finished runs, reads
// each run's goal and result once (a finished run never changes, so the text
// is cached), and ranks by weighted term overlap with a rarity weight
// computed over the scanned runs plus a small recency boost. The scan is
// bounded (MaxScan runs, a time budget), and a result says when it was cut
// short. A database full-text index would remove the bound; this keeps the
// local SQLite store and PostgreSQL on one implementation.
package runsearch

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

// Source lists runs (implemented by both stores).
type Source interface {
	ListRuns(ctx context.Context, tenantID string, status domain.RunStatus, limit int) ([]domain.Run, error)
}

// Artifacts reads a run's goal and result documents.
type Artifacts interface {
	Get(ctx context.Context, tenantID, ref string) ([]byte, error)
}

// Query selects and ranks runs.
type Query struct {
	TenantID string
	Text     string
	Service  string
	// Status is COMPLETED (default), FAILED, CANCELLED or "any" (all finished).
	Status string
	Limit  int
	// Caller scopes visibility: a user's chats are visible only to that user.
	Caller Caller
}

// Caller is the run on whose behalf the search runs.
type Caller struct {
	RunID         string
	InitiatorType string
	InitiatorID   string
}

// Hypothesis is a concluded hypothesis of a past run.
type Hypothesis struct {
	Statement  string  `json:"statement"`
	Status     string  `json:"status"`
	Confidence float64 `json:"confidence,omitempty"`
}

// Hit is one past run.
type Hit struct {
	Ref        string       `json:"ref"`
	RunID      string       `json:"run_id"`
	Status     string       `json:"status"`
	At         time.Time    `json:"at"`
	Goal       string       `json:"goal"`
	Answer     string       `json:"answer,omitempty"`
	Hypotheses []Hypothesis `json:"hypotheses,omitempty"`
	Score      float64      `json:"score"`
	Matched    []string     `json:"matched_terms,omitempty"`
}

// Result is a search outcome.
type Result struct {
	Hits    []Hit `json:"hits"`
	Scanned int   `json:"scanned_runs"`
	// Partial is true when the scan hit its run or time bound: older runs
	// were not searched.
	Partial bool `json:"partial,omitempty"`
}

// Config bounds the search.
type Config struct {
	MaxScan     int           // finished runs scanned per search (default 300)
	Budget      time.Duration // time budget per search (default 6s)
	Concurrency int           // artifact reads in flight (default 8)
	CacheSize   int           // cached run documents (default 4000)
}

func (c *Config) defaults() {
	if c.MaxScan <= 0 {
		c.MaxScan = 300
	}
	if c.Budget <= 0 {
		c.Budget = 6 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.CacheSize <= 0 {
		c.CacheSize = 4000
	}
}

// Searcher searches past runs. Safe for concurrent use.
type Searcher struct {
	Runs      Source
	Artifacts Artifacts
	Cfg       Config
	Now       func() time.Time

	mu    sync.Mutex
	cache map[string]*doc // key: tenant/run
	order []string
}

type doc struct {
	goal, answer string
	services     []string
	hyps         []Hypothesis // first few, for search hits
	allHyps      []Hypothesis
	verdict      string // independent verification verdict ("" when none)
	goalTok      map[string]int
	ansTok       map[string]int
	hypTok       map[string]int
	loaded       bool
}

const (
	maxGoalRunes   = 300
	maxAnswerRunes = 700
)

func (s *Searcher) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Search ranks the visible finished runs against the query.
func (s *Searcher) Search(ctx context.Context, q Query) (Result, error) {
	s.Cfg.defaults()
	limit := q.Limit
	if limit <= 0 || limit > 10 {
		limit = 5
	}
	terms := uniq(memory.Tokenize(q.Text))
	if len(terms) == 0 {
		return Result{Hits: []Hit{}}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.Cfg.Budget)
	defer cancel()

	var runs []domain.Run
	for _, st := range statuses(q.Status) {
		// One more than the bound, so "there are older runs" is observable.
		rs, err := s.Runs.ListRuns(ctx, q.TenantID, st, s.Cfg.MaxScan+1)
		if err != nil {
			return Result{}, err
		}
		runs = append(runs, rs...)
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt.After(runs[j].CreatedAt) })
	res := Result{Hits: []Hit{}}
	if len(runs) > s.Cfg.MaxScan {
		runs, res.Partial = runs[:s.Cfg.MaxScan], true
	}
	visible := runs[:0]
	for _, r := range runs {
		if visibleTo(q.Caller, r) {
			visible = append(visible, r)
		}
	}
	docs := s.load(ctx, q.TenantID, visible)
	if ctx.Err() != nil {
		res.Partial = true
	}
	res.Scanned = len(docs)

	// Rarity weights over the scanned runs.
	df := map[string]int{}
	for _, d := range docs {
		for _, t := range terms {
			if d.goalTok[t] > 0 || d.ansTok[t] > 0 || d.hypTok[t] > 0 {
				df[t]++
			}
		}
	}
	svc := strings.ToLower(strings.TrimSpace(q.Service))
	n := float64(len(docs))
	for _, d := range docs {
		if svc != "" && !d.mentions(svc) {
			continue
		}
		score, matched := 0.0, []string{}
		for _, t := range terms {
			tf := 2*float64(d.goalTok[t]) + 1.5*float64(d.hypTok[t]) + float64(d.ansTok[t])
			if tf == 0 {
				continue
			}
			idf := math.Log(1 + n/(1+float64(df[t])))
			score += idf * (1 + math.Log(1+tf))
			matched = append(matched, t)
		}
		if score == 0 {
			continue
		}
		age := s.now().Sub(d.run.CreatedAt).Hours() / 24
		score *= 1 + 0.2*math.Exp(-age/30)
		redGoal, _ := memory.Redact(d.goal)
		redAns, _ := memory.Redact(d.answer)
		hits := Hit{Ref: "run:" + d.run.ID, RunID: d.run.ID, Status: string(d.run.Status), At: d.run.CreatedAt,
			Goal: clip(redGoal, maxGoalRunes), Answer: clip(redAns, maxAnswerRunes), Hypotheses: d.hyps,
			Score: math.Round(score*100) / 100, Matched: matched}
		res.Hits = append(res.Hits, hits)
	}
	sort.SliceStable(res.Hits, func(i, j int) bool {
		if res.Hits[i].Score != res.Hits[j].Score {
			return res.Hits[i].Score > res.Hits[j].Score
		}
		return res.Hits[i].At.After(res.Hits[j].At)
	})
	if len(res.Hits) > limit {
		res.Hits = res.Hits[:limit]
	}
	return res, nil
}

func statuses(s string) []domain.RunStatus {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "FAILED":
		return []domain.RunStatus{domain.RunFailed}
	case "CANCELLED":
		return []domain.RunStatus{domain.RunCancelled}
	case "ANY":
		return []domain.RunStatus{domain.RunCompleted, domain.RunFailed, domain.RunCancelled}
	}
	return []domain.RunStatus{domain.RunCompleted}
}

// visibleTo: a user's own runs and every non-user run (triggers, schedules,
// Works) are visible; other users' chats are not. Child runs of a flow are
// skipped (the parent carries the conclusion), and so is the caller itself.
func visibleTo(c Caller, r domain.Run) bool {
	if r.ID == c.RunID || r.ParentRunID != nil {
		return false
	}
	if r.InitiatorType == "user" {
		return c.InitiatorType == "user" && c.InitiatorID != "" && r.InitiatorID != nil && *r.InitiatorID == c.InitiatorID
	}
	return true
}

type scanned struct {
	*doc
	run domain.Run
}

// load returns the searchable text of runs, reading uncached ones with
// bounded concurrency until the context ends.
func (s *Searcher) load(ctx context.Context, tenant string, runs []domain.Run) []scanned {
	out := make([]scanned, len(runs))
	ok := make([]bool, len(runs))
	sem := make(chan struct{}, s.Cfg.Concurrency)
	var wg sync.WaitGroup
	for i, r := range runs {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			if d := s.docFor(ctx, tenant, r); d != nil {
				out[i], ok[i] = scanned{doc: d, run: r}, true
			}
		}()
	}
	wg.Wait()
	kept := out[:0]
	for i := range out {
		if ok[i] {
			kept = append(kept, out[i])
		}
	}
	return kept
}

func (s *Searcher) docFor(ctx context.Context, tenant string, r domain.Run) *doc {
	key := tenant + "/" + r.ID
	s.mu.Lock()
	if d, hit := s.cache[key]; hit {
		s.mu.Unlock()
		return d
	}
	s.mu.Unlock()
	d := &doc{}
	if data, err := s.Artifacts.Get(ctx, tenant, r.GoalRef); err == nil {
		var g struct {
			Goal    string `json:"goal"`
			Context *struct {
				Services []string `json:"services"`
			} `json:"context"`
		}
		if json.Unmarshal(data, &g) == nil {
			d.goal = g.Goal
			if g.Context != nil {
				d.services = g.Context.Services
			}
		}
	} else if ctx.Err() != nil {
		return nil // out of time: do not cache an empty document
	}
	if r.ResultRef != nil {
		if data, err := s.Artifacts.Get(ctx, tenant, *r.ResultRef); err == nil {
			var res struct {
				Answer     string `json:"answer"`
				Hypotheses []struct {
					Statement  string  `json:"statement"`
					Status     string  `json:"status"`
					Confidence float64 `json:"confidence_estimate"`
				} `json:"hypotheses"`
				Verification struct {
					Verdict string `json:"verdict"`
				} `json:"independent_verification"`
			}
			if json.Unmarshal(data, &res) == nil {
				d.answer = res.Answer
				d.verdict = res.Verification.Verdict
				for _, h := range res.Hypotheses {
					hy := Hypothesis{h.Statement, h.Status, h.Confidence}
					d.allHyps = append(d.allHyps, hy)
					if len(d.hyps) < 4 {
						d.hyps = append(d.hyps, hy)
					}
				}
			}
		} else if ctx.Err() != nil {
			return nil
		}
	}
	d.goalTok = count(memory.Tokenize(d.goal + " " + strings.Join(d.services, " ")))
	d.ansTok = count(memory.Tokenize(d.answer))
	var hs []string
	for _, h := range d.hyps {
		hs = append(hs, h.Statement)
	}
	d.hypTok = count(memory.Tokenize(strings.Join(hs, " ")))
	d.loaded = true
	s.mu.Lock()
	if s.cache == nil {
		s.cache = map[string]*doc{}
	}
	if _, dup := s.cache[key]; !dup {
		s.order = append(s.order, key)
		if len(s.order) > s.Cfg.CacheSize {
			delete(s.cache, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.cache[key] = d
	s.mu.Unlock()
	return d
}

func (d *doc) mentions(svc string) bool {
	if strings.Contains(strings.ToLower(d.goal), svc) || strings.Contains(strings.ToLower(d.answer), svc) {
		return true
	}
	for _, s := range d.services {
		if strings.EqualFold(s, svc) {
			return true
		}
	}
	return false
}

func count(ts []string) map[string]int {
	m := make(map[string]int, len(ts))
	for _, t := range ts {
		m[t]++
	}
	return m
}

func uniq(ts []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range ts {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n]) + "…"
}

// Outcome is a finished run as a verifier of memory sees it: what it
// concluded about each hypothesis, and whether an independent check
// supported the conclusion.
type Outcome struct {
	RunID      string
	At         time.Time
	Goal       string
	Answer     string
	Services   []string
	Hypotheses []Hypothesis
	// Verdict is the independent verification verdict (SUPPORTED, ...), or
	// "" when the run had none.
	Verdict string
}

// Outcomes returns the finished (COMPLETED) runs created after since, newest
// first, at most max of them (default MaxScan), with their full hypothesis
// lists. Child runs are skipped. Unlike Search it applies no per-user
// visibility: it is for the framework's own maintenance (memory curation),
// never for an agent.
func (s *Searcher) Outcomes(ctx context.Context, tenantID string, since time.Time, max int) ([]Outcome, error) {
	s.Cfg.defaults()
	if max <= 0 || max > s.Cfg.MaxScan {
		max = s.Cfg.MaxScan
	}
	ctx, cancel := context.WithTimeout(ctx, s.Cfg.Budget*4)
	defer cancel()
	runs, err := s.Runs.ListRuns(ctx, tenantID, domain.RunCompleted, max)
	if err != nil {
		return nil, err
	}
	var keep []domain.Run
	for _, r := range runs {
		if r.ParentRunID == nil && r.CreatedAt.After(since) {
			keep = append(keep, r)
		}
	}
	docs := s.load(ctx, tenantID, keep)
	out := make([]Outcome, 0, len(docs))
	for _, d := range docs {
		out = append(out, Outcome{RunID: d.run.ID, At: d.run.CreatedAt, Goal: d.goal, Answer: d.answer, Services: d.services,
			Hypotheses: d.allHyps, Verdict: d.verdict})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return out, nil
}
