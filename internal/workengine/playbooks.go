package workengine

import (
	"context"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/playbook"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
)

// versionPredicates are world facts read as component versions for memory
// applicability (fact predicate -> component name).
var versionPredicates = map[string]string{
	"kubelet_version": "kubernetes", "kubernetes_version": "kubernetes", "driver_version": "driver",
	"cuda_version": "cuda", "serving_engine_version": "engine", "version": "service",
}

// wirePlaybooks enables Playbook proposals and outcome feedback when the
// kernel store also keeps memories (pg and the local store do).
func (e *Engine) wirePlaybooks(o Options) {
	ml, ok := o.Server.Store.(playbook.MemoryLedger)
	if !ok {
		return
	}
	emb := o.Embedder
	if emb == nil {
		emb = memory.HashingEmbedder{}
	}
	writer := memory.Writer{Embedder: emb, Artifacts: o.Server.Artifacts}
	svc := &playbook.Service{Works: o.Store, Memories: ml, Clock: o.Store.Clock, Log: e.log}
	if o.Server.Artifacts != nil {
		svc.Prepare = writer.Prepare
	}
	e.Playbooks = svc
	e.API.Playbooks = svc
	onClose := func(ctx context.Context, w work.Work) {
		// Detached and bounded: feedback never slows or fails the commit;
		// the sweep retries anything lost here.
		go func() {
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if n, err := svc.WorkClosed(fctx, w); err != nil {
				e.log.Warn("playbook outcome feedback failed", "work_id", w.ID, "err", err)
			} else if n > 0 {
				e.log.Info("playbook outcomes recorded", "work_id", w.ID, "memories", n, "status", w.Status)
			}
		}()
	}
	e.Orch.OnClose = onClose
	e.Service.OnClose = onClose
}

// sweepPlaybooks re-applies outcome feedback for recently closed Works.
func (e *Engine) sweepPlaybooks(ctx context.Context) {
	const every = 10 * time.Minute
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		since := e.World.Clock.Now().Add(-24 * time.Hour)
		if n, err := e.Playbooks.Sweep(ctx, e.Orch.Store, since, 500); err != nil && ctx.Err() == nil {
			e.log.Warn("playbook sweep failed", "err", err)
		} else if n > 0 {
			e.log.Info("playbook sweep recorded outcomes", "memories", n)
		}
	}
}

// MemoryEnv derives the memory-retrieval environment of a run from the
// Work it belongs to: environment label, subject names and kinds, and
// component versions from World facts. Attach it as memory.Retriever.Env.
func (e *Engine) MemoryEnv(ctx context.Context, tenantID, runID string) (memory.Env, bool) {
	if runID == "" {
		return memory.Env{}, false
	}
	link, ok, err := e.Orch.Store.WorkForRun(ctx, tenantID, runID)
	if err != nil || !ok {
		return memory.Env{}, false
	}
	w, err := e.Orch.Store.GetWork(ctx, tenantID, link.WorkID)
	if err != nil {
		return memory.Env{}, false
	}
	env := memory.Env{Environment: w.Goal.Labels["environment"], Versions: map[string]string{}}
	kinds := map[string]bool{}
	for _, ref := range w.Goal.SubjectRefs {
		kind, rest, ok := strings.Cut(ref, "://")
		if !ok {
			continue
		}
		if !kinds[kind] {
			kinds[kind] = true
			env.SubjectKinds = append(env.SubjectKinds, kind)
		}
		if i := strings.LastIndex(rest, "/"); i >= 0 {
			rest = rest[i+1:]
		}
		env.Services = append(env.Services, rest)
	}
	preds := make([]string, 0, len(versionPredicates))
	for p := range versionPredicates {
		preds = append(preds, p)
	}
	now := e.World.Clock.Now()
	facts, err := e.World.Store.FactsAt(ctx, tenantID, w.Goal.SubjectRefs, preds, now, now)
	if err == nil {
		for _, f := range facts {
			if c := versionPredicates[f.Predicate]; c != "" && env.Versions[c] == "" {
				env.Versions[c] = f.Value
			}
		}
	}
	return env, true
}

// stepLister is the kernel store's step listing (pg and local stores).
type stepLister interface {
	ListSteps(ctx context.Context, tenantID, runID string) ([]domain.Step, error)
}

// wireEval configures the Work scorecard evaluator (§14.4).
func (e *Engine) wireEval(o Options) {
	ev := &workeval.Evaluator{Ledger: o.Store}
	if sl, ok := o.Server.Store.(stepLister); ok {
		ev.ToolCalls = func(ctx context.Context, tenantID, runID string) (int, error) {
			steps, err := sl.ListSteps(ctx, tenantID, runID)
			n := 0
			for _, st := range steps {
				if st.Kind == domain.StepTool {
					n++
				}
			}
			return n, err
		}
	}
	if ml, ok := o.Server.Store.(playbook.MemoryLedger); ok {
		ev.MemoryMatch = func(ctx context.Context, w work.Work) (int, int, error) {
			links, err := o.Store.ListRunLinks(ctx, w.TenantID, w.ID)
			if err != nil {
				return 0, 0, err
			}
			now := o.Store.Clock.Now()
			seen := map[string]bool{}
			matched, total := 0, 0
			for _, l := range links {
				ids, err := ml.RunMemoryIDs(ctx, w.TenantID, l.RunID)
				if err != nil {
					return matched, total, err
				}
				env, _ := e.MemoryEnv(ctx, w.TenantID, l.RunID)
				for _, id := range ids {
					if seen[id] {
						continue
					}
					seen[id] = true
					m, err := ml.GetMemory(ctx, w.TenantID, id)
					if err != nil || m.Applicability == nil {
						continue
					}
					total++
					if ok, _ := m.Applies(env, now); ok {
						matched++
					}
				}
			}
			return matched, total, nil
		}
	}
	e.Eval = ev
	e.API.Eval = ev
	e.API.Postmortems = &postmortem.Builder{Ledger: o.Store, Eval: ev}
}
