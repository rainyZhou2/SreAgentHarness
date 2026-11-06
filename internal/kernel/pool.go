package kernel

import (
	"context"
	"sync"
	"time"
)

// Pool bounds how many runs a process executes concurrently (§14.4). A slot
// is held only while a step executes; runs waiting for approval or retry hold
// none, because they are not executing at all.
type Pool struct {
	w   *Worker
	sem chan struct{}
	wg  sync.WaitGroup
}

func NewPool(w *Worker, maxInflight int) *Pool {
	return &Pool{w: w, sem: make(chan struct{}, maxInflight)}
}

// Handle matches notify.Handler: it blocks until a slot is free (backpressure
// to the broker), then executes asynchronously and reports the disposition.
func (p *Pool) Handle(ctx context.Context, runID string, done func(ack bool, redeliverAfter time.Duration)) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		done(false, 0)
		return
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()
		d := p.w.HandleNotification(ctx, runID)
		done(d.Ack, d.Delay)
	}()
}

// Wait blocks until in-flight executions finish (graceful shutdown).
func (p *Pool) Wait() { p.wg.Wait() }
