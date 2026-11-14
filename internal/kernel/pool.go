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
	w        *Worker
	sem      chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex // serializes admission with the start of drain
	draining bool
	drain    chan struct{}
}

func NewPool(w *Worker, maxInflight int) *Pool {
	return &Pool{w: w, sem: make(chan struct{}, maxInflight), drain: make(chan struct{})}
}

// Handle matches notify.Handler: it blocks until a slot is free (backpressure
// to the broker), then executes asynchronously and reports the disposition.
func (p *Pool) Handle(ctx context.Context, runID string, done func(ack bool, redeliverAfter time.Duration)) {
	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		done(false, 0)
		return
	case <-p.drain:
		done(false, 0)
		return
	}
	p.mu.Lock()
	if p.draining {
		p.mu.Unlock()
		<-p.sem
		done(false, 0)
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()
	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()
		d := p.w.HandleNotification(ctx, runID)
		done(d.Ack, d.Delay)
	}()
}

// Wait permanently closes admission, then waits for accepted executions to
// finish. The shared lock ensures no Add can race with a zero-count Wait.
func (p *Pool) Wait() {
	p.mu.Lock()
	if !p.draining {
		p.draining = true
		close(p.drain)
	}
	p.mu.Unlock()
	p.wg.Wait()
}
