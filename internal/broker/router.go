package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/hamanakam1/vortexmesh/internal/envelope"
)

// ErrRouterClosed is returned when Publish is called after Close.
var ErrRouterClosed = errors.New("router: closed")

// Metrics is a point-in-time snapshot of router counters.
type Metrics struct {
	// Accepted counts envelopes that entered the bounded queue.
	Accepted uint64
	// Delivered counts envelopes the broker fanned out successfully.
	Delivered uint64
	// Dropped counts envelopes rejected by validation, cancelled while
	// waiting for queue space, or failed by the broker.
	Dropped uint64
}

// Router decouples producers from fan-out with a bounded queue and a fixed
// worker pool. Producers never block on a slow subscriber for longer than
// their own context allows.
type Router struct {
	broker *Broker
	queue  chan envelope.Envelope

	ctx    context.Context
	cancel context.CancelFunc

	workers   sync.WaitGroup
	closeOnce sync.Once

	mu     sync.RWMutex
	closed bool

	accepted  atomic.Uint64
	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// NewRouter starts workers goroutines consuming from a queue of queueSize
// envelopes. b must not be nil.
func NewRouter(b *Broker, workers, queueSize int) *Router {
	if b == nil {
		panic("broker.NewRouter: nil broker")
	}
	if workers < 1 {
		workers = 1
	}
	if queueSize < 1 {
		queueSize = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{
		broker: b,
		queue:  make(chan envelope.Envelope, queueSize),
		ctx:    ctx,
		cancel: cancel,
	}
	r.workers.Add(workers)
	for i := 0; i < workers; i++ {
		go r.worker()
	}
	return r
}

func (r *Router) worker() {
	defer r.workers.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-r.queue:
			if err := r.broker.Publish(r.ctx, msg); err != nil {
				r.dropped.Add(1)
				continue
			}
			r.delivered.Add(1)
		}
	}
}

// Publish validates msg and enqueues it without blocking past ctx. A full
// queue surfaces as ctx.Err() so the caller decides whether to retry or shed.
func (r *Router) Publish(ctx context.Context, msg envelope.Envelope) error {
	if err := msg.Validate(); err != nil {
		r.dropped.Add(1)
		return err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		r.dropped.Add(1)
		return ErrRouterClosed
	}

	select {
	case r.queue <- msg:
		r.accepted.Add(1)
		return nil
	case <-ctx.Done():
		r.dropped.Add(1)
		return ctx.Err()
	case <-r.ctx.Done():
		r.dropped.Add(1)
		return ErrRouterClosed
	}
}

// Metrics returns a snapshot of the router counters.
func (r *Router) Metrics() Metrics {
	return Metrics{
		Accepted:  r.accepted.Load(),
		Delivered: r.delivered.Load(),
		Dropped:   r.dropped.Load(),
	}
}

// Close stops accepting work, cancels in-flight fan-out, and waits for the
// workers to exit. Envelopes still queued at shutdown are abandoned; they are
// not counted as dropped.
func (r *Router) Close() {
	r.closeOnce.Do(func() {
		// Cancel first: in-flight Publish calls observe r.ctx.Done() and
		// release their read lock, so the write lock below cannot deadlock
		// against a full queue.
		r.cancel()
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		r.workers.Wait()
	})
}
