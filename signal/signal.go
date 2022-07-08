package signal

import (
	"context"
	atomicx "github.com/arya-analytics/x/atomic"
	"golang.org/x/sync/errgroup"
	"sync"
	"time"
)

// Context is an extension of the standard context.Context that provides a way to
// signal a goroutine to maybeStop.
type Context interface {
	context.Context
	Go
	WaitGroup
	Errors
	Census
}

type Errors interface {
	Transient() chan error
}

func WithCancel(ctx context.Context, opts ...Option) (Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	c := newCore(ctx, opts...)
	return c, func() { cancel(); c.maybeStop() }
}

func WithTimeout(ctx context.Context, d time.Duration, opts ...Option) (Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, d)
	c := newCore(ctx, opts...)
	return c, func() { cancel(); c.maybeStop() }
}

func Background(opts ...Option) (Context, context.CancelFunc) {
	return WithCancel(context.Background(), opts...)
}

func newCore(ctx context.Context, opts ...Option) *core {
	o := newOptions(opts...)
	c := &core{
		Context:   ctx,
		options:   o,
		transient: make(chan error, *o.transientCap),
		numForked: &atomicx.Int32Counter{},
		numExited: &atomicx.Int32Counter{},
	}
	c.mu.stopped = make(chan struct{})
	return c
}

type core struct {
	*options
	context.Context
	// transient receives errors from goroutines that should not result in the
	// cancellation of the routine, but do need to be reported to the caller.
	transient chan error
	mu        struct {
		sync.RWMutex
		// stopped is closed when all goroutines have exited.
		stopped chan struct{}
	}
	// _numForked is the number of goroutines that have been started.
	// This is mutex protected by atomic.AddInt32 within runPrelude.
	numForked *atomicx.Int32Counter
	// numExited is the number of goroutines that have exited.
	// This is mutex protected by atomic.AddInt32.
	numExited *atomicx.Int32Counter
	eg        errgroup.Group
}

// Transient implements the Errors interface.
func (c *core) Transient() chan error { return c.transient }

func (c *core) numRunning() int32 { return c.numForked.Value() - c.numExited.Value() }

func runDeferals(deferals []func()) {
	for _, f := range deferals {
		f()
	}
}
