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

// WithCancel returns a Context derived from ctx that is canceled by the given cancel
// function. If any goroutine returns a non-nil error, the Context will be canceled.
func WithCancel(ctx context.Context, opts ...Option) (Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	c := newCore(ctx, cancel, opts...)
	return c, func() { cancel(); c.maybeStop() }
}

// WithTimeout returns a Context derived from ctx that is canceled by the given
//timeout. If any goroutine returns a non-nil error, the Context will be canceled.
func WithTimeout(ctx context.Context, d time.Duration, opts ...Option) (Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, d)
	c := newCore(ctx, cancel, opts...)
	return c, func() { cancel(); c.maybeStop() }
}

// TODO wraps context.TODO in a Context that can be cancelled. If any goroutine
// returns a non-nil error, the Context will be cancelled.
func TODO(opts ...Option) (Context, context.CancelFunc) {
	return WithCancel(context.TODO(), opts...)
}

func newCore(
	ctx context.Context,
	cancel context.CancelFunc,
	opts ...Option,
) *core {
	o := newOptions(opts...)
	c := &core{
		Context:   ctx,
		cancel:    cancel,
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
	cancel context.CancelFunc
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
	// wrapped is used to start and signal errors to goroutines.
	wrapped errgroup.Group
}

// Transient implements the Errors interface.
func (c *core) Transient() chan error { return c.transient }

func runDeferals(deferals []func()) {
	for _, f := range deferals {
		f()
	}
}
