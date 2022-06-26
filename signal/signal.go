package signal

import (
	"context"
	atomicx "github.com/arya-analytics/x/atomic"
	"github.com/cockroachdb/errors"
	"time"
)

// Context is an extension of the standard context.Context that provides a way to
// signal a goroutine to maybeStop.
type Context interface {
	context.Context
	Go
	WaitGroup
	Errors
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
	return &core{
		Context:   ctx,
		options:   newOptions(opts...),
		transient: make(chan error),
		fatal:     make(chan error),
		stopped:   make(chan struct{}),
		numForked: &atomicx.Int32Counter{},
		numExited: &atomicx.Int32Counter{},
	}
}

type core struct {
	*options
	context.Context
	// transient receives errors from goroutines that should not result in the
	// cancellation of the routine, but do need to be reported to the caller.
	transient chan error
	// fatal receives errors from goroutines that indicate a fatal error (i.e. the
	// routine crashed).
	fatal chan error
	// stopped is closed when all goroutines have exited.
	stopped chan struct{}
	// _numForked is the number of goroutines that have been started.
	// This is mutex protected by atomic.AddInt32 within runPrelude.
	numForked *atomicx.Int32Counter
	// numExited is the number of goroutines that have exited.
	// This is mutex protected by atomic.AddInt32.
	numExited *atomicx.Int32Counter
}

// Transient implements the Errors interface.
func (c *core) Transient() chan error { return c.transient }

func (c *core) numRunning() int32 { return c.numForked.Value() - c.numExited.Value() }

func runDeferals(deferals []func()) {
	for _, f := range deferals {
		f()
	}
}

// moreSignificant returns true if the first error is more relevant to the caller
// than the second error.
func moreSignificant(err, reference error) bool {
	// In the case where both errors are nil, we return false. In the case that both
	// errors are non nil, err is more significant is reference is a context error.
	return err != nil &&
		(reference == nil || errors.Is(reference, context.Canceled) || errors.Is(reference, context.DeadlineExceeded))
}
