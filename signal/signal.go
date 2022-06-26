package signal

import (
	"context"
	"github.com/cockroachdb/errors"
	"sync/atomic"
)

// Context is an extension of the standard context.Context that provides a way to
// signal a goroutine to stop.
type Context interface {
	context.Context
	Go
	WaitGroup
	Errors
}

// Go is the core interface for forking a new goroutine.
type Go interface {
	// Go starts a new goroutine controlled by the provided Context. When Context.Done()
	// is closed, the goroutine should gracefully complete its remaining work and exit.
	// Additional parameters can be passed to the goroutine to modify particular
	// behavior. See option specific documentation for more.
	Go(f func() error, opts ...GoOption)
}

// WaitGroup provides methods for detecting and waiting for the exit of goroutines
// managed by a signal.Conductor.
type WaitGroup interface {
	// WaitOnAny waits for any of the running goroutines to exit with an error.
	// If allowNil is set to false, waits for the first goroutine to exit with a non-nil
	// error. Returns the error encountered by the first goroutine to exit. Returns nil
	// if no goroutines are running.
	WaitOnAny(allowNil bool) error
	// WaitOnAll waits for all running goroutines to exit, then proceeds to return
	// the first non-nil error (returns nil if all errors are nil). Returns nil
	// if no goroutines are running. This is an equivalent call to errgroup.Group.Wait().
	WaitOnAll() error
	Stopped() <-chan struct{}
}

type Errors interface {
	Transient() chan error
}

type core struct {
	*options
	context.Context
	// transient receives errors from goroutines that should not result in the
	// cancellation of the routine, but do need to be reported to the caller.
	transient chan error
	// fatal receives errors from goroutines that indicate a fatal error (i.e. the
	// routine crashed).
	fatal   chan error
	stopped chan struct{}
	// _numForked is the number of goroutines that have been started.
	// This is mutex protected by atomic.AddInt32 within markOpen.
	_numForked int32
	// _numExited is the number of goroutines that have exited.
	// This is mutex protected by atomic.AddInt32.
	_numExited int32
}

func WithCancel(ctx context.Context, opts ...Option) (Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	return Wrap(ctx, opts...), cancel
}

func Wrap(ctx context.Context, opts ...Option) Context {
	return &core{
		Context:   ctx,
		options:   newOptions(opts...),
		transient: make(chan error),
		fatal:     make(chan error),
		stopped:   make(chan struct{}),
	}
}

func Background(opts ...Option) (Context, context.CancelFunc) {
	return WithCancel(context.Background(), opts...)
}

// WaitOnAny implements the Shutdown interface.
func (c *core) WaitOnAny(allowNil bool) error { return c.waitForNToExit(1, allowNil) }

// WaitOnAll implements the Shutdown interface.
func (c *core) WaitOnAll() error {
	return c.waitForNToExit(c.numRunning(), true)
}

// Transient implements the Errors interface.
func (c *core) Transient() chan error { return c.transient }

func (c *core) Stopped() <-chan struct{} { return c.stopped }

func (c *core) maybeCloseStopped() {
	if c.numRunning() == 0 && c.Err() != nil {
		select {
		case <-c.stopped:
		default:
			close(c.stopped)
		}
	}
}

func (c *core) waitForNToExit(count int32, allowNil bool) error {
	var (
		numExited int32
		err       error
	)
	for _err := range c.fatal {
		if _err != nil {
			numExited++
			if moreSignificant(_err, err) {
				err = _err
			}
		} else if allowNil {
			numExited++
		}
		if numExited >= count {
			break
		}
	}
	c.markClosed(numExited)
	c.maybeCloseStopped()
	return err
}

func (c *core) markOpen() int32 { return atomic.AddInt32(&c._numForked, 1) }

func (c *core) markClosed(delta int32) int32 { return atomic.AddInt32(&c._numExited, -delta) }

func (c *core) numRunning() int32 { return c._numForked - c._numExited }

type Routine struct {
	Error   error
	Running bool
	options *goOptions
}

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
