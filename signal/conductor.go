package signal

import (
	"context"
	"sync"
)

// Signal is an extension of the standard context.Context that provides a way to
// signal a goroutine to stop. Go best practices recommend that contexts should
// only be passed as function arguments. The signal package is designed around the notion
// that goroutines may be forked dynamically in a manner that is not known to the
// parent. As a result, it's not possible to explicitly pass a context.Context to
// every goroutine serving a particular request. Instead,
// the signal package provides Signal, a pseudo-context that can be passed to goroutines
// through the Go interface. Wrapping a standard context.Context object is used to indicate
// to developers that this context does not behave according to typical go practices.
type Signal struct {
	context.Context
}

type Conductor interface {
	Go
	Census
	WaitGroup
}

// Go is the core interface for forking a new goroutine.
type Go interface {
	// Go starts a new goroutine controlled by the provided Signal. When the Signal.Done()
	// is closed, the goroutine should gracefully complete its remaining work and exit.
	// Additional parameters can be passed to the goroutine to modify particular
	// behavior. See option specific documentation for more.
	Go(f func(sig Signal) error, opts ...GoOption)
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
}

// Census tracks information about the goroutines forked by a Conductor.
type Census interface {
	// Count returns the number of goroutines currently running.
	Count() int
	// GoCount returns the number of calls made to Go (i.e. the number of both dead
	// and alive goroutines).
	GoCount() int
}

func New(ctx context.Context, opts ...Option) Conductor {
	c := &core{
		signal:   Signal{ctx},
		options:  newOptions(opts...),
		routines: make(map[string]Routine),
	}
	c._close = make(chan Routine, c.options.closeBufferSize)
	return c
}

type core struct {
	*options
	signal   Signal
	mu       sync.RWMutex
	_close   chan Routine
	routines map[string]Routine
}

// Count implements the Census interface.
func (c *core) Count() (count int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, r := range c.routines {
		if r.Running {
			count++
		}
	}
	return count
}

// GoCount implements the Census interface.
func (c *core) GoCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.routines)
}

// WaitOnAny implements the Shutdown interface.
func (c *core) WaitOnAny(allowNil bool) error { return c.waitForNToExit(1, allowNil) }

// WaitOnAll implements the Shutdown interface.
func (c *core) WaitOnAll() error {
	return c.waitForNToExit(c.GoCount(), true)
}

func (c *core) waitForNToExit(count int, allowNil bool) error {
	var (
		numExited int
		err       error
	)
	for r := range c._close {
		if !moreSignificant(err, r.Error) {
			err = r.Error
			numExited++
		} else if allowNil {
			numExited++
		}
		if numExited >= count {
			break
		}
	}
	return err
}

func (c *core) open(key string, options *goOptions) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.routines[key] = Routine{Key: key, Running: true, options: options}
}

func (c *core) get(key string) Routine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.routines[key]
}

func (c *core) close(key string, err error) {
	r := c.get(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	r.Running = false
	r.Error = err
	c.routines[key] = r
	c._close <- r
}

type Routine struct {
	Key     string
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
func moreSignificant(errA, errB error) bool {
	// We consider error b more significant if error a is nil and error b is not nil
	if errA == nil {
		return errB == nil
	}
	// We consider error a more significant if it is not nil and error b is nil.
	if errB == nil {
		return true
	}
	// If both errors are not nil, error b is more significant if error a is a context
	// error.
	return errA != context.Canceled && errA != context.DeadlineExceeded
}
