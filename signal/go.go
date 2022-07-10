package signal

import (
	"time"
)

// Go is the core interface for forking a new goroutine.
type Go interface {
	// Go starts a new goroutine with the provided Context. When the Context is canceled,
	// the goroutine should abort its work and exit. The provided Context is canceled
	// the first time a function passed to go returns a non-nil error.
	// Additional parameters can be passed to the goroutine to modify its behavior.
	// See the GoOption documentation for more.
	Go(f func(ctx Context) error, opts ...GoOption)
}

// Go implements the Go interface.
func (c *core) Go(f func(ctx Context) error, opts ...GoOption) {
	// Check if the context has been canceled and increment the number of
	// running routines by one.
	if c.runPrelude() {
		return
	}
	o := newGoOptions(opts)
	c.wrapped.Go(func() (err error) {
		// Run deferals before the routine exists, decrement the number of
		// running routines by one, and cancel the context is the error is non-nil.
		// This deferral must be wrapped in a closure for err to be captured.
		defer func() {
			c.runPostlude(o, err)
		}()
		err = f(c)
		return err
	})
}

// GoRange starts a new goroutine controlled by the provided Go that ranges
// over the values in ch. The goroutine will exit when the context is canceled
// or the channel is closed. Additional parameters can be passed to the goroutine
// to modify its behavior. See the GoOption documentation for more.
func GoRange[V any](g Go, ch <-chan V, f func(Context, V) error, opts ...GoOption) {
	g.Go(func(ctx Context) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-ch:
				if !ok {
					return nil
				}
				if err := f(ctx, v); err != nil {
					return err
				}
			}
		}
	}, opts...)
}

// GoTick starts a new goroutine controlled by the provided Go that
// ticks at the provided interval. The goroutine will exit when the context
// is cancelled. Additional parameters can be passed to the goroutine to
// modify its behavior. See the GoOption documentation for more.
func GoTick(
	g Go,
	interval time.Duration,
	f func(Context, time.Time) error,
	opts ...GoOption,
) {
	t := time.NewTicker(interval)
	GoRange(g, t.C, f, append(opts, Defer(func() { t.Stop() }))...)
}

func (c *core) maybeStop() {
	// If we have any running goroutines or the context hasn't been canceled,
	// we don't do anything.
	if c.NumRunning() != 0 || c.Err() == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// If we have already closed the maybeStop channel, we don't do anything.
	select {
	case <-c.mu.stopped:
		return
	default:
		close(c.mu.stopped)
	}
}

func (c *core) runPrelude() (prevent bool) {
	if c.Err() != nil {
		return true
	}
	c.numForked.Add(1)
	return false
}

func (c *core) runPostlude(o *goOptions, err error) {
	runDeferals(o.deferals)
	c.numExited.Add(1)
	if err != nil {
		c.cancel()
	}
	c.maybeStop()
}
