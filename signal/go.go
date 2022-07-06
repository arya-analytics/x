package signal

import (
	"golang.org/x/sync/errgroup"
	"time"
)

// Go is the core interface for forking a new goroutine.
type Go interface {
	// Go starts a new goroutine controlled by the provided Context. When Context.Done()
	// is closed, the goroutine should gracefully complete its remaining work and exit.
	// Additional parameters can be passed to the goroutine to modify particular
	// behavior. See option specific documentation for more.
	Go(f func() error, opts ...GoOption)
}

// Go implements the Go interface.
func (c *core) Go(f func() error, opts ...GoOption) {
	// If the context has already been cancelled, don't even both forking
	// the new routine.
	if c.runPrelude() {
		return
	}
	o := newGoOptions(opts)
	go func() {
		defer c.runPostlude(o)
		err := f()
		c.fatal.Inlet() <- err
	}()
}

func GoRange[V any](ctx Context, ch <-chan V, f func(Context, V) error, opts ...GoOption) {
	ctx.Go(func() error {
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

func GoRangeEach[V any](
	ctx Context,
	channels []<-chan V,
	f func(Context, V) error,
	opts ...GoOption,
) {
	ctx.Go(func() error {
		wg := errgroup.Group{}
		for _, ch := range channels {
			_ch := ch
			wg.Go(func() error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case v, ok := <-_ch:
						if !ok {
							return nil
						}
						if err := f(ctx, v); err != nil {
							return err
						}
					}
				}
			})
		}
		return wg.Wait()
	}, opts...)
}

func GoTick(ctx Context, interval time.Duration, f func(Context, time.Time) error, opts ...GoOption) {
	t := time.NewTicker(interval)
	GoRange(ctx, t.C, f, append(opts, Defer(func() { t.Stop() }))...)
}

func (c *core) maybeStop() {
	// If we have any running goroutines or the context hasn't been canceled,
	// we don't do anything.
	if c.numRunning() != 0 || c.Err() == nil {
		return
	}

	// If we have already closed the maybeStop channel, we don't do anything.
	select {
	case <-c.stopped:
		return
	default:
		close(c.stopped)
	}
}

func (c *core) runPrelude() (prevent bool) {
	if c.Err() != nil {
		return true
	}
	c.numForked.Add(1)
	c.fatal.Resize(int(c.numForked.Value()))
	return false
}

func (c *core) runPostlude(o *goOptions) {
	runDeferals(o.deferals)
	c.numExited.Add(1)
	c.maybeStop()
}
