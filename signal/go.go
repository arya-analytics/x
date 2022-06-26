package signal

import (
	"time"
)

// Go implements the Go interface.
func (c *core) Go(f func() error, opts ...GoOption) {
	o := newGoOptions(opts)
	c.markOpen()
	go func() {
		defer runDeferals(o.deferals)
		c.fatal <- f()
	}()
}

func GoRange[V any](ctx Context, ch <-chan V, f func(V) error, opts ...GoOption) {
	if ctx.Err() != nil {
		return
	}
	ctx.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-ch:
				if !ok {
					return nil
				}
				if err := f(v); err != nil {
					return err
				}
			}
		}
	}, opts...)
}

func GoTick(ctx Context, interval time.Duration, f func(t time.Time) error, opts ...GoOption) {
	t := time.NewTicker(interval)
	GoRange(ctx, t.C, f, append(opts, WithDefer(func() { t.Stop() }))...)
}
