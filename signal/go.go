package signal

import (
	"time"
)

// Go implements the Go interface.
func (c *core) Go(f func() error, opts ...GoOption) {
	o := newGoOptions(c, opts)
	key := c.markOpen(o)
	go func() {
		var err error
		defer runDeferals(o.deferals)
		defer func() {
			c.markClosed(key, err)
		}()
		err = f()
	}()
}

func GoRange[V any](ctx Context, ch <-chan V, f func(V) error, opts ...GoOption) {
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
