package signal

import (
	"time"
)

// Go implements the Go interface.
func (c *core) Go(f func(sig Signal) error, opts ...GoOption) {
	o := newGoOptions(c, opts)
	c.open(o.key, o)
	go func() {
		var err error
		defer runDeferals(o.deferals)
		defer func() {
			c.close(o.key, err)
		}()
		err = f(c.signal)
	}()
}

func GoRange[V any](c Conductor, ch <-chan V, f func(V) error, opts ...GoOption) {
	c.Go(func(sig Signal) error {
		for {
			select {
			case <-sig.Done():
				return sig.Err()
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

func GoTick(c Conductor, interval time.Duration, f func(t time.Time) error, opts ...GoOption) {
	t := time.NewTicker(interval)
	GoRange(c, t.C, f, append(opts, WithDefer(func() { t.Stop() }))...)
}
