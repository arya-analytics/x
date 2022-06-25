package queue

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"time"
)

type DebounceConfig struct {
	// Interval is the time between flushes.
	Interval time.Duration
	// Threshold is the maximum number of values to store in Debounce.
	// Debounce will flush when this threshold is reached, regardless of the Interval.
	Threshold int
}

// Debounce is a simple, goroutine safe queue that flushes data to a channel on a timer or queue size threshold.
type Debounce[V confluence.Value] struct {
	Config DebounceConfig
	confluence.Linear[[]V]
}

const emptyCycleShutdownCount = 5

// Flow starts the queue.
func (d *Debounce[V]) Flow(ctx confluence.Context) {
	ctx.Go(func(sig signal.Signal) error {
		var (
			t        = time.NewTicker(d.Config.Interval)
			sd       = false
			numEmpty = 0
		)
		defer t.Stop()
		for {
			select {
			case <-sig.Done():
				sd = true
			default:
			}
			values := d.fill(t.C)
			if len(values) == 0 {
				if sd {
					numEmpty++
					if numEmpty > emptyCycleShutdownCount {
						return sig.Err()
					}
				}
				continue
			}
			d.Out.Inlet() <- values
		}
	})
}

func (d *Debounce[V]) fill(C <-chan time.Time) []V {
	ops := make([]V, 0, d.Config.Threshold)
	for {
		select {
		case values := <-d.In.Outlet():
			ops = append(ops, values...)
			if len(ops) >= d.Config.Threshold {
				return ops
			}
		case <-C:
			return ops
		}
	}
}
