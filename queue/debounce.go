package queue

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	"time"
)

// Debounce is a simple, goroutine safe queue that flushes data to a channel on a timer or queue size threshold.
type Debounce[V confluence.Value] struct {
	// Interval is the time between flushes.
	Interval time.Duration
	// Threshold is the maximum number of values to store in Debounce.
	// Debounce will flush when this threshold is reached, regardless of the Interval.
	Threshold int
	confluence.Linear[[]V]
}

const emptyCycleShutdownCount = 5

// Flow starts the queue.
func (d *Debounce[V]) Flow(ctx confluence.Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		var (
			t        = time.NewTicker(d.Interval)
			sd       = false
			numEmpty = 0
		)
		defer t.Stop()
		for {
			select {
			case <-sig:
				sd = true
			default:
			}
			values := d.fill(t)
			if len(values) == 0 {
				if sd {
					numEmpty++
					if numEmpty > emptyCycleShutdownCount {
						return nil
					}
				}
				continue
			}
			d.Out.Inlet() <- values
		}
	})
}

func (d *Debounce[V]) fill(t *time.Ticker) []V {
	ops := make([]V, 0, d.Threshold)
	for {
		select {
		case values := <-d.In.Outlet():
			ops = append(ops, values...)
			if len(ops) >= d.Threshold {
				return ops
			}
		case <-t.C:
			return ops
		}
	}
}
