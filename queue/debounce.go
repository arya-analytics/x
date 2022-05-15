package queue

import (
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"time"
)

// Debounce is a simple, goroutine safe queue that flushes data to a channel on a timer or queue size threshold.
type Debounce[T any] struct {
	// In is the channel to send values to add to the queue.
	In chan []T
	// Out is the channel to receive values from the queue.
	// Out will be closed when the queue is closed.
	Out    chan []T
	Config DebounceConfig
}

type DebounceConfig struct {
	// Shutdown is used to gracefully shutdown down the queue.
	Shutdown shutdown.Shutdown
	// Interval is the time between flushes.
	Interval time.Duration
	// Threshold is the maximum number of values to store in Debounce.
	// Debounce will flush when this threshold is reached, regardless of the Interval.
	Threshold int
	// Logger is the logger to use for logging.
	Logger *zap.Logger
}

const (
	emptyCycleShutdownCount = 5
)

// Start starts the queue.
func (d *Debounce[T]) Start() {
	d.Config.Shutdown.Go(func(sig chan shutdown.Signal) error {
		var (
			t        = time.NewTicker(d.Config.Interval)
			sd       = false
			numEmpty = 0
		)
		defer t.Stop()
		for {
			select {
			case <-sig:
				d.Config.Logger.Info("shutting down debounce queue")
				sd = true
			default:
			}
			values := d.fill(t)
			//d.Logger.Debug("flushing debounce queue", zap.Int("count", len(values)))
			if len(values) == 0 {
				if sd {
					numEmpty++
					if numEmpty > emptyCycleShutdownCount {
						close(d.Out)
						d.Config.Logger.Info("debounce queue shutdown down")
						return nil
					}
				}
				continue
			}
			d.Out <- values
			d.Config.Logger.Debug("flushed debounce queue")
		}
	})
}

func (d *Debounce[T]) fill(t *time.Ticker) []T {
	ops := make([]T, 0, d.Config.Threshold)
	for {
		select {
		case requests := <-d.In:
			ops = append(ops, requests...)
			if len(ops) >= d.Config.Threshold {
				return ops
			}
		case <-t.C:
			return ops
		}
	}
}
