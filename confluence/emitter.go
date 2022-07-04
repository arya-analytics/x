package confluence

import (
	"github.com/arya-analytics/x/signal"
	"time"
)

// Emitter is a Source that emits values to an Inlet at a regular interval.
type Emitter[V Value] struct {
	// Emitter is called on each tick. If it returns an error, the Emitter closes
	// and returns a fatal error to the context.
	Emit func(ctx signal.Context) (V, error)
	// Interval is the duration between ticks.
	Interval time.Duration
	AbstractUnarySource[V]
}

// Flow implements the Flow interface.
func (e *Emitter[V]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(e)
	signal.GoTick(ctx, e.Interval, e.emit, fo.Signal...)
}

func (e *Emitter[V]) emit(ctx signal.Context, t time.Time) error {
	v, err := e.Emit(ctx)
	if err != nil {
		return err
	}
	e.Out.Inlet() <- v
	return nil
}
