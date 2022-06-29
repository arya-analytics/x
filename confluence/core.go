package confluence

import (
	"github.com/arya-analytics/x/signal"
)

// |||||| LINEAR ||||||

// Linear is a segment that reads values from a single input stream and pipes them
// to a single output stream. Linear is an abstract segment, meaning it should not be
// instantiated directly, and should be embedded in other segments. Linear will
// panic if Flow is called.
type Linear[V Value] struct {
	In  Outlet[V]
	Out Inlet[V]
}

// InFrom implements the Segment interface.
func (l *Linear[V]) InFrom(outlets ...Outlet[V]) {
	if len(outlets) == 0 {
		panic("[confluence.Linear] must have exactly one inlet")
	}
	l.In = outlets[0]
}

// OutTo implements the Segment interface.
func (l *Linear[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) == 0 {
		panic("[confluence.Linear] - must have exactly one outlet")
	}
	l.Out = inlets[0]
}

// Flow implements the Segment interface.
func (l *Linear[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	panic("[confluence.Linear] - abstract segment")
}
