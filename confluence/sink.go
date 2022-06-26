package confluence

import "github.com/arya-analytics/x/signal"

// Sink is a segment that can accept values from outlets, but cannot
// send values to inlets. Sinks are typically used to write values
// to network pipes or persistent storage.
type Sink[V Value] interface {
	InFrom(outlets ...Outlet[V])
	Flow[V]
}

// CoreSink is a basic implementation of Sink. It implements the Segment
// interface, but will panic if any inlets are added.
type CoreSink[V Value] struct {
	Sink   func(ctx signal.Context, value V) error
	inFrom []Outlet[V]
}

// InFrom implements the Segment interface.
func (s *CoreSink[V]) InFrom(outlet ...Outlet[V]) { s.inFrom = append(s.inFrom, outlet...) }

// OutTo implements the Segment interface. This method will panic if called.
func (s *CoreSink[V]) OutTo(_ ...Inlet[V]) {
	panic("[confluence.Sink] - cannot pipe values out")
}

// Flow implements the Segment interface.
func (s *CoreSink[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, s.inFrom, func(v V) error { return s.Sink(ctx, v) })
}

type UnarySink[V Value] struct {
	In Outlet[V]
}

func (u *UnarySink[V]) InFrom(outlets ...Outlet[V]) {
	if len(outlets) != 1 {
		panic("[confluence.UnarySink] - must have exactly one outlet")
	}
	u.In = outlets[0]
}

func (u *UnarySink[V]) OutTo(_ ...Inlet[V]) {
	panic("[confluence.Sink] - cannot pipe values out")
}

func (u *UnarySink[V]) Flow(ctx signal.Context) {}
