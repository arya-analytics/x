package confluence

import "github.com/arya-analytics/x/signal"

// AbstractMultiSink is a basic implementation of Sink that can receive values from
// multiple Inlet(s).
type AbstractMultiSink[V Value] struct {
	// Sink is called whenever a Value is received from an Inlet.
	Sink func(ctx signal.Context, value V) error
	In   []Outlet[V]
}

// InFrom implements the Sink interface.
func (ams *AbstractMultiSink[V]) InFrom(outlet ...Outlet[V]) { ams.In = append(ams.In, outlet...) }

// Flow implements the Flow interface.
func (ams *AbstractMultiSink[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRangeEach(ctx, ams.In, func(v V) error { return ams.Sink(ctx, v) }, opts...)
}

// AbstractUnarySink is a basic implementation of Sink that can receive values from
// a single Inlet.
type AbstractUnarySink[V Value] struct {
	Sink func(ctx signal.Context, value V) error
	In   Outlet[V]
}

// InFrom implements the Sink interface.
func (aus *AbstractUnarySink[V]) InFrom(outlets ...Outlet[V]) {
	if len(outlets) != 1 {
		panic("[confluence.UnarySink] - must have exactly one outlet")
	}
	aus.In = outlets[0]
}

// Flow implements the Flow interface.
func (aus *AbstractUnarySink[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRange(ctx, aus.In, func(v V) error { return aus.Sink(ctx, v) }, opts...)
}
