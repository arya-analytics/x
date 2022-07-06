package confluence

import (
	"github.com/arya-analytics/x/signal"
)

// MultiSink is a basic implementation of Sink that can receive values from
// multiple Inlet(s).
type MultiSink[V Value] struct {
	// ApplySink is called whenever a Value is received from an Outlet.
	ApplySink func(ctx signal.Context, value V) error
	In        []Outlet[V]
}

// InFrom implements the Sink interface.
func (ams *MultiSink[V]) InFrom(outlet ...Outlet[V]) { ams.In = append(ams.In, outlet...) }

// Flow implements the Flow interface.
func (ams *MultiSink[V]) Flow(ctx signal.Context, opts ...Option) {
	ams.GoRangeEach(ctx, ams.ApplySink, NewOptions(opts).Signal...)
}

func (ams *MultiSink[V]) GoRangeEach(
	ctx signal.Context,
	f func(signal.Context, V) error,
	opts ...signal.GoOption,
) {
	var channels []<-chan V
	for _, outlet := range ams.In {
		channels = append(channels, outlet.Outlet())
	}
	signal.GoRangeEach(ctx, channels, f, opts...)
}

// UnarySink is a basic implementation of Sink that can receive values from a single Inlet.
type UnarySink[V Value] struct {
	// Sink is called whenever a value is received from the Outlet.
	Sink func(ctx signal.Context, value V) error
	In   Outlet[V]
}

// InFrom implements the Sink interface.
func (us *UnarySink[V]) InFrom(outlets ...Outlet[V]) {
	if len(outlets) != 1 {
		panic("[confluence.UnarySink] - must have exactly one outlet")
	}
	us.In = outlets[0]
}

// Flow implements the Flow interface.
func (us *UnarySink[V]) Flow(ctx signal.Context, opts ...Option) {
	us.GoRange(ctx, us.Sink, NewOptions(opts).Signal...)
}

func (us *UnarySink[V]) GoRange(ctx signal.Context, f func(signal.Context, V) error, opts ...signal.GoOption) {
	signal.GoRange(ctx, us.In.Outlet(), f, opts...)
}
