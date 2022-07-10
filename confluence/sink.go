package confluence

import (
	"github.com/arya-analytics/x/signal"
)

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
