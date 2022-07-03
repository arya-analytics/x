package confluence

import "github.com/arya-analytics/x/signal"

// Store is a Sink that stores values by calling a function.
type Store[V Value] struct {
	// Store is called on each value received from the Outlet.
	// If it returns an error, the Store closes and returns a fatal
	// error to the context.
	Store func(ctx signal.Context, v V) error
	AbstractUnarySink[V]
}

func (s *Store[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRange(ctx, s.In, func(v V) error { return s.Store(ctx, v) }, opts...)
}
