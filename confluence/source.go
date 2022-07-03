package confluence

import "github.com/arya-analytics/x/signal"

// AbstractMultiSource is a basic implementation of a Source that can send values to
// multiple Outlet(s). It implements an empty Flow method, as sources are typically
// driven by external events. The user can define a custom Flow method if they wish to
// drive the source themselves.
type AbstractMultiSource[V Value] struct {
	Out []Inlet[V]
}

// OutTo implements the Source interface.
func (ams *AbstractMultiSource[V]) OutTo(inlets ...Inlet[V]) { ams.Out = append(ams.Out, inlets...) }

// Flow implements the Flow interface.
func (ams *AbstractMultiSource[V]) Flow(ctx signal.Context, opts ...FlowOption) {}

// AbstractUnarySource is a basic implementation of a Source that sends values to a
// single Outlet. It implements an empty Flow method, as sources are typically
// driven by external events. The user can define a custom Flow method if they wish to
// drive the source themselves.
type AbstractUnarySource[V Value] struct {
	Out Inlet[V]
}

// OutTo implements the Source interface.
func (aus *AbstractUnarySource[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) != 1 {
		panic("[confluence.UnarySource] -  must have exactly one outlet")
	}
	aus.Out = inlets[0]
}

// Flow implements the Flow interface.
func (aus *AbstractUnarySource[V]) Flow(ctx signal.Context, opts ...FlowOption) {}
