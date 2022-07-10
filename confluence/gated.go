package confluence

import (
	"github.com/arya-analytics/x/lock"
	"github.com/arya-analytics/x/signal"
)

// Gated is wraps a Segment so that can not be started while already running.
// To wrap a Segment, call the Gate function.
type Gated[I, O Value] struct {
	Segment[I, O]
	Gate *lock.Gate
}

// Flow implements the Flow interface. If Flow was already called and the Segment
// goroutines are still running, the call is ignored.
func (g Gated[I, O]) Flow(ctx signal.Context, opts ...Option) {
	if g.Gate.Open() {
		g.Segment.Flow(ctx, append(opts, Defer(g.Gate.Close))...)
	}
}

// Gate gates a Segment with a Gated wrapper. See the Gated type for more details.
func Gate[I, O Value](segment Segment[I, O]) Gated[I, O] {
	return Gated[I, O]{Segment: segment, Gate: &lock.Gate{}}
}

// GatedSource is a Source that can not be started while already running.
type GatedSource[V Value] struct {
	Source[V]
	Gate *lock.Gate
}

// GateSource gates a Source with a GatedSource wrapper. See the GatedSource type for
// more details.
func GateSource[V Value](source Source[V]) GatedSource[V] {
	return GatedSource[V]{Source: source, Gate: &lock.Gate{}}
}

// Flow implements the Flow interface. If Flow was already called and the Source
// goroutines are still running, the call is ignored.
func (g GatedSource[V]) Flow(ctx signal.Context, opts ...Option) {
	if g.Gate.Open() {
		g.Source.Flow(ctx, append(opts, Defer(g.Gate.Close))...)
	}
}

// GatedSink is a Sink that can not be started while already running.
type GatedSink[V Value] struct {
	Sink[V]
	Gate *lock.Gate
}

// GateSink gates a Sink with a GatedSink wrapper. See the GatedSink type for more
// details.
func GateSink[V Value](sink Sink[V]) GatedSink[V] {
	return GatedSink[V]{Sink: sink, Gate: &lock.Gate{}}
}

// Flow implements the Flow interface. If Flow was already called and the Sink
// goroutines are still running, the call is ignored.
func (g GatedSink[V]) Flow(ctx signal.Context, opts ...Option) {
	if g.Gate.Open() {
		g.Sink.Flow(ctx, append(opts, Defer(g.Gate.Close))...)
	}
}
