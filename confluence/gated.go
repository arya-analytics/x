package confluence

import (
	"github.com/arya-analytics/x/lock"
	"github.com/arya-analytics/x/signal"
)

type Gated[I, O Value] struct {
	Segment[I, O]
	Gate *lock.Gate
}

func (g Gated[I, O]) Flow(ctx signal.Context, opts ...FlowOption) {
	if g.Gate.Open() {
		g.Segment.Flow(ctx, append(opts, Defer(g.Gate.Close))...)
	}
}

func Gate[I, O Value](segment Segment[I, O]) Gated[I, O] {
	return Gated[I, O]{Segment: segment, Gate: &lock.Gate{}}
}

type GatedSource[V Value] struct {
	Source[V]
	Gate *lock.Gate
}

func GateSource[V Value](source Source[V]) GatedSource[V] {
	return GatedSource[V]{Source: source, Gate: &lock.Gate{}}
}

func (g GatedSource[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	if g.Gate.Open() {
		g.Source.Flow(ctx, append(opts, Defer(g.Gate.Close))...)
	}
}

type GatedSink[V Value] struct {
	Sink[V]
	Gate *lock.Gate
}

func GateSink[V Value](sink Sink[V]) GatedSink[V] {
	return GatedSink[V]{Sink: sink, Gate: &lock.Gate{}}
}
func (g GatedSink[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	if g.Gate.Open() {
		g.Sink.Flow(ctx, append(opts, Defer(g.Gate.Close))...)
	}
}
