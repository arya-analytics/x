package confluence

import (
	"github.com/arya-analytics/x/lock"
	"github.com/arya-analytics/x/signal"
)

type Gated[V Value] struct {
	Segment[V]
	*lock.Gate
}

func Gate[V Value](segment Segment[V]) Segment[V] {
	return &Gated[V]{Segment: segment, Gate: &lock.Gate{}}
}

func (g *Gated[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	if g.Open() {
		g.Segment.Flow(ctx, append(opts, Defer(g.Close))...)
	}
}
