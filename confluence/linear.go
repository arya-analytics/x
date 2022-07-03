package confluence

import "github.com/arya-analytics/x/signal"

// LinearAbstract is an abstract Segment that reads values from a single Inlet and
// pipes them to a single Outlet. LinearAbstract does not implement the Flow method,
// and is therefore not usable directly. It should be embedded in a concrete segment.
type LinearAbstract[I, O Value] struct {
	In  Outlet[I]
	Out Inlet[O]
}

func (la *LinearAbstract[I, O]) InFrom(outlets ...Outlet[I]) {
	if len(outlets) == 0 {
		panic("[confluence.Linear] - must have exactly one inlet")
	}
	la.In = outlets[0]
}

// OutTo implements the Segment interface.
func (la *LinearAbstract[I, O]) OutTo(inlets ...Inlet[O]) {
	if len(inlets) == 0 {
		panic("[confluence.Linear] - must have exactly one outlet")
	}
	la.Out = inlets[0]
}

// LinearTransform is a Segment that reads values from a single Inlet, performs a
// transformation, and writes the result to a single Outlet.
type LinearTransform[I, O Value] struct {
	LinearAbstract[I, O]
	Transform[I, O]
}

// Flow implements the Segment interface.
func (l *LinearTransform[I, O]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRange(ctx, l.In, func(i I) error {
		v, ok, err := l.Apply(ctx, i)
		if err != nil || !ok {
			return err
		}
		l.Out.Inlet() <- v
		return nil
	}, opts...)
}
