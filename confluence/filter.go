package confluence

import "github.com/arya-analytics/x/signal"

// Filter is a segment that reads values from an input Stream, filters them through a
// function, and optionally discards them to an output Stream.
type Filter[V Value] struct {
	la LinearAbstract[V, V]
	// Apply is called on each value passing through the Filter. If it returns false,
	// the value is discarded or sent to the Rejects Inlet. If it returns true,
	// the value is sent through the standard Inlet. If an error is returned,
	// the Filter is closed and a fatal error is returned to the context.
	Apply func(ctx signal.Context, v V) (ok bool, err error)
	// Rejects is the Inlet that receives values that were discarded by Apply.
	Rejects Inlet[V]
}

// InFrom implements the Segment interface.
func (f *Filter[V]) InFrom(outlets ...Outlet[V]) { f.la.InFrom(outlets...) }

// OutTo implements the Segment interface. It accepts either one or two Inlet(s).
// The first Inlet is where accepted values are sent, and the second Inlet (if provided)
// is where Rejected values are sent.
func (f *Filter[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) > 2 || len(inlets) == 0 {
		panic("[confluence.Apply] - provide at most two and at least one inlet")
	}
	f.la.OutTo(inlets[0])
	if len(inlets) == 2 {
		f.Rejects = inlets[1]
	}
}

// Flow implements the Segment interface.
func (f *Filter[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	signal.GoRange(ctx, f.la.In.Outlet(), func(v V) error {
		ok, err := f.Apply(ctx, v)
		if err != nil {
			return err
		}
		if ok {
			f.la.Out.Inlet() <- v
		} else if f.Rejects != nil {
			f.Rejects.Inlet() <- v
		}
		return nil
	}, fo.Signal...)
}
