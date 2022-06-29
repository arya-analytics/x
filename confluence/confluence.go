package confluence

import (
	"github.com/arya-analytics/x/signal"
	"time"
)

// |||||| VALUE ||||||

// Value represents an item that can be sent through a Stream.
type Value any

// |||||| STREAMS ||||||

// |||||| SEGMENT ||||||

type Flow[V Value] interface {
	Flow(ctx signal.Context, opts ...FlowOption)
}

// Segment is an interface that accepts values from an Inlet Stream, does some operation on them, and returns the
// values through an Outlet Stream.
type Segment[V Value] interface {
	Source[V]
	Sink[V]
}

// |||||| SWITCH ||||||

func goRangeEach[V Value](ctx signal.Context, outlets []Outlet[V], f func(v V) error, opts ...signal.GoOption) {
	for i, outlet := range outlets {
		outlet = outlet
		if i == 0 {
			signal.GoRange(ctx, outlet.Outlet(), func(v V) error { return f(v) }, opts...)
		}
		signal.GoRange(ctx, outlet.Outlet(), func(v V) error { return f(v) })
	}
}

// |||||| CONFLUENCE ||||||

// Confluence is a segment that reads values from a set of input streams and pipes them to multiple output streams.
// Every output streamImpl receives a copy of the value.
type Confluence[V Value] struct {
	In  []Outlet[V]
	Out []Inlet[V]
}

// InFrom implements the Segment interface.
func (d *Confluence[V]) InFrom(outlets ...Outlet[V]) { d.In = append(d.In, outlets...) }

// OutTo implements the Segment interface.
func (d *Confluence[V]) OutTo(inlets ...Inlet[V]) { d.Out = append(d.Out, inlets...) }

// Flow implements the Segment interface.
func (d *Confluence[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	goRangeEach(ctx, d.In, func(v V) error {
		for _, inlet := range d.Out {
			inlet.Inlet() <- v
		}
		return nil
	}, fo.Signal...)
}

// Delta is similar to Confluence. It reads values from a set of input streams and pipes them to a set of output streams.
// Only one output stream receives a copy of each value from the input stream. This value is taken by the first
// inlet that can accept it.
type Delta[V Value] struct {
	In  []Outlet[V]
	Out []Inlet[V]
}

// InFrom implements the Segment interface.
func (d *Delta[V]) InFrom(outlets ...Outlet[V]) { d.In = append(d.In, outlets...) }

// OutTo implements the Segment interface.
func (d *Delta[V]) OutTo(inlets ...Inlet[V]) { d.Out = append(d.Out, inlets...) }

// Flow implements the Segment interface.
func (d *Delta[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, d.In, func(v V) error {
	o:
		for _, inlet := range d.Out {
			select {
			case inlet.Inlet() <- v:
				break o
			default:
			}
		}
		return nil
	})
}

// |||||| TRANSFORM ||||||

// Transform is a segment that reads values from an input streamImpl, executes a transformation on them,
// and sends them to an out Stream.
type Transform[V Value] struct {
	Transform func(ctx signal.Context, value V) (V, bool, error)
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Transform[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	signal.GoRange(ctx, f.In.Outlet(), func(v V) error {
		v, ok, err := f.Transform(ctx, v)
		if !ok || err != nil {
			return err
		}
		f.Linear.Out.Inlet() <- v
		return nil
	}, fo.Signal...)
}

// |||||| FILTER ||||||

// Filter is a segment that reads values from an input Stream, filters them through a function, and
// optionally discards them to an output Stream.
type Filter[V Value] struct {
	Filter  func(ctx signal.Context, value V) (bool, error)
	Rejects Inlet[V]
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Filter[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	signal.GoRange(ctx, f.In.Outlet(), func(v V) error {
		ok, err := f.Filter(ctx, v)
		if err != nil {
			return err
		}
		if ok {
			f.Linear.Out.Inlet() <- v
		} else if f.Rejects != nil {
			f.Rejects.Inlet() <- v
		}
		return nil
	}, fo.Signal...)
}

// |||||| EMITTER ||||||

type Emitter[V Value] struct {
	Emit     func(ctx signal.Context) (V, error)
	Store    func(ctx signal.Context, value V) error
	Interval time.Duration
	Linear[V]
}

func (e *Emitter[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	signal.GoRange(ctx, e.In.Outlet(), func(batch V) error { return e.Store(ctx, batch) })
	signal.GoTick(ctx, e.Interval, func(t time.Time) error {
		v, err := e.Emit(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e.Out.Inlet() <- v:
		}
		return nil
	})
}

func (e *Emitter[V]) emit(ctx signal.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case v := <-e.In.Outlet():
		if err := e.Store(ctx, v); err != nil {
			return err
		}
	}
	return nil
}
