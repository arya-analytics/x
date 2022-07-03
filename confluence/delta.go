package confluence

import (
	"github.com/arya-analytics/x/signal"
)

// DeltaAbstract is an abstract Segment that reads values from multiple input streams
// and pipes them to multiple output streams. DeltaAbstract does not implement the
// Flow method, and is therefore not usable directly. It should be embedded in a
// concrete segment.
type DeltaAbstract[I, O Value] struct {
	In  []Outlet[I]
	Out []Inlet[O]
}

// InFrom implements the Segment interface.
func (d *DeltaAbstract[I, O]) InFrom(outlets ...Outlet[I]) { d.In = append(d.In, outlets...) }

// OutTo implements the Segment interface.
func (d *DeltaAbstract[I, O]) OutTo(inlets ...Inlet[O]) { d.Out = append(d.Out, inlets...) }

// DeltaMultiplier reads a value from a set of input streams and copies the value to
// every output stream.
type DeltaMultiplier[V Value] struct{ DeltaAbstract[V, V] }

// Flow implements the Segment interface.
func (d *DeltaMultiplier[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, d.In, func(v V) error {
		for _, inlet := range d.Out {
			inlet.Inlet() <- v
		}
		return nil
	})
}

// DeltaSelector reads a value from an input stream and writes the value to
// the first output stream that accepts it.
type DeltaSelector[V Value] struct{ DeltaAbstract[V, V] }

// Flow implements the Segment interface.
func (d *DeltaSelector[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, d.In, func(v V) error {
		selectAndSend(d.Out, v)
		return nil
	})
}

// DeltaTransformSelector reads a value from an input stream, performs a transformation
// on it, and writes the transformed value to the first output stream that accepts it.
type DeltaTransformSelector[I, O Value] struct {
	DeltaAbstract[I, O]
	Transform[I, O]
}

// Flow implements the Segment interface.
func (d *DeltaTransformSelector[I, O]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRangeEach(ctx, d.In, func(i I) error {
		o, ok, err := d.Apply(ctx, i)
		if !ok || err != nil {
			return err
		}
		selectAndSend(d.Out, o)
		return nil
	}, opts...)
}

// DeltaTransformMultiplier reads a value from an input stream, performs a
// transformation on it, and writes the transformed value to every output stream.
type DeltaTransformMultiplier[I, O Value] struct {
	DeltaAbstract[I, O]
	Transform[I, O]
}

// Flow implements the Segment interface.
func (d *DeltaTransformMultiplier[I, O]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRangeEach(ctx, d.In, func(i I) error {
		v, ok, err := d.Apply(ctx, i)
		if !ok || err != nil {
			return err
		}
		multiply(d.Out, v)
		return nil
	}, opts...)
}
