package confluence

import (
	"github.com/arya-analytics/x/signal"
)

// Delta is an abstract Segment that reads values from multiple input streams
// and pipes them to multiple output streams. Delta does not implement the
// Flow method, and is therefore not usable directly. It should be embedded in a
// concrete segment.
type Delta[I, O Value] struct {
	MultiSink[I]
	AbstractMultiSource[O]
}

// DeltaMultiplier reads a value from a set of input streams and copies the value to
// every output stream.
type DeltaMultiplier[V Value] struct{ Delta[V, V] }

// Flow implements the Segment interface.
func (d *DeltaMultiplier[V]) Flow(ctx signal.Context) { d.GoRangeEach(ctx, d.SendToEach) }

// DeltaSelector reads a value from an input stream and writes the value to
// the first output stream that accepts it.
type DeltaSelector[V Value] struct{ Delta[V, V] }

// Flow implements the Segment interface.
func (d *DeltaSelector[V]) Flow(ctx signal.Context) { d.GoRangeEach(ctx, d.SendToAvailable) }

// DeltaTransformSelector reads a value from an input stream, performs a transformation
// on it, and writes the transformed value to the first output stream that accepts it.
type DeltaTransformSelector[I, O Value] struct {
	Delta[I, O]
	TransformFunc[I, O]
}

// Flow implements the Segment interface.
func (d *DeltaTransformSelector[I, O]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(d)
	d.GoRangeEach(ctx, d.transformAndSelect, fo.Signal...)
}

func (d *DeltaTransformSelector[I, O]) transformAndSelect(ctx signal.Context, i I) error {
	o, ok, err := d.ApplyTransform(ctx, i)
	if !ok || err != nil {
		return err
	}
	return d.SendToAvailable(ctx, o)
}

// DeltaTransformMultiplier reads a value from an input stream, performs a
// transformation on it, and writes the transformed value to every output stream.
type DeltaTransformMultiplier[I, O Value] struct {
	Delta[I, O]
	TransformFunc[I, O]
}

// Flow implements the Segment interface.
func (d *DeltaTransformMultiplier[I, O]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(d)
	d.GoRangeEach(ctx, d.transformAndMultiply, fo.Signal...)
}

func (d *DeltaTransformMultiplier[I, O]) transformAndMultiply(ctx signal.Context, i I) error {
	o, ok, err := d.ApplyTransform(ctx, i)
	if !ok || err != nil {
		return err
	}
	return d.SendToEach(ctx, o)
}
