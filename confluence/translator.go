package confluence

import (
	"github.com/arya-analytics/x/signal"
)

// Translator is a pseudo-segment that reads values from an input stream,
// translates them to a different value type, and writes them to an output stream.
// The translator is a pseudo-segment because it does not classically implement
// the Segment interface. It implements the Source interface for the input type
// and the Sink interface for the output type.
type Translator[I, O Value] interface {
	Sink[I]
	Source[O]
}

// CoreTranslator is a basic template implementation of the Translator interface.
type CoreTranslator[I, O Value] struct {
	Translate func(ctx signal.Context, value I) (O, error)
	UnarySource[O]
	UnarySink[I]
}

func (t *CoreTranslator[I, O]) OutTo(inlets ...Inlet[O]) { t.UnarySource.OutTo(inlets...) }

func (t *CoreTranslator[I, O]) InFrom(outlets ...Outlet[I]) { t.UnarySink.InFrom(outlets...) }

// Flow implements the Flow interface.
func (t *CoreTranslator[I, O]) Flow(ctx signal.Context) {
	signal.GoRange(ctx, t.UnarySink.In.Outlet(), func(v I) error {
		tv, err := t.Translate(ctx, v)
		if err != nil {
			return err
		}
		t.UnarySource.Out.Inlet() <- tv
		return nil
	})
}
