package confluence

import "github.com/arya-analytics/x/shutdown"

// Translator is a pseudo-segment that reads values from an input stream,
// translates them to a different value type, and writes them to an output stream.
// The translator is a pseudo-segment because it does not classically implement
// the Segment interface. It implements the Source interface for the input type
// and the Sink interface for the output type.
type Translator[I, O Value] struct {
	Translate func(ctx Context, value I) O
	UnarySource[O]
	UnarySink[I]
}

func (t *Translator[I, O]) OutTo(inlets ...Inlet[O]) { t.UnarySource.OutTo(inlets...) }

func (t *Translator[I, O]) InFrom(outlets ...Outlet[I]) { t.UnarySink.InFrom(outlets...) }

// Flow implements the Flow interface.
func (t *Translator[I, O]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case t.UnarySource.Out.Inlet() <- t.Translate(ctx, <-t.UnarySink.In.Outlet()):
			}
		}
	})

}
