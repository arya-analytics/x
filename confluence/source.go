package confluence

import (
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
)

// Source is a segment that can send values to inlets
type Source[V Value] interface {
	OutTo(inlets ...Inlet[V])
	Flow[V]
}

// CoreSource is a basic implementation of a Source. It implements the Segment interface,
// but will panic if any outlets are added.
type CoreSource[V Value] struct {
	outTo []Inlet[V]
}

func (s *CoreSource[V]) InFrom(_ ...Outlet[V]) { panic("sources cannot receive values") }

func (s *CoreSource[V]) OutTo(inlets ...Inlet[V]) { s.outTo = append(s.outTo, inlets...) }

// Writer implements the Segment interface that allows the caller to write values to it.
type Writer[V Value] struct {
	CoreSource[V]
	Values <-chan V
}

func (s *Writer[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for v := range s.Values {
			select {
			case <-sig:
				return nil
			case rand.Slice(s.outTo).Inlet() <- v:
			}
		}
		return nil
	})
}
