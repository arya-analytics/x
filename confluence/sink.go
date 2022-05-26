package confluence

import (
	"github.com/arya-analytics/x/shutdown"
	"sync"
)

// Sink is a segment that can accept values from outlets, but cannot
// send values to inlets. Sinks are typically used to write values
// to network pipes or persistent storage.
type Sink[V Value] interface {
	InFrom(outlets ...Outlet[V])
	Flow[V]
}

// CoreSink is a basic implementation of Sink. It implements the Segment
// interface, but will panic if any inlets are added.
type CoreSink[V Value] struct {
	Sink   func(V) error
	inFrom []Outlet[V]
}

// InFrom implements the Segment interface.
func (s *CoreSink[V]) InFrom(outlet ...Outlet[V]) { s.inFrom = append(s.inFrom, outlet...) }

// OutTo implements the Segment interface. This method will panic if called.
func (s *CoreSink[V]) OutTo(_ ...Inlet[V]) { panic("sinks cannot pipe values out") }

// Flow implements the Segment interface.
func (s *CoreSink[V]) Flow(ctx Context) {
	for _, outlet := range s.inFrom {
		outlet = outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					if err := s.Sink(v); err != nil {
						return err
					}
				}
			}
		})
	}
}

// PoolSink is a Sink that collects values from multiple outlets into a slice.
type PoolSink[V Value] struct {
	CoreSink[V]
	mu     sync.Mutex
	Values []V
}

// Flow implements the Segment interface.
func (p *PoolSink[V]) Flow(ctx Context) {
	for _, outlet := range p.inFrom {
		_outlet := outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-_outlet.Outlet():
					p.mu.Lock()
					p.Values = append(p.Values, v)
					p.mu.Unlock()
				}
			}
		})
	}
}
