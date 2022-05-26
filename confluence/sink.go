package confluence

import (
	"github.com/arya-analytics/x/shutdown"
	"sync"
)

type Sink[V Value] interface {
	InFrom(outlets ...Outlet[V])
	Flow(sd shutdown.Shutdown) <-chan error
}

type CoreSink[V Value] struct {
	Sink   func(V) error
	inFrom []Outlet[V]
}

func (s *CoreSink[V]) InFrom(outlet ...Outlet[V]) {
	s.inFrom = append(s.inFrom, outlet...)
}

func (s *CoreSink[V]) OutTo(_ ...Inlet[V]) {
	panic("outTo cannot receive Values from an outTo.")
}

func (s *CoreSink[V]) Flow(sd shutdown.Shutdown) <-chan error {
	errC := make(chan error)
	for _, outlet := range s.inFrom {
		outlet = outlet
		sd.Go(func(sig chan shutdown.Signal) error {
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
	return errC
}

type PoolSink[V Value] struct {
	CoreSink[V]
	mu     sync.Mutex
	Values []V
}

func (p *PoolSink[V]) Flow(sd shutdown.Shutdown) <-chan error {
	for _, outlet := range p.inFrom {
		_outlet := outlet
		sd.Go(func(sig chan shutdown.Signal) error {
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
	return nil
}
