package confluence

import (
	"github.com/arya-analytics/x/shutdown"
	"sync"
)

type Sink[V Value] interface {
	InFrom(outlets ...Outlet[V])
	Flow(sd shutdown.Shutdown) <-chan error
}

type AbstractSink[V Value] struct {
	inFrom []Outlet[V]
}

func (s *AbstractSink[V]) InFrom(outlet ...Outlet[V]) {
	s.inFrom = append(s.inFrom, outlet...)
}

func (s *AbstractSink[V]) OutTo(_ ...Inlet[V]) {
	panic("sink cannot receive Values from an outlet.")
}

type PoolSink[V Value] struct {
	AbstractSink[V]
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
