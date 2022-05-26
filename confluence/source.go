package confluence

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/rand"
	"github.com/arya-analytics/x/shutdown"
)

type Source[V Value] interface {
	OutTo(inlets ...Inlet[V])
	Flow(sd shutdown.Shutdown) <-chan error
}

type AbstractSource[V Value] struct {
	outTo map[address.Address]Inlet[V]
}

func (s *AbstractSource[V]) InFrom(_ ...Outlet[V]) {
	panic("source cannot receive Values from an outlet.")
}

func (s *AbstractSource[V]) OutTo(inlets ...Inlet[V]) {
	if s.outTo == nil {
		s.outTo = make(map[address.Address]Inlet[V])
	}
	for _, inlet := range inlets {
		s.outTo[inlet.InletAddress()] = inlet
	}
}

type PoolSource[V Value] struct {
	AbstractSource[V]
	Values []V
}

func (s *PoolSource[V]) Flow(sd shutdown.Shutdown) <-chan error {
	sd.Go(func(sig chan shutdown.Signal) error {
		for _, v := range s.Values {
			select {
			case <-sig:
				return nil
			default:
				outTo := rand.MapValue(s.outTo)
				outTo.Inlet() <- v
			}
		}
		return nil
	})
	return nil
}