package confluence

import (
	"github.com/arya-analytics/x/address"
	"github.com/cockroachdb/errors"
)

// Stitch is the method a Router  uses to stitch together the segments specified in its route.
type Stitch byte

const (
	// StitchUnary is the default stitching method. It means the router will create a single stream and connected
	// it to all input (from) segments and output (to) segments.
	StitchUnary Stitch = iota
	// StitchWeave is a stitching method that means the router will create a stream for each unique combination of
	// input (from) and output (to) segments.
	StitchWeave
	// StitchDivergent is a stitching where a router creates a stream for each input (from) segment and connects it
	// to all output (to) segments.
	StitchDivergent
	// StitchConvergent is a stitching where a router creates a stream for each output (to) segment and connects it
	// to all input (from) segments.
	StitchConvergent
)

// ErrNotFound is returned when a Router cannot find an address.
var ErrNotFound = errors.New("[confluence] - segment not found")

func notFound(addr address.Address) error { return errors.Wrapf(ErrNotFound, "address %s", addr) }

type Router[V Value] interface {
	sourceTargets() []address.Address
	sinkTargets() []address.Address
	capacity() int
	Route(p *Pipeline[V]) error
}

type UnaryRouter[V Value] struct {
	SourceTarget address.Address
	SinkTarget   address.Address
	Capacity     int
}

func (u UnaryRouter[V]) Route(p *Pipeline[V]) error {
	return p.route(u.SourceTarget, u.SinkTarget, NewStream[V](u.Capacity))
}

// sourceTargets implements Router.
func (u UnaryRouter[V]) sourceTargets() []address.Address { return []address.Address{u.SourceTarget} }

// sinkTargets implements Router.
func (u UnaryRouter[V]) sinkTargets() []address.Address { return []address.Address{u.SinkTarget} }

// capacity implements Router.
func (u UnaryRouter[V]) capacity() int { return u.Capacity }

type MultiRouter[V Value] struct {
	SourceTargets []address.Address
	SinkTargets   []address.Address
	Capacity      int
	Stitch        Stitch
}

func (m MultiRouter[V]) Route(p *Pipeline[V]) error {
	switch m.Stitch {
	case StitchUnary:
		return m.linear(p)
	case StitchWeave:
		return m.weave(p)
	case StitchDivergent:
		return m.divergent(p)
	case StitchConvergent:
		return m.convergent(p)
	}
	panic("[confluence.Router] - invalid stitch provided")
}

func (m MultiRouter[V]) sourceTargets() []address.Address { return m.SourceTargets }

func (m MultiRouter[V]) sinkTargets() []address.Address { return m.SinkTargets }

func (m MultiRouter[V]) capacity() int { return m.Capacity }

func (m *MultiRouter[V]) linear(p *Pipeline[V]) error {
	stream := NewStream[V](m.Capacity)
	return m.iterAddresses(func(from address.Address, to address.Address) error { return p.route(from, to, stream) })
}

func (m MultiRouter[V]) weave(p *Pipeline[V]) error {
	return m.iterAddresses(func(from address.Address, to address.Address) error {
		return UnaryRouter[V]{from, to, m.Capacity}.Route(p)
	})
}

func (m MultiRouter[V]) divergent(p *Pipeline[V]) error {
	return m.iterSources(func(from address.Address) error {
		stream := NewStream[V](m.Capacity)
		return m.iterSinks(func(to address.Address) error { return p.route(from, to, stream) })
	})
}
func (m MultiRouter[V]) convergent(p *Pipeline[V]) error {
	return m.iterSinks(func(to address.Address) error {
		stream := NewStream[V](m.Capacity)
		return m.iterSources(func(from address.Address) error { return p.route(from, to, stream) })
	})
}

func (m MultiRouter[V]) iterAddresses(f func(source, sink address.Address) error) error {
	for _, sourceAddr := range m.SourceTargets {
		for _, sinkAddr := range m.SinkTargets {
			if err := f(sourceAddr, sinkAddr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m MultiRouter[v]) iterSources(f func(source address.Address) error) error {
	for _, fromAddr := range m.SourceTargets {
		if err := f(fromAddr); err != nil {
			return err
		}
	}
	return nil
}

func (m MultiRouter[V]) iterSinks(f func(to address.Address) error) error {
	for _, toAddr := range m.SinkTargets {
		if err := f(toAddr); err != nil {
			return err
		}
	}
	return nil
}
