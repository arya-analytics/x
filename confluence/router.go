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
	From() []address.Address
	To() []address.Address
	Cap() int
	Route(p *Pipeline[V]) error
}

type UnaryRouter[V Value] struct {
	FromAddr address.Address
	ToAddr   address.Address
	Capacity int
}

func (u UnaryRouter[V]) Route(p *Pipeline[V]) error {
	return p.route(u.FromAddr, u.ToAddr, NewStream[V](u.Capacity))
}

func (u UnaryRouter[V]) From() []address.Address { return []address.Address{u.FromAddr} }

func (u UnaryRouter[V]) To() []address.Address { return []address.Address{u.ToAddr} }

func (u UnaryRouter[V]) Cap() int { return u.Capacity }

type MultiRouter[V Value] struct {
	FromAddresses []address.Address
	ToAddresses   []address.Address
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
	panic("invalid stitch provided to router")
}

func (m MultiRouter[V]) From() []address.Address { return m.FromAddresses }

func (m MultiRouter[V]) To() []address.Address { return m.ToAddresses }

func (m MultiRouter[V]) Cap() int { return m.Capacity }

func (m *MultiRouter[V]) linear(p *Pipeline[V]) error {
	stream := NewStream[V](m.Capacity)
	return m.iterAddr(func(from address.Address, to address.Address) error { return p.route(from, to, stream) })
}

func (m MultiRouter[V]) weave(p *Pipeline[V]) error {
	return m.iterAddr(func(from address.Address, to address.Address) error {
		return UnaryRouter[V]{from, to, m.Capacity}.Route(p)
	})
}

func (m MultiRouter[V]) divergent(p *Pipeline[V]) error {
	return m.iterFrom(func(from address.Address) error {
		stream := NewStream[V](m.Capacity)
		return m.iterTo(func(to address.Address) error { return p.route(from, to, stream) })
	})
}

func (m MultiRouter[V]) convergent(p *Pipeline[V]) error {
	return m.iterTo(func(to address.Address) error {
		stream := NewStream[V](m.Capacity)
		return m.iterFrom(func(from address.Address) error { return p.route(from, to, stream) })
	})
}

func (m MultiRouter[V]) iterAddr(f func(from address.Address, to address.Address) error) error {
	for _, fromAddr := range m.FromAddresses {
		for _, toAddr := range m.ToAddresses {
			if err := f(fromAddr, toAddr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m MultiRouter[v]) iterFrom(f func(from address.Address) error) error {
	for _, fromAddr := range m.FromAddresses {
		if err := f(fromAddr); err != nil {
			return err
		}
	}
	return nil
}

func (m MultiRouter[V]) iterTo(f func(to address.Address) error) error {
	for _, toAddr := range m.ToAddresses {
		if err := f(toAddr); err != nil {
			return err
		}
	}
	return nil
}
