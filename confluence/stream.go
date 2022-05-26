package confluence

import "github.com/arya-analytics/x/address"

// Stream represents a streamImpl of values. Each streamImpl has an addressable Outlet
// and an addressable Inlet. These addresses are best represented as unique locations where values
// are received from (Inlet) and sent to (Outlet). It is also generally OK to share a streamImpl across multiple
// Segments, as long as those segments perform are replicates of one another..
type Stream[V Value] interface {
	Inlet[V]
	Outlet[V]
}

type streamImpl[V Value] struct {
	inletAddr  address.Address
	outletAddr address.Address
	Values     chan V
}

// Inlet implements Stream.
func (c *streamImpl[V]) Inlet() chan<- V { return c.Values }

// Outlet represents Stream.
func (c *streamImpl[V]) Outlet() <-chan V { return c.Values }

// InletAddress implements Stream.
func (c *streamImpl[V]) InletAddress() address.Address { return c.inletAddr }

// SetInletAddress implements Stream.
func (c *streamImpl[V]) SetInletAddress(addr address.Address) { c.inletAddr = addr }

// OutletAddress implements Stream.
func (c *streamImpl[V]) OutletAddress() address.Address { return c.outletAddr }

// SetOutletAddress implements Stream.
func (c *streamImpl[V]) SetOutletAddress(addr address.Address) { c.outletAddr = addr }

// NewStream opens a new Stream with the given buffer capacity.
func NewStream[V Value](buffer int) Stream[V] { return &streamImpl[V]{Values: make(chan V, buffer)} }
