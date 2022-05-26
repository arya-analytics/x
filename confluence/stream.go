package confluence

import "github.com/arya-analytics/x/address"

// Stream represents a stream of values. Each stream has an addressable Outlet
// and an addressable Inlet. These addresses are best represented as unique locations where values
// are received from (Inlet) and sent to (Outlet). It is also generally OK to share a stream across multiple
// Segments, as long as those segments perform are replicates of one another..
type Stream[V Value] interface {
	Inlet[V]
	Outlet[V]
}

type stream[V Value] struct {
	inletAddr  address.Address
	outletAddr address.Address
	Values     chan V
}

// Inlet implements Stream.
func (c *stream[V]) Inlet() chan<- V { return c.Values }

// Outlet represents Stream.
func (c *stream[V]) Outlet() <-chan V { return c.Values }

// InletAddress implements Stream.
func (c *stream[V]) InletAddress() address.Address { return c.inletAddr }

// SetInletAddress implements Stream.
func (c *stream[V]) SetInletAddress(addr address.Address) { c.inletAddr = addr }

// OutletAddress implements Stream.
func (c *stream[V]) OutletAddress() address.Address { return c.outletAddr }

// SetOutletAddress implements Stream.
func (c *stream[V]) SetOutletAddress(addr address.Address) { c.outletAddr = addr }

// NewStream opens a new Stream with the given buffer capacity.
func NewStream[V Value](buffer int) Stream[V] { return &stream[V]{Values: make(chan V, buffer)} }
