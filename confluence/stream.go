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
	values     chan V
}

// Inlet implements Stream.
func (s *streamImpl[V]) Inlet() chan<- V { return s.values }

// Outlet represents Stream.
func (s *streamImpl[V]) Outlet() <-chan V { return s.values }

// InletAddress implements Stream.
func (s *streamImpl[V]) InletAddress() address.Address { return s.inletAddr }

func (s *streamImpl[V]) Close() { close(s.values) }

// SetInletAddress implements Stream.
func (s *streamImpl[V]) SetInletAddress(addr address.Address) { s.inletAddr = addr }

// OutletAddress implements Stream.
func (s *streamImpl[V]) OutletAddress() address.Address { return s.outletAddr }

// SetOutletAddress implements Stream.
func (s *streamImpl[V]) SetOutletAddress(addr address.Address) { s.outletAddr = addr }

// NewStream opens a new Stream with the given buffer capacity.
func NewStream[V Value](buffer int) Stream[V] { return &streamImpl[V]{values: make(chan V, buffer)} }
