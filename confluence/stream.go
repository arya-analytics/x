package confluence

import "github.com/arya-analytics/x/address"

// NewStream opens a new Stream with the given buffer capacity.
func NewStream[V Value](buffer int) Stream[V] { return &streamImpl[V]{values: make(chan V, buffer)} }

// NewInlet returns an Inlet that wraps the provided channel.
func NewInlet[V Value](ch chan<- V) Inlet[V] { return &inletImpl[V]{values: ch} }

// NewOutlet returns an Outlet that wraps the provided channel.
func NewOutlet[V Value](ch <-chan V) Outlet[V] { return &outletImpl[V]{values: ch} }

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

type inletImpl[V Value] struct {
	addr   address.Address
	values chan<- V
}

// Inlet implements Inlet.
func (i *inletImpl[V]) Inlet() chan<- V { return i.values }

// InletAddress implements Inlet.
func (i *inletImpl[V]) InletAddress() address.Address { return i.addr }

// SetInletAddress implements Inlet.
func (i *inletImpl[V]) SetInletAddress(addr address.Address) { i.addr = addr }

// Close implements inlet.
func (i *inletImpl[V]) Close() { close(i.values) }

type outletImpl[V Value] struct {
	addr   address.Address
	values <-chan V
}

// Outlet implements Outlet.
func (o *outletImpl[V]) Outlet() <-chan V { return o.values }

// OutletAddress implements Outlet.
func (o *outletImpl[V]) OutletAddress() address.Address { return o.addr }

// SetOutletAddress implements Outlet.
func (o *outletImpl[V]) SetOutletAddress(addr address.Address) { o.addr = addr }
