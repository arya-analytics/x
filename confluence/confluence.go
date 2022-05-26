package confluence

import (
	"errors"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
)

// |||||| VALUE ||||||

type Value any

// |||||| STREAMS ||||||

type Inlet[V Value] interface {
	Inlet() chan<- V
	InletAddress() address.Address
	SetInletAddress(address.Address)
}

type Outlet[V Value] interface {
	Outlet() <-chan V
	OutletAddress() address.Address
	SetOutletAddress(address.Address)
}

type OutletMap[V Value] map[address.Address]Outlet[V]

type InletMap[V Value] map[address.Address]Inlet[V]

type Inlets[V Value] []Inlet[V]

type Outlets[V Value] []Outlet[V]

type Stream[V Value] interface {
	Inlet[V]
	Outlet[V]
}

type CoreStream[V Value] struct {
	inletAddr  address.Address
	outletAddr address.Address
	Values     chan V
}

func (c *CoreStream[V]) Inlet() chan<- V { return c.Values }

func (c *CoreStream[V]) Outlet() <-chan V { return c.Values }

func (c *CoreStream[V]) InletAddress() address.Address { return c.inletAddr }

func (c *CoreStream[V]) SetInletAddress(addr address.Address) { c.inletAddr = addr }

func (c *CoreStream[V]) OutletAddress() address.Address { return c.outletAddr }

func (c *CoreStream[V]) SetOutletAddress(addr address.Address) { c.outletAddr = addr }

func NewStream[V Value](buffer int) Stream[V] { return &CoreStream[V]{Values: make(chan V, buffer)} }

// |||||| SEGMENT ||||||

type Segment[V Value] interface {
	Source[V]
	Sink[V]
}

// |||||| ROUTER ||||||

type Router[V Value] struct {
	Route  func(v V) address.Address
	inFrom []Outlet[V]
	outTo  map[address.Address]Inlet[V]
}

func (r *Router[V]) InFrom(outlets ...Outlet[V]) {
	r.inFrom = append(r.inFrom, outlets...)
}

func (r *Router[V]) OutTo(inlets ...Inlet[V]) {
	if r.outTo == nil {
		r.outTo = make(map[address.Address]Inlet[V])
	}
	for _, inlet := range inlets {
		r.outTo[inlet.InletAddress()] = inlet
	}
}

func (r *Router[V]) Flow(sd shutdown.Shutdown) <-chan error {
	errC := make(chan error)
	for _, outlet := range r.inFrom {
		sd.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					addr := r.Route(v)
					inlet, ok := r.outTo[addr]
					if !ok {
						errC <- address.NotFoundError{Address: addr}
						continue
					}
					inlet.Inlet() <- v
				}
			}
		})
	}
	return errC
}

// |||||| CONFLUENCE ||||||

type Confluence[V Value] struct {
	inFrom []Outlet[V]
	outTo  []Inlet[V]
}

func (d *Confluence[V]) InFrom(outlets ...Outlet[V]) {
	d.inFrom = append(d.inFrom, outlets...)
}

func (d *Confluence[V]) OutTo(inlets ...Inlet[V]) {
	d.outTo = append(d.outTo, inlets...)
}

func (d *Confluence[V]) Flow(sd shutdown.Shutdown) <-chan error {
	for _, outlet := range d.inFrom {
		sd.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					for _, inlet := range d.outTo {
						inlet.Inlet() <- v
					}
				}
			}
		})
	}
	return nil
}

// |||||| TRANSFORM ||||||

type Transform[V Value] struct {
	Transform func(V) V
	Linear[V]
}

func (f *Transform[V]) Flow(sd shutdown.Shutdown) <-chan error {
	errC := make(chan error)
	sd.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case v := <-f.inFrom.Outlet():
				f.outTo.Inlet() <- f.Transform(v)
			}
		}
	})
	return errC
}

// |||||| FILTER ||||||

type Filter[V Value] struct {
	Filter  func(Value) bool
	Rejects Inlet[V]
	Linear[V]
}

func (f *Filter[V]) Flow(sd shutdown.Shutdown) <-chan error {
	errC := make(chan error)
	sd.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case v := <-f.inFrom.Outlet():
				if f.Filter(v) {
					f.outTo.Inlet() <- v
				} else if f.Rejects != nil {
					f.Rejects.Inlet() <- v
				}
			}
		}
	})
	return errC
}

// |||||| LINEAR ||||||

type Linear[V Value] struct {
	inFrom Outlet[V]
	outTo  Inlet[V]
}

func (l *Linear[V]) InFrom(outlets ...Outlet[V]) {
	if l.inFrom != nil {
		panic("inFrom already set")
	}
	if len(outlets) != 1 {
		panic("linear inFrom must have exactly one outlets")
	}
	l.inFrom = outlets[0]
}

func (l *Linear[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) != 1 {
		panic("linear inFrom must have exactly one outlets")
	}
	l.outTo = inlets[0]
}

func (l *Linear[V]) Flow(sd shutdown.Shutdown) <-chan error {
	errC := make(chan error)
	sd.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case l.outTo.Inlet() <- <-l.inFrom.Outlet():
			}
		}
	})
	return errC
}

// |||||| MAP ||||||

type Map[V Value] struct {
	Map    func(addr address.Address, v Value) address.Address
	inFrom map[address.Address]Outlet[V]
	outTo  map[address.Address]Inlet[V]
}

func (m *Map[V]) InFrom(outlet ...Outlet[V]) {
	if m.inFrom == nil {
		m.inFrom = make(map[address.Address]Outlet[V])
	}
	for _, o := range outlet {
		m.inFrom[o.OutletAddress()] = o
	}
}

func (m *Map[V]) OutTo(inlet ...Inlet[V]) {
	if m.outTo == nil {
		m.outTo = make(map[address.Address]Inlet[V])
	}
	for _, i := range inlet {
		m.outTo[i.InletAddress()] = i
	}
}

func (m *Map[V]) Flow(sd shutdown.Shutdown) <-chan error {
	for inAddr, outlet := range m.inFrom {
		sd.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					m.outTo[m.Map(inAddr, v)].Inlet() <- v
				}
			}
		})
	}
	return nil
}

type Route struct {
	From address.Address
	To   address.Address
}

type Composite[V Value] struct {
	Segments map[address.Address]Segment[V]
	Routes   map[address.Address]map[address.Address]Stream[V]
}

func (c *Composite[V]) Route(from address.Address, to address.Address, buffer int) error {
	fromSeg, ok := c.getSegment(from)
	if !ok {
		return ErrNotFound
	}
	toSeg, ok := c.getSegment(to)
	if !ok {
		return ErrNotFound
	}
	stream := NewStream[V](buffer)
	stream.SetInletAddress(to)
	stream.SetOutletAddress(from)
	toSeg.InFrom(stream)
	fromSeg.OutTo(stream)
	c.setStream(from, to, stream)
	return nil
}

func (c *Composite[V]) AddSegment(addr address.Address, seg Segment[V]) {
	c.setSegment(addr, seg)
}

func (c *Composite[V]) getStream(from address.Address, to address.Address) (Stream[V], bool) {
	if c.Routes == nil {
		c.Routes = make(map[address.Address]map[address.Address]Stream[V])
		return nil, false
	}
	opts := c.Routes[from]
	if opts == nil {
		c.Routes[from] = make(map[address.Address]Stream[V])
		return nil, false
	}
	stream, ok := opts[to]
	return stream, ok
}

func (c *Composite[V]) setStream(from address.Address, to address.Address, stream Stream[V]) {
	if c.Routes == nil {
		c.Routes = make(map[address.Address]map[address.Address]Stream[V])
	}
	if c.Routes[from] == nil {
		c.Routes[from] = make(map[address.Address]Stream[V])
	}
	c.Routes[from][to] = stream
}

func (c *Composite[V]) getSegment(addr address.Address) (Segment[V], bool) {
	if c.Segments == nil {
		c.Segments = make(map[address.Address]Segment[V])
		return nil, false
	}
	seg, ok := c.Segments[addr]
	return seg, ok
}

func (c *Composite[V]) setSegment(addr address.Address, seg Segment[V]) {
	if c.Segments == nil {
		c.Segments = make(map[address.Address]Segment[V])
	}
	c.Segments[addr] = seg
}

func (c *Composite[V]) Flow(sd shutdown.Shutdown) {
	for _, seg := range c.Segments {
		seg.Flow(sd)
	}
}

var (
	ErrNotFound = errors.New("not found")
)
