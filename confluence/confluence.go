package confluence

import (
	"context"
	"errors"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
)

// |||||| VALUE ||||||

// Value represents an item that can be sent through a Stream.
type Value any

// |||||| STREAMS ||||||

// Inlet is a Stream that accepts values and can be addressed.
type Inlet[V Value] interface {
	// Inlet pipes a value through the stream.
	Inlet() chan<- V
	// InletAddress returns the address of the inlet.
	InletAddress() address.Address
	// SetInletAddress sets the address of the inlet.
	SetInletAddress(address.Address)
}

// Outlet is a Stream that emits values and can be addressed.
type Outlet[V Value] interface {
	// Outlet receives a value from the stream.
	Outlet() <-chan V
	// OutletAddress returns the address of the outlet.
	OutletAddress() address.Address
	// SetOutletAddress sets the address of the outlet.
	SetOutletAddress(address.Address)
}

// |||||| SEGMENT ||||||

type Flow[V Value] interface {
	Flow(ctx Context)
}

// Segment is an interface that accepts values from an Inlet Stream, does some operation on them, and returns the
// values through an Outlet Stream.
type Segment[V Value] interface {
	Source[V]
	Sink[V]
}

// |||||| CONTEXT ||||||

type Context struct {
	Ctx      context.Context
	ErrC     chan error
	Shutdown shutdown.Shutdown
}

func DefaultContext() Context {
	return Context{
		Ctx:      context.Background(),
		ErrC:     make(chan error),
		Shutdown: shutdown.New(),
	}
}

// |||||| ROUTER ||||||

// Router is a segment that reads values from a set of input streams, resolves their address,
// and sends them to the appropriate output streams. Router is a more efficient implementation of
// Map for when the addresses of input streams are not important.
type Router[V Value] struct {
	// Route is a function that resolves the address of a Value.
	Route  func(v V) address.Address
	inFrom []Outlet[V]
	outTo  map[address.Address]Inlet[V]
}

// InFrom implements the Segment interface.
func (r *Router[V]) InFrom(outlets ...Outlet[V]) {
	r.inFrom = append(r.inFrom, outlets...)
}

// OutTo implements the Segment interface.
func (r *Router[V]) OutTo(inlets ...Inlet[V]) {
	if r.outTo == nil {
		r.outTo = make(map[address.Address]Inlet[V])
	}
	for _, inlet := range inlets {
		r.outTo[inlet.InletAddress()] = inlet
	}
}

// Flow implements the Segment interface.
func (r *Router[V]) Flow(ctx Context) {
	for _, outlet := range r.inFrom {
		_outlet := outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-_outlet.Outlet():
					addr := r.Route(v)
					inlet, ok := r.outTo[addr]
					if !ok {
						panic("address not found")
					}
					inlet.Inlet() <- v
				}
			}
		})
	}
}

// |||||| CONFLUENCE ||||||

// Confluence is a segment that reads values from a set of input streams and pipes them to multiple output streams.
// Every output stream receives a copy of the value.
type Confluence[V Value] struct {
	inFrom []Outlet[V]
	outTo  []Inlet[V]
}

// InFrom implements the Segment interface.
func (d *Confluence[V]) InFrom(outlets ...Outlet[V]) {
	d.inFrom = append(d.inFrom, outlets...)
}

// OutTo implements the Segment interface.
func (d *Confluence[V]) OutTo(inlets ...Inlet[V]) {
	d.outTo = append(d.outTo, inlets...)
}

// Flow implements the Segment interface.
func (d *Confluence[V]) Flow(ctx Context) {
	for _, outlet := range d.inFrom {
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
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
}

// Delta is similar to confluence. It reads values from a set of input streams and pipes them to a set of output streams.
// Only one output stream receives a copy of each value from input stream. This value is taken by the first
// inlet that can accept it.
type Delta[V Value] struct {
	inFrom []Outlet[V]
	outTo  []Inlet[V]
}

// InFrom implements the Segment interface.
func (d *Delta[V]) InFrom(outlets ...Outlet[V]) {
	d.inFrom = append(d.inFrom, outlets...)
}

// OutTo implements the Segment interface.
func (d *Delta[V]) OutTo(inlets ...Inlet[V]) {
	d.outTo = append(d.outTo, inlets...)
}

// Flow implements the Segment interface.
func (d *Delta[V]) Flow(ctx Context) {
	for _, outlet := range d.inFrom {
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
				o:
					for _, inlet := range d.outTo {
						select {
						case inlet.Inlet() <- v:
							break o
						default:
						}
					}
				}
			}
		})
	}
}

// |||||| TRANSFORM ||||||

// Transform is a segment that reads values from an input stream, executes a transformation on them,
// and sends them to an ouput stream.
type Transform[V Value] struct {
	Transform func(V) V
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Transform[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case v := <-f.inFrom.Outlet():
				f.outTo.Inlet() <- f.Transform(v)
			}
		}
	})
}

// |||||| FILTER ||||||

// Filter is a segment that reads values from an input stream, filters them through a function, and
// optionally discards them to an output stream.
type Filter[V Value] struct {
	Filter  func(V) bool
	Rejects Inlet[V]
	Linear[V]
}

// Flow implements the Segment interface.
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

// Linear is a segment that reads values from a single input stream and pipes them to a single output stream.
type Linear[V Value] struct {
	inFrom Outlet[V]
	outTo  Inlet[V]
}

// InFrom implements the Segment interface.
func (l *Linear[V]) InFrom(outlets ...Outlet[V]) {
	if l.inFrom != nil {
		panic("inFrom already set")
	}
	if len(outlets) != 1 {
		panic("linear inFrom must have exactly one outlets")
	}
	l.inFrom = outlets[0]
}

// OutTo implements the Segment interface.
func (l *Linear[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) != 1 {
		panic("linear inFrom must have exactly one outlets")
	}
	l.outTo = inlets[0]
}

// Flow implements the Segment interface.
func (l *Linear[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case l.outTo.Inlet() <- <-l.inFrom.Outlet():
			}
		}
	})
}

// |||||| MAP ||||||

// Map is a segment that reads values from an addresses input stream, maps them to an output address, and
// sends them. Map is a relatively inefficient Segment, use Router when possible.
type Map[V Value] struct {
	Map    func(addr address.Address, v Value) address.Address
	inFrom map[address.Address]Outlet[V]
	outTo  map[address.Address]Inlet[V]
}

// InFrom implements the Segment interface.
func (m *Map[V]) InFrom(outlet ...Outlet[V]) {
	if m.inFrom == nil {
		m.inFrom = make(map[address.Address]Outlet[V])
	}
	for _, o := range outlet {
		m.inFrom[o.OutletAddress()] = o
	}
}

// OutTo implements the Segment interface.
func (m *Map[V]) OutTo(inlet ...Inlet[V]) {
	if m.outTo == nil {
		m.outTo = make(map[address.Address]Inlet[V])
	}
	for _, i := range inlet {
		m.outTo[i.InletAddress()] = i
	}
}

// Flow implements the Segment interface.
func (m *Map[V]) Flow(ctx Context) {
	for inAddr, outlet := range m.inFrom {
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
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
}

// ErrNotFound is returned when a value is not found in a Composite.
var ErrNotFound = errors.New("not found")

// Composite is a segment that allows the caller to compose a set of sub-segments in a routed manner.
type Composite[V Value] struct {
	segments      map[address.Address]Segment[V]
	routes        map[address.Address]map[address.Address]Stream[V]
	inFromToAddr  address.Address
	outToFromAddr address.Address
	Linear[V]
}

// Route adds a Stream connecting the Segment at address from to the Segment at address to. Sets the buffer size
// on the Stream to buffer.
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
	fromSeg.OutTo(stream)
	toSeg.InFrom(stream)
	c.setStream(from, to, stream)
	return nil
}

// RouteInletTo routes from the inlet of the Composite to the given Segment.
func (c *Composite[V]) RouteInletTo(to address.Address) error {
	if _, ok := c.getSegment(to); !ok {
		return ErrNotFound
	}
	c.inFromToAddr = to
	return nil
}

// RouteOutletFrom routes from the given Segment to the outlet of the Composite.
func (c *Composite[V]) RouteOutletFrom(from address.Address) error {
	if _, ok := c.getSegment(from); !ok {
		return ErrNotFound
	}
	c.outToFromAddr = from
	return nil
}

// SetSegment sets the Segment at the given address.
func (c *Composite[V]) SetSegment(addr address.Address, seg Segment[V]) {
	c.setSegment(addr, seg)
}

// Flow implements the Segment interface.
func (c *Composite[V]) Flow(ctx Context) {
	for _, seg := range c.segments {
		seg.Flow(ctx)
	}
}

func (c *Composite[V]) runRouteInFrom() {
	toSeg, _ := c.getSegment(c.inFromToAddr)
	c.inFrom.SetOutletAddress(c.inFromToAddr)
	toSeg.InFrom(c.inFrom)
}

func (c *Composite[V]) runRouteOutTo() {
	fromSeg, _ := c.getSegment(c.outToFromAddr)
	c.outTo.SetInletAddress(c.outToFromAddr)
	fromSeg.OutTo(c.outTo)
}

func (c *Composite[V]) getStream(from address.Address, to address.Address) (Stream[V], bool) {
	if c.routes == nil {
		c.routes = make(map[address.Address]map[address.Address]Stream[V])
		return nil, false
	}
	opts := c.routes[from]
	if opts == nil {
		c.routes[from] = make(map[address.Address]Stream[V])
		return nil, false
	}
	stream, ok := opts[to]
	return stream, ok
}

func (c *Composite[V]) setStream(from address.Address, to address.Address, stream Stream[V]) {
	if c.routes == nil {
		c.routes = make(map[address.Address]map[address.Address]Stream[V])
	}
	if c.routes[from] == nil {
		c.routes[from] = make(map[address.Address]Stream[V])
	}
	c.routes[from][to] = stream
}

func (c *Composite[V]) getSegment(addr address.Address) (Segment[V], bool) {
	if c.segments == nil {
		c.segments = make(map[address.Address]Segment[V])
		return nil, false
	}
	seg, ok := c.segments[addr]
	return seg, ok
}

func (c *Composite[V]) setSegment(addr address.Address, seg Segment[V]) {
	if c.segments == nil {
		c.segments = make(map[address.Address]Segment[V])
	}
	c.segments[addr] = seg
}
