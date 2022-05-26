package confluence

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
)

// |||||| VALUE ||||||

// Value represents an item that can be sent through a Stream.
type Value any

// |||||| STREAMS ||||||

// Inlet is a Stream that accepts values and can be addressed.
type Inlet[V Value] interface {
	// Inlet pipes a value through the streamImpl.
	Inlet() chan<- V
	// InletAddress returns the address of the inlet.
	InletAddress() address.Address
	// SetInletAddress sets the address of the inlet.
	SetInletAddress(address.Address)
}

// Outlet is a Stream that emits values and can be addressed.
type Outlet[V Value] interface {
	// Outlet receives a value from the streamImpl.
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

// Switch is a segment that reads values from a set of input streams, resolves their address,
// and sends them to the appropriate output streams. Switch is a more efficient implementation of
// Map for when the addresses of input streams are not important.
type Switch[V Value] struct {
	// Route is a function that resolves the address of a Value.
	Route  func(v V) address.Address
	inFrom []Outlet[V]
	outTo  map[address.Address]Inlet[V]
}

// InFrom implements the Segment interface.
func (r *Switch[V]) InFrom(outlets ...Outlet[V]) {
	r.inFrom = append(r.inFrom, outlets...)
}

// OutTo implements the Segment interface.
func (r *Switch[V]) OutTo(inlets ...Inlet[V]) {
	if r.outTo == nil {
		r.outTo = make(map[address.Address]Inlet[V])
	}
	for _, inlet := range inlets {
		r.outTo[inlet.InletAddress()] = inlet
	}
}

// Flow implements the Segment interface.
func (r *Switch[V]) Flow(ctx Context) {
	for _, outlet := range r.inFrom {
		outlet = outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
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
// Every output streamImpl receives a copy of the value.
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
		outlet = outlet
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
// Only one output streamImpl receives a copy of each value from input streamImpl. This value is taken by the first
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
		outlet = outlet
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

// Transform is a segment that reads values from an input streamImpl, executes a transformation on them,
// and sends them to an out Stream.
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

// Filter is a segment that reads values from an input Stream, filters them through a function, and
// optionally discards them to an output Stream.
type Filter[V Value] struct {
	Filter  func(V) bool
	Rejects Inlet[V]
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Filter[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
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
}

// |||||| LINEAR ||||||

// Linear is a segment that reads values from a single input streamImpl and pipes them to a single output streamImpl.
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

// Map is a segment that reads values from an addresses input streamImpl, maps them to an output address, and
// sends them. Map is a relatively inefficient Segment, use Switch when possible.
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
