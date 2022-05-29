package confluence

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/shutdown"
	"time"
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
	// Switch is a function that resolves the address of a Value.
	Switch func(ctx Context, value V) address.Address
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
					addr := r.Switch(ctx, v)
					if addr == "" {
						continue
					}
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

type BatchSwitch[V Value] struct {
	Switch func(ctx Context, value V) map[address.Address]V
	sw     Switch[V]
}

func (b *BatchSwitch[V]) InFrom(outlets ...Outlet[V]) { b.sw.InFrom(outlets...) }

func (b *BatchSwitch[V]) OutTo(inlets ...Inlet[V]) { b.sw.OutTo(inlets...) }

func (b *BatchSwitch[V]) Flow(ctx Context) {
	for _, outlet := range b.sw.inFrom {
		outlet = outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					addrMap := b.Switch(ctx, v)
					for addr, batch := range addrMap {
						inlet, ok := b.sw.outTo[addr]
						if !ok {
							panic("address not found")
						}
						inlet.Inlet() <- batch
					}
				}
			}
		})
	}
}

// |||||| CONFLUENCE ||||||

// Confluence is a segment that reads values from a set of input streams and pipes them to multiple output streams.
// Every output streamImpl receives a copy of the value.
type Confluence[V Value] struct {
	In  []Outlet[V]
	Out []Inlet[V]
}

// InFrom implements the Segment interface.
func (d *Confluence[V]) InFrom(outlets ...Outlet[V]) {
	d.In = append(d.In, outlets...)
}

// OutTo implements the Segment interface.
func (d *Confluence[V]) OutTo(inlets ...Inlet[V]) {
	d.Out = append(d.Out, inlets...)
}

// Flow implements the Segment interface.
func (d *Confluence[V]) Flow(ctx Context) {
	for _, outlet := range d.In {
		outlet = outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					for _, inlet := range d.Out {
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
	In  []Outlet[V]
	Out []Inlet[V]
}

// InFrom implements the Segment interface.
func (d *Delta[V]) InFrom(outlets ...Outlet[V]) {
	d.In = append(d.In, outlets...)
}

// OutTo implements the Segment interface.
func (d *Delta[V]) OutTo(inlets ...Inlet[V]) {
	d.Out = append(d.Out, inlets...)
}

// Flow implements the Segment interface.
func (d *Delta[V]) Flow(ctx Context) {
	for _, outlet := range d.In {
		outlet = outlet
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
				o:
					for _, inlet := range d.Out {
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
	Transform func(ctx Context, value V) (V, bool)
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Transform[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case v := <-f.In.Outlet():
				if tv, ok := f.Transform(ctx, v); ok {
					f.Out.Inlet() <- tv
				}
			}
		}
	})
}

// |||||| FILTER ||||||

// Filter is a segment that reads values from an input Stream, filters them through a function, and
// optionally discards them to an output Stream.
type Filter[V Value] struct {
	Filter  func(ctx Context, value V) bool
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
			case v := <-f.In.Outlet():
				if f.Filter(ctx, v) {
					f.Out.Inlet() <- v
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
	In  Outlet[V]
	Out Inlet[V]
}

// InFrom implements the Segment interface.
func (l *Linear[V]) InFrom(outlets ...Outlet[V]) {
	if len(outlets) != 1 {
		panic("linear In must have exactly one outlets")
	}
	l.In = outlets[0]
}

// OutTo implements the Segment interface.
func (l *Linear[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) != 1 {
		panic("linear In must have exactly one outlets")
	}
	l.Out = inlets[0]
}

// Flow implements the Segment interface.
func (l *Linear[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case l.Out.Inlet() <- <-l.In.Outlet():
			}
		}
	})
}

// |||||| MAP ||||||

// Map is a segment that reads values from an addresses input streamImpl, maps them to an output address, and
// sends them. Map is a relatively inefficient Segment, use Switch when possible.
type Map[V Value] struct {
	Map func(ctx Context, addr address.Address, v Value) address.Address
	In  map[address.Address]Outlet[V]
	Out map[address.Address]Inlet[V]
}

// InFrom implements the Segment interface.
func (m *Map[V]) InFrom(outlet ...Outlet[V]) {
	if m.In == nil {
		m.In = make(map[address.Address]Outlet[V])
	}
	for _, o := range outlet {
		m.In[o.OutletAddress()] = o
	}
}

// OutTo implements the Segment interface.
func (m *Map[V]) OutTo(inlet ...Inlet[V]) {
	if m.Out == nil {
		m.Out = make(map[address.Address]Inlet[V])
	}
	for _, i := range inlet {
		m.Out[i.InletAddress()] = i
	}
}

// Flow implements the Segment interface.
func (m *Map[V]) Flow(ctx Context) {
	for inAddr, outlet := range m.In {
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for {
				select {
				case <-sig:
					return nil
				case v := <-outlet.Outlet():
					m.Out[m.Map(ctx, inAddr, v)].Inlet() <- v
				}
			}
		})
	}
}

// |||||| EMITTER ||||||

type Emitter[V Value] struct {
	Emit     func(ctx Context) V
	Store    func(ctx Context, value V)
	Interval time.Duration
	Linear[V]
}

func (e *Emitter[V]) Flow(ctx Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for batch := range e.In.Outlet() {
			e.Store(ctx, batch)
		}
		return nil
	})
	ctx.Shutdown.GoTick(e.Interval, func() error { e.Out.Inlet() <- e.Emit(ctx); return nil })
}
