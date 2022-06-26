package confluence

import (
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/signal"
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
	Flow(ctx signal.Context)
}

// Segment is an interface that accepts values from an Inlet Stream, does some operation on them, and returns the
// values through an Outlet Stream.
type Segment[V Value] interface {
	Source[V]
	Sink[V]
}

// |||||| SWITCH ||||||

// Switch is a segment that reads values from a set of input streams, resolves their address,
// and sends them to the appropriate output streams. Switch is a more efficient implementation of
// Map for when the addresses of input streams are not important.
type Switch[V Value] struct {
	// Switch is a function that resolves the address of a Value.
	Switch func(ctx signal.Context, value V) (address.Address, error)
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
func (r *Switch[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, r.inFrom, func(v V) error {
		addr, err := r.Switch(ctx, v)
		if err != nil || addr == "" {
			return err
		}
		inlet, ok := r.outTo[addr]
		if !ok {
			panic(fmt.Sprintf("address %s not found", addr))
		}
		inlet.Inlet() <- v
		return nil
	})
}

func goRangeEach[V Value](ctx signal.Context, outlets []Outlet[V], f func(v V) error) {
	for _, outlet := range outlets {
		outlet = outlet
		signal.GoRange(ctx, outlet.Outlet(), func(v V) error { return f(v) })
	}
}

type BatchSwitch[V Value] struct {
	Switch func(ctx signal.Context, value V) (map[address.Address]V, error)
	sw     Switch[V]
}

func (b *BatchSwitch[V]) InFrom(outlets ...Outlet[V]) { b.sw.InFrom(outlets...) }

func (b *BatchSwitch[V]) OutTo(inlets ...Inlet[V]) { b.sw.OutTo(inlets...) }

func (b *BatchSwitch[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, b.sw.inFrom, func(v V) error {
		addrMap, err := b.Switch(ctx, v)
		if err != nil {
			return err
		}
		for addr, batch := range addrMap {
			inlet, ok := b.sw.outTo[addr]
			if !ok {
				panic("[confluence.BatchSwitch] - address not found")
			}
			inlet.Inlet() <- batch
		}
		return nil
	})
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
func (d *Confluence[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, d.In, func(v V) error {
		for _, inlet := range d.Out {
			inlet.Inlet() <- v
		}
		return nil
	})
}

// Delta is similar to Confluence. It reads values from a set of input streams and pipes them to a set of output streams.
// Only one output stream receives a copy of each value from the input stream. This value is taken by the first
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
func (d *Delta[V]) Flow(ctx signal.Context) {
	goRangeEach(ctx, d.In, func(v V) error {
	o:
		for _, inlet := range d.Out {
			select {
			case inlet.Inlet() <- v:
				break o
			default:
			}
		}
		return nil
	})
}

// |||||| TRANSFORM ||||||

// Transform is a segment that reads values from an input streamImpl, executes a transformation on them,
// and sends them to an out Stream.
type Transform[V Value] struct {
	Transform func(ctx signal.Context, value V) (V, bool, error)
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Transform[V]) Flow(ctx signal.Context) {
	signal.GoRange(ctx, f.In.Outlet(), func(v V) error {
		v, ok, err := f.Transform(ctx, v)
		if !ok || err != nil {
			return err
		}
		f.Linear.Out.Inlet() <- v
		return nil
	})
}

// |||||| FILTER ||||||

// Filter is a segment that reads values from an input Stream, filters them through a function, and
// optionally discards them to an output Stream.
type Filter[V Value] struct {
	Filter  func(ctx signal.Context, value V) (bool, error)
	Rejects Inlet[V]
	Linear[V]
}

// Flow implements the Segment interface.
func (f *Filter[V]) Flow(ctx signal.Context) {
	signal.GoRange(ctx, f.In.Outlet(), func(v V) error {
		ok, err := f.Filter(ctx, v)
		if err != nil {
			return err
		}
		if ok {
			f.Linear.Out.Inlet() <- v
		} else if f.Rejects != nil {
			f.Rejects.Inlet() <- v
		}
		return nil
	})
}

// |||||| LINEAR ||||||

// Linear is a segment that reads values from a single input stream and pipes them
// to a single output stream. Linear is an abstract segment, meaning it should not be
// instantiated directly, and should be embedded in other segments. Linear will
// panic if Flow is called.
type Linear[V Value] struct {
	In  Outlet[V]
	Out Inlet[V]
}

// InFrom implements the Segment interface.
func (l *Linear[V]) InFrom(outlets ...Outlet[V]) {
	if len(outlets) == 0 {
		panic("[confluence.Linear] must have exactly one inlet")
	}
	l.In = outlets[0]
}

// OutTo implements the Segment interface.
func (l *Linear[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) == 0 {
		panic("[confluence.Linear] - must have exactly one outlet")
	}
	l.Out = inlets[0]
}

// Flow implements the Segment interface.
func (l *Linear[V]) Flow(ctx signal.Context) { panic("[confluence.Linear] - abstract segment") }

// |||||| MAP ||||||

// Map is a segment that reads values from an addresses input streamImpl, maps them to an output address, and
// sends them. Map is a relatively inefficient Segment, use Switch when possible.
type Map[V Value] struct {
	Map func(ctx signal.Context, addr address.Address, v Value) (address.Address, error)
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
func (m *Map[V]) Flow(ctx signal.Context) {
	for inAddr, outlet := range m.In {
		outlet = outlet
		signal.GoRange(ctx, outlet.Outlet(), func(v V) error {
			addr, err := m.Map(ctx, inAddr, v)
			if err != nil {
				return err
			}
			m.Out[addr].Inlet() <- v
			return nil
		})
	}
}

// |||||| EMITTER ||||||

type Emitter[V Value] struct {
	Emit     func(ctx signal.Context) (V, error)
	Store    func(ctx signal.Context, value V) error
	Interval time.Duration
	Linear[V]
}

func (e *Emitter[V]) Flow(ctx signal.Context) {
	signal.GoRange(ctx, e.In.Outlet(), func(batch V) error { return e.Store(ctx, batch) })
	signal.GoTick(ctx, e.Interval, func(t time.Time) error {
		v, err := e.Emit(ctx)
		if err != nil {
			return err
		}
		e.Out.Inlet() <- v
		return nil
	})
}

func panicAbstract() { panic("[confluence]- abstract segment") }
