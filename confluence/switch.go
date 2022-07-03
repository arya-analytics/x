package confluence

import (
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/signal"
)

// |||||| SWITCH ||||||

// Switch is a Segment that reads a value from an Inlet, resolves its address,
// and writes the value to the correct Outlet. Switch is a more efficient implementation
// of Map for cases when the addresses of input streams do not matter.
type Switch[V Value] struct {
	// Apply resolves the address of the input value. If ok is false, the
	// value is not written to any output stream. If error is non-nil, the
	// switch terminates and returns the error as fatal to the context.
	Apply func(ctx signal.Context, value V) (address.Address, bool, error)
	Out   map[address.Address]Inlet[V]
	AbstractMultiSink[V]
}

// OutTo implements the Segment interface.
func (r *Switch[V]) OutTo(inlets ...Inlet[V]) {
	if r.Out == nil {
		r.Out = make(map[address.Address]Inlet[V])
	}
	for _, inlet := range inlets {
		r.Out[inlet.InletAddress()] = inlet
	}
}

// Flow implements the Segment interface.
func (r *Switch[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRangeEach(ctx, r.In, func(v V) error {
		addr, ok, err := r.Apply(ctx, v)
		if err != nil || !ok {
			return err
		}
		r.send(addr, v)
		return nil
	}, opts...)
}

func (r *Switch[V]) send(addr address.Address, v V) {
	inlet, ok := r.Out[addr]
	if !ok {
		panic(fmt.Sprintf("[confluence.Apply] - address %s not found", addr))
	}
	inlet.Inlet() <- v
}

// |||||| BATCH ||||||

type BatchSwitch[V Value] struct {
	Apply func(ctx signal.Context, value V) (map[address.Address]V, error)
	Switch[V]
}

func (b *BatchSwitch[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	goRangeEach(ctx, b.Switch.In, func(v V) error {
		addrMap, err := b.Apply(ctx, v)
		if err != nil {
			return err
		}
		for addr, batch := range addrMap {
			b.send(addr, batch)
		}
		return nil
	}, opts...)
}

// |||||| MAP |||||||

// Map is a Segment that a values from a set of Inlet(s), resolves its address,
// and writes the value to the correct Outlet. Map is a less efficient implementation
// of Switch for cases when the addresses of Inlet(s) matter.
type Map[V Value] struct {
	// Apply resolves the address of the input value. If ok is false, the
	// value is not written to any output stream. If error is non-nil, the
	// switch terminates and returns the error as fatal to the context.
	Apply func(ctx signal.Context, addr address.Address, v Value) (address.Address, bool, error)
	In    map[address.Address]Outlet[V]
	Switch[V]
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
func (m *Map[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	for inAddr, outlet := range m.In {
		outlet = outlet
		signal.GoRange(ctx, outlet.Outlet(), func(v V) error {
			addr, ok, err := m.Apply(ctx, inAddr, v)
			if err != nil || !ok {
				return err
			}
			m.send(addr, v)
			return nil
		}, fo.Signal...)
	}
}
