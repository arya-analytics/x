package confluence

import (
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/signal"
)

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
func (r *Switch[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
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
	}, fo.Signal...)
}

// |||||| BATCH ||||||

type BatchSwitch[V Value] struct {
	Switch func(ctx signal.Context, value V) (map[address.Address]V, error)
	Core   Switch[V]
}

func (b *BatchSwitch[V]) InFrom(outlets ...Outlet[V]) { b.Core.InFrom(outlets...) }

func (b *BatchSwitch[V]) OutTo(inlets ...Inlet[V]) { b.Core.OutTo(inlets...) }

func (b *BatchSwitch[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	goRangeEach(ctx, b.Core.inFrom, func(v V) error {
		addrMap, err := b.Switch(ctx, v)
		if err != nil {
			return err
		}
		for addr, batch := range addrMap {
			inlet, ok := b.Core.outTo[addr]
			if !ok {
				panic("[confluence.BatchSwitch] - address not found")
			}
			inlet.Inlet() <- batch
		}
		return nil
	}, fo.Signal...)
}

// |||||| MAP |||||||

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
func (m *Map[V]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	for inAddr, outlet := range m.In {
		outlet = outlet
		signal.GoRange(ctx, outlet.Outlet(), func(v V) error {
			addr, err := m.Map(ctx, inAddr, v)
			if err != nil {
				return err
			}
			m.Out[addr].Inlet() <- v
			return nil
		}, fo.Signal...)
	}
}
