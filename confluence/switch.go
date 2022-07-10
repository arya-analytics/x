package confluence

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/signal"
)

type SwitchFunc[V Value] struct {
	// ApplySwitch resolves the address of the input value. If ok is false, the
	// value is not written to any output stream. If error is non-nil, the
	// switch terminates and returns the error as fatal to the context.
	ApplySwitch func(signal.Context, V) (address.Address, bool, error)
}

// Switch is a Segment that reads a value from Inlet(s), resolves its address,
// and writes the value to the matching Outlet.
type Switch[V Value] struct {
	SwitchFunc[V]
	UnarySink[V]
	AbstractAddressableSource[V]
}

// Flow implements the Flow interface, reading values from the Outlet, resolving their
// address, and sending them to the correct Inlet. If an address cannot be found,
// the switch will exit with an address.TargetNotFound error.
func (sw *Switch[V]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(sw)
	sw.GoRange(ctx, sw._switch, fo.Signal...)
}

func (sw *Switch[V]) _switch(ctx signal.Context, v V) error {
	target, ok, err := sw.SwitchFunc.ApplySwitch(ctx, v)
	if !ok || err != nil {
		return err
	}
	return sw.Send(target, v)
}

type BatchSwitchFunc[I, O Value] struct {
	// ApplySwitch resolves the address of the input value. The caller should bind
	// output addresses and values to the provided out map. If error is non-nil,
	// the switch terminates and returns the error as fatal to the context.
	ApplySwitch func(ctx signal.Context, batch I, out map[address.Address]O) error
}

// BatchSwitch is a Segment that reads a batch of values from an inlet,
// resolves the addresses of its values into a map, and then sends them to their resolved
// addresses. BatchSwitch should be used in cases where certain parts of a value may
// need to be routed to different locations.
type BatchSwitch[I, O Value] struct {
	BatchSwitchFunc[I, O]
	UnarySink[I]
	AbstractAddressableSource[O]
	addrMap map[address.Address]O
}

// Flow implements the Flow interface, reading batches from the Outlet, resolving
// their address, and sending them to the correct Inlet. If an address cannot be
// found, the BatchSwitch will exit with an address.NotFound error.
func (bsw *BatchSwitch[I, O]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(bsw)
	bsw.addrMap = make(map[address.Address]O)
	bsw.GoRange(ctx, bsw._switch, fo.Signal...)
}

func (bsw *BatchSwitch[I, O]) _switch(
	ctx signal.Context,
	v I,
) error {
	if err := bsw.BatchSwitchFunc.ApplySwitch(ctx, v, bsw.addrMap); err != nil {
		return err
	}
	for target, batch := range bsw.addrMap {
		if err := bsw.Send(target, batch); err != nil {
			return err
		}
		delete(bsw.addrMap, target)
	}
	return nil
}
