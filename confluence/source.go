package confluence

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/signal"
)

// AbstractMultiSource is a basic implementation of a Source that can send values to
// multiple Outlet(s). It implements an empty Flow method, as sources are typically
// driven by external events. The user can define a custom Flow method if they wish to
// drive the source themselves.
type AbstractMultiSource[V Value] struct {
	Out []Inlet[V]
}

// OutTo implements the Source interface.
func (ams *AbstractMultiSource[V]) OutTo(inlets ...Inlet[V]) { ams.Out = append(ams.Out, inlets...) }

// SendToEach sends the provided value to each Inlet in the Source.
func (ams *AbstractMultiSource[V]) SendToEach(ctx signal.Context, v V) error {
	for _, inlet := range ams.Out {
		inlet.Inlet() <- v
	}
	return nil
}

// SendToAvailable sends the provided value to the first Inlet in the Source that
// can accept it. NOTE: This is implementation is quite crude and probably buggy.
// be careful when using.
func (ams *AbstractMultiSource[V]) SendToAvailable(ctx signal.Context, v V) error {
	for {
		for _, inlet := range ams.Out {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case inlet.Inlet() <- v:
				return nil
			default:
			}
		}
	}
}

// CloseInlets implements the InletCloser interface.
func (ams *AbstractMultiSource[V]) CloseInlets() {
	for _, inlet := range ams.Out {
		inlet.Close()
	}
}

// AbstractUnarySource is a basic implementation of a Source that sends values to a
// single Outlet. The user can define a custom Flow method if they wish to
// drive the source themselves.
type AbstractUnarySource[V Value] struct {
	Out Inlet[V]
}

// OutTo implements the Source interface.
func (aus *AbstractUnarySource[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) != 1 {
		panic("[confluence.AbstractUnarySource] -  must have exactly one outlet")
	}
	aus.Out = inlets[0]
}

// CloseInlets implements the InletCloser interface.
func (aus *AbstractUnarySource[V]) CloseInlets() { aus.Out.Close() }

// AbstractAddressableSource is an implementation of a Source that stores its Inlet(s) in an
// addressable map. This is ideal for use cases where the address of an Inlet is
// relevant to the routing of the value (such as a Switch).
type AbstractAddressableSource[V Value] struct {
	// Out is an address map of all Inlet(s) reachable by the Source.
	Out map[address.Address]Inlet[V]
}

// OutTo implements the Source interface. Inlets provided must have a valid Inlet.
// InletAddress. If two inlets are provided with the same address, the last Inlet
// will override the previous one.
func (aas *AbstractAddressableSource[V]) OutTo(inlets ...Inlet[V]) {
	if aas.Out == nil {
		aas.Out = make(map[address.Address]Inlet[V])
	}
	for _, inlet := range inlets {
		aas.Out[inlet.InletAddress()] = inlet
	}
}

// Send sends a value to the target address. Returns add
func (aas *AbstractAddressableSource[V]) Send(target address.Address, v V) error {
	inlet, ok := aas.Out[target]
	if !ok {
		return address.TargetNotFound(target)
	}
	inlet.Inlet() <- v
	return nil
}

// CloseInlets closes all Inlet(s) provided to AbstractAddressableSource.OutTo.
func (aas *AbstractAddressableSource[V]) CloseInlets() {
	for _, inlet := range aas.Out {
		inlet.Close()
	}
}
