package mock

import (
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
	"sync"
)

// Network is a mock network implementation that is ideal for in-memory testing
// scenarios. It serves as a factory for transport.Stream and transport.Unary.
type Network[I, O transport.Message] struct {
	mu sync.Mutex
	// Entries is a slice of entries in the network. Entries currently only supports
	// unary entries.
	Entries      []NetworkEntry[I, O]
	UnaryRoutes  map[address.Address]*Unary[I, O]
	StreamRoutes map[address.Address]*Stream[I, O]
}

// NetworkEntry is a single entry in the network's history. NetworkEntry
type NetworkEntry[I, O transport.Message] struct {
	Host     address.Address
	Target   address.Address
	Request  I
	Response O
	Error    error
}

// RouteUnary returns a new transport.Unary hosted at the given address. This transport
// is not reachable by other hosts in the network until transport.Unary.Handle is called.
func (n *Network[I, O]) RouteUnary(host address.Address) *Unary[I, O] {
	pHost := n.parseTarget(host)
	t := &Unary[I, O]{Address: pHost, Network: n}
	n.UnaryRoutes[pHost] = t
	return t
}

const defaultStreamBuffer = 10

// RouteStream returns a new transport.Stream hosted at the given address.
// This transport is not reachable by other hosts in the network until
// transport.Stream.Handle is called.
func (n *Network[I, O]) RouteStream(host address.Address, buffer int) *Stream[I, O] {
	addr := n.parseTarget(host)
	if buffer <= 0 {
		buffer = defaultStreamBuffer
	}
	t := &Stream[I, O]{Address: addr, Network: n, BufferSize: buffer}
	n.StreamRoutes[addr] = t
	return t
}

func (n *Network[I, O]) parseTarget(target address.Address) address.Address {
	if target == "" {
		return address.Address(fmt.Sprintf("localhost:%v", len(n.UnaryRoutes)+len(n.StreamRoutes)))
	}
	return target
}

func (n *Network[I, O]) appendEntry(host, target address.Address, req I, res O, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Entries = append(n.Entries, NetworkEntry[I, O]{
		Host:     host,
		Target:   target,
		Request:  req,
		Response: res,
		Error:    err,
	})
}

// NewNetwork returns a new network that can exchange the provided message types.
func NewNetwork[I, O transport.Message]() *Network[I, O] {
	return &Network[I, O]{
		UnaryRoutes:  make(map[address.Address]*Unary[I, O]),
		StreamRoutes: make(map[address.Address]*Stream[I, O]),
	}
}
