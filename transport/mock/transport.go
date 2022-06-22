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
// is not reachable by other hosts in the network until transport.Unary.
// Handle is called.
func (n *Network[I, O]) RouteUnary(host address.Address) *Unary[I, O] {
	t := &Unary[I, O]{Address: n.parseTarget(host), Network: n}
	n.UnaryRoutes[host] = t
	return t
}

// RouteStream returns a new transport.Stream hosted at the given address.
// This transport is not reachable by other hosts in the network until transport.Stream.
// Handle is called.
func (n *Network[I, O]) RouteStream(host address.Address) *Stream[I, O] {
	t := &Stream[I, O]{Address: n.parseTarget(host), Network: n}
	n.StreamRoutes[host] = t
	return t
}

func (n *Network[I, O]) parseTarget(target address.Address) address.Address {
	if target == "" {
		return address.Address(fmt.Sprintf("localhost:%v", len(n.UnaryRoutes)))
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

func NewNetwork[I, O transport.Message]() *Network[I, O] {
	return &Network[I, O]{UnaryRoutes: make(map[address.Address]*Unary[I, O])}
}
