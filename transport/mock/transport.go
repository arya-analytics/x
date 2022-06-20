package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
	"sync"
)

type Unary[
	I transport.Message,
	O transport.Message] struct {
	Address address.Address
	Network *Network[I, O]
	Handler func(context.Context, I) (O, error)
}

func (t *Unary[I, O]) Send(ctx context.Context, addr address.Address, req I) (O, error) {
	return t.Network.Send(ctx, addr, req)
}

func (t *Unary[I, O]) Handle(handler func(context.Context, I) (O, error)) {
	t.Handler = handler
}

func (t *Unary[I, O]) String() string { return fmt.Sprintf("mock.Unary{} at %s", t.Address) }

type Stream[I, O transport.Message] struct {
	Network Network[I, O]
	Address address.Address
	Handler func(context.Context, <-chan I) (<-chan O, error)
}

func (s *Stream[I, O]) Stream(ctx context.Context, addr address.Address, req <-chan I) (<-chan O, error) {
	return s.Network.Stream(ctx, addr, req)
}

func (s *Stream[I, O]) Handle(handler func(context.Context, <-chan I) (<-chan O, error)) {
	s.Handler = handler
}

type Network[I, O transport.Message] struct {
	mu           sync.Mutex
	Entries      []NetworkEntry[I, O]
	UnaryRoutes  map[address.Address]*Unary[I, O]
	StreamRoutes map[address.Address]*Stream[I, O]
}

type NetworkEntry[I, O transport.Message] struct {
	Address  address.Address
	Request  I
	Response O
	Error    error
}

func (n *Network[I, O]) Route(addr address.Address) *Unary[I, O] {
	if addr == "" {
		addr = address.Address(fmt.Sprintf("localhost:%v", len(n.UnaryRoutes)))
	}
	t := &Unary[I, O]{Address: addr, Network: n}
	n.UnaryRoutes[addr] = t
	return t
}

func (n *Network[I, O]) Send(ctx context.Context, addr address.Address, req I) (res O, err error) {
	route, ok := n.UnaryRoutes[addr]
	if !ok {
		return res, fmt.Errorf("no route to %v", addr)
	}
	if route.Handler == nil {
		return res, fmt.Errorf("no handler for %v", addr)
	}
	res, err = route.Handler(ctx, req)
	n.appendEntry(addr, req, res, err)
	return res, err
}

func (n *Network[I, O]) Stream(ctx context.Context, addr address.Address, req <-chan I) (<-chan O, error) {
	route, ok := n.StreamRoutes[addr]
	if !ok {
		return nil, fmt.Errorf("no route to %v", addr)
	}
	if route.Handler == nil {
		return nil, fmt.Errorf("no handler for %v", addr)
	}
	res, err := route.Handler(ctx, req)
	n.appendEntry(addr, req, res, err)
	return res, err
}

func (n *Network[I, O]) appendEntry(addr address.Address, req I, res O, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Entries = append(n.Entries, NetworkEntry[I, O]{Address: addr, Request: req, Response: res, Error: err})
}

func NewNetwork[I, O transport.Message]() *Network[I, O] {
	return &Network[I, O]{UnaryRoutes: make(map[address.Address]*Unary[I, O])}
}
