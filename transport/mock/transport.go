package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	transport "github.com/arya-analytics/x/transport"
	"sync"
)

type Unary[
	REQ transport.Request,
	RES transport.Response] struct {
	Address address.Address
	Network *Network[REQ, RES]
	Handler func(context.Context, REQ) (RES, error)
}

func (t *Unary[REQ, RES]) Send(ctx context.Context, addr address.Address, req REQ) (RES, error) {
	return t.Network.Send(ctx, addr, req)
}

func (t *Unary[REQ, RES]) Handle(handler func(context.Context, REQ) (RES, error)) {
	t.Handler = handler
}

type Network[
	REQ transport.Request,
	RES transport.Response] struct {
	mu      sync.Mutex
	Entries []NetworkEntry[REQ, RES]
	routes  map[address.Address]*Unary[REQ, RES]
}

type NetworkEntry[REQ transport.Request, RES transport.Response] struct {
	Address address.Address
	REQ     transport.Request
	RES     transport.Response
	Error   error
}

func (n *Network[REQ, RES]) Route(addr address.Address) *Unary[REQ, RES] {
	if addr == "" {
		addr = address.Address(fmt.Sprintf("localhost:%v", len(n.routes)))
	}
	t := &Unary[REQ, RES]{Address: addr, Network: n}
	n.routes[addr] = t
	return t
}

func (n *Network[REQ, RES]) Send(ctx context.Context, addr address.Address, req REQ) (RES, error) {
	res, err := n.routes[addr].Handler(ctx, req)
	n.appendEntry(addr, req, res, err)
	return res, err
}

func (n *Network[REQ, RES]) appendEntry(addr address.Address, req REQ, res RES, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Entries = append(n.Entries, NetworkEntry[REQ, RES]{Address: addr, REQ: req, RES: res, Error: err})
}

func NewNetwork[REQ transport.Request, RES transport.Response]() *Network[REQ, RES] {
	return &Network[REQ, RES]{routes: make(map[address.Address]*Unary[REQ, RES])}
}
