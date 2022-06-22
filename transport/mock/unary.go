package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
)

// Unary is a mock, synchronous implementation of the transport.Unary interface.
type Unary[I, O transport.Message] struct {
	Address address.Address
	Network *Network[I, O]
	Handler func(context.Context, I) (O, error)
}

// Send implements the transport.Unary interface.
func (t *Unary[I, O]) Send(ctx context.Context, target address.Address, req I) (res O,
	err error) {
	route, ok := t.Network.UnaryRoutes[target]
	if !ok {
		return res, fmt.Errorf("no route to %v", target)
	}
	if route.Handler == nil {
		return res, fmt.Errorf("no handler for %v", target)
	}
	res, err = route.Handler(ctx, req)
	t.Network.appendEntry(t.Address, target, req, res, err)
	return res, err
}

// Handle implements the transport.Unary interface.
func (t *Unary[I, O]) Handle(handler func(context.Context, I) (O, error)) { t.Handler = handler }

// String implements the transport.Unary interface.
func (t *Unary[I, O]) String() string { return fmt.Sprintf("mock.RouteUnary{} at %s", t.Address) }
