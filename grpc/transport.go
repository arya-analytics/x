package grpc

import (
	"context"
	"github.com/arya-analytics/x/address"
)

type UnaryClient[I, O any] struct {
	Pool   *Pool
	Sender func(ctx context.Context, conn *ClientConn, req I) (res O, err error)
}

func (t *UnaryClient[I, O]) Send(ctx context.Context, addr address.Address, req I) (res O, err error) {
	conn, err := t.Pool.Acquire(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	return t.Sender(ctx, conn, req)
}

type UnaryServer[I, O any] struct {
	handle func(ctx context.Context, req I) (res O, err error)
}

func (s *UnaryServer[I, O]) Handle(handle func(ctx context.Context, req I) (res O, err error)) {
	s.handle = handle
}
