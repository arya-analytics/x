package grpc

import (
	"context"
	"github.com/arya-analytics/x/address"
)

type UnaryService[I, O any] interface {
	Exec(ctx context.Context, req I) (res O, err error)
}

type UnaryClientTemplate[I, O any] interface {
	Send(ctx context.Context, conn *ClientConn, req I) (res O, err error)
}

type UnaryClient[I, O any] struct {
	pool     *Pool
	Template UnaryClientTemplate[I, O]
}

func (t *UnaryClient[I, O]) Send(ctx context.Context, addr address.Address, req I) (res O, err error) {
	conn, err := t.pool.Acquire(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Release()
	return t.Template.Send(ctx, conn, req)
}

type UnaryServer[I, O any] struct {
	handle func(ctx context.Context, req I) (res O, err error)
}

func (s *UnaryServer[I, O]) Handle(handle func(ctx context.Context, req I) (res O, err error)) {
	s.handle = handle
}
