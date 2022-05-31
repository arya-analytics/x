package grpc

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
)

// |||||| CORE ||||||

type AbstractTransport[I, O transport.Message] interface {
	Send(ctx context.Context, c *ClientConn, req I) (res O, err error)
}

type Transport[I, O transport.Message] struct {
	AbstractTransport[I, O]
	pool   *Pool
	handle func(context.Context, I) (O, error)
}

func (t *Transport[I, O]) Send(ctx context.Context, addr address.Address, req I) (res O, err error) {
	c, err := t.pool.Acquire(addr)
	if err != nil {
		return res, err
	}
	defer c.Release()
	return t.AbstractTransport.Send(c)(ctx, req)
}

func (t *Transport[I, O]) Handle(handle func(context.Context, I) (O, error)) { t.handle = handle }

// |||||| TRANSLATED ||||||

type AbstractTranslatedTransport[I, O transport.Message, TI, TO any] interface {
	ReqTranslator() Translator[I, TI]
	ResTranslator() Translator[O, TO]
	AbstractTransport[I, O]
}

type Translator[O any, I transport.Message] interface {
	Forward(I) O
	Backward(O) I
}

type TranslatedTransport[I, O transport.Message, TI, TO any] struct {
	AbstractTranslatedTransport[I, O, TI, TO]
	Transport[I, O]
}

func (t *TranslatedTransport[I, O, TI, TO]) Send(ctx context.Context, addr address.Address, req TI) (TO, error) {
	res, err := t.Transport.Send(ctx, addr, t.ReqTranslator().Backward(req))
	return t.ResTranslator().Forward(res), err
}

func (t *TranslatedTransport[I, O, TI, TO]) Handle(handle func(context.Context, TI) (TO, error)) {
	t.Transport.Handle(func(ctx context.Context, req I) (O, error) {
		res, err := handle(ctx, t.ReqTranslator().Forward(req))
		return t.ResTranslator().Backward(res), err
	})
}
