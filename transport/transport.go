package transport

import (
	"context"
	"github.com/arya-analytics/x/address"
)

type Message interface{}

type Unary[I, O Message] interface {
	UnaryClient[I, O]
	UnaryServer[I, O]
}

type UnaryClient[I, O Message] interface {
	Send(context.Context, address.Address, I) (O, error)
}

type UnaryServer[I, O Message] interface {
	Handle(func(context.Context, I) (O, error))
}

type Stream[I, O Message] interface {
	StreamClient[I, O]
	StreamServer[I, O]
}

type StreamClient[I, O Message] interface {
	Stream(context.Context, address.Address, <-chan I) (<-chan O, error)
}

type StreamServer[I, O Message] interface {
	Handle(func(context.Context, address.Address, <-chan I) (<-chan O, error))
}
