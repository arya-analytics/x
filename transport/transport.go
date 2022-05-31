package transport

import (
	"context"
	"github.com/arya-analytics/x/address"
)

type Message interface{}

type Unary[REQ, RES Message] interface {
	Send(context.Context, address.Address, REQ) (RES, error)
	Handle(func(context.Context, REQ) (RES, error))
}
