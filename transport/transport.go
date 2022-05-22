package transport

import (
	"context"
	"github.com/arya-analytics/x/address"
)

type Request interface{}

type Response interface{}

type Unary[REQ Request, RES Response] interface {
	Send(context.Context, address.Address, REQ) (RES, error)
	Handle(func(context.Context, REQ) (RES, error))
}
