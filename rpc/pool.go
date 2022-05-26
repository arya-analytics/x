package rpc

import (
	"github.com/arya-analytics/x/address"
	"google.golang.org/grpc"
)

type (
	ClientConn = grpc.ClientConn
	DialOption = grpc.DialOption
)

type Pool interface {
	Acquire(addr address.Address, opts ...DialOption) (ClientConn, error)
	Release()
}

func main() {
	conn, err := grpc.Dial()
	conn.GetState()
	conn.
}
