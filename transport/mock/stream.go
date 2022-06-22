package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
)

// Stream is a mock implementation of the transport.Stream interface.
type Stream[I, O transport.Message] struct {
	Address address.Address
	Network *Network[I, O]
	Handler func(ctx context.Context, srv transport.StreamServer[I, O]) error
}

// Stream implements the transport.Stream interface.
func (s *Stream[I, O]) Stream(
	ctx context.Context,
	target address.Address,
) (transport.StreamClient[I, O], error) {
	route, ok := s.Network.StreamRoutes[target]
	if !ok || route.Handler == nil {
		return nil, transport.NewTargetNotFound(target)
	}
	req, res := make(chan I), make(chan O)
	reqErrC, resErrC := make(chan error), make(chan error)
	server := &StreamServer[I, O]{
		requests:   req,
		responses:  res,
		clientErrC: reqErrC,
		serverErrC: resErrC,
	}
	go func() {
		if err := route.Handler(ctx, server); err != nil {
			resErrC <- err
			// Panicking here because we don't want to add a dependency on a logging
			// package. This error also shouldn't occur during mock operation.
			if err := server.CloseSend(); err != nil {
				panic(errors.Wrap(err, "failed to close send"))
			}
		}
	}()
	return &StreamClient[I, O]{
		requests:   req,
		responses:  res,
		clientErrC: reqErrC,
		serverErrC: resErrC,
	}, nil
}

func (s *Stream[I, O]) String() string {
	return fmt.Sprintf("mock.Stream{} at %s", s.Address)
}

// Handle implements the transport.Stream interface.
func (s *Stream[I, O]) Handle(handler func(
	ctx context.Context,
	srv transport.StreamServer[I, O]) error) {
	s.Handler = handler
}

// StreamClient is a mock implementation of the transport.StreamClient interface.
type StreamClient[I, O transport.Message] struct {
	requests   chan I
	responses  chan O
	serverErrC <-chan error
	clientErrC chan<- error
}

// Send implements the transport.StreamClient interface.
func (s *StreamClient[I, O]) Send(req I) error { s.requests <- req; return nil }

// Receive implements the transport.StreamClient interface.
func (s *StreamClient[I, O]) Receive() (resp O, err error) {
	select {
	case resp = <-s.responses:
		return resp, nil
	case err = <-s.serverErrC:
		return resp, err
	}
}

// CloseSend implements the transport.StreamClient interface.
func (s *StreamClient[I, O]) CloseSend() error {
	s.clientErrC <- transport.EOF
	close(s.requests)
	return nil
}

// StreamServer implements the transport.StreamServer interface.
type StreamServer[I, O transport.Message] struct {
	requests   chan I
	responses  chan O
	serverErrC chan<- error
	clientErrC <-chan error
}

// Receive implements the transport.StreamServer interface.
func (s *StreamServer[I, O]) Receive() (req I, err error) {
	select {
	case req = <-s.requests:
		return req, nil
	case err = <-s.clientErrC:
		return req, err
	}
}

// Send implements the transport.StreamServer interface.
func (s *StreamServer[I, O]) Send(res O) error { s.responses <- res; return nil }

// CloseSend implements the transport.StreamServer interface.
func (s *StreamServer[I, O]) CloseSend() error {
	s.serverErrC <- transport.EOF
	close(s.responses)
	return nil
}
