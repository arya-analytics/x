package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
	"io"
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
	addr address.Address,
) (*StreamClient[I, O], error) {
	route, ok := s.Network.StreamRoutes[addr]
	if !ok || route.Handler == nil {
		return nil, errors.Wrap(
			transport.TargetNotFound,
			fmt.Sprintf("no route to target %s", addr),
		)
	}
	req, res, errC := make(chan I), make(chan O), make(chan error)
	server := &StreamServer[I, O]{Requests: req, Responses: res, ErrC: errC}
	go func() {
		if err := route.Handler(ctx, server); err != nil {
			errC <- err
		}
	}()
	return &StreamClient[I, O]{Requests: req, Responses: res, ErrC: errC}, nil
}

// Handle implements the transport.Stream interface.
func (s *Stream[I, O]) Handle(handler func(ctx context.Context, srv transport.StreamServer[I, O]) error) {
	s.Handler = handler
}

// StreamClient is a mock implementation of the transport.StreamClient interface.
type StreamClient[I, O transport.Message] struct {
	Requests  chan I
	Responses chan O
	ErrC      <-chan error
}

// Send implements the transport.StreamClient interface.
func (s *StreamClient[I, O]) Send(req I) error {
	s.Requests <- req
	select {
	case err := <-s.ErrC:
		return err
	default:
		return nil
	}
}

// Receive implements the transport.StreamClient interface.
func (s *StreamClient[I, O]) Receive() (O, error) {
	select {
	case resp := <-s.Responses:
		return resp, nil
	case err := <-s.ErrC:
		return nil, err
	}
}

// CloseSend implements the transport.StreamClient interface.
func (s *StreamClient[I, O]) CloseSend() error {
	s.Requests <- io.EOF
	close(s.Requests)
	return nil
}

// StreamServer implements the transport.StreamServer interface.
type StreamServer[I, O transport.Message] struct {
	Requests  chan I
	Responses chan O
	ErrC      chan<- error
}

// Receive implements the transport.StreamServer interface.
func (s *StreamServer[I, O]) Receive() (I, error) { return <-s.Requests, nil }

// Send implements the transport.StreamServer interface.
func (s *StreamServer[I, O]) Send(res O) error { s.Responses <- res; return nil }

// CloseSend implements the transport.StreamServer interface.
func (s *StreamServer[I, O]) CloseSend() error {
	s.ErrC <- transport.EOF
	close(s.Responses)
	return nil
}
