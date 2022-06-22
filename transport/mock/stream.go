package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
	"io"
)

type Stream[I, O transport.Message] struct {
	Address address.Address
	Network *Network[I, O]
	Handler func(ctx context.Context, srv transport.StreamServer[I, O]) error
}

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
	requests, responses, errC := make(chan I), make(chan O), make(chan error)
	server := &StreamServer[I, O]{
		Requests:  requests,
		Responses: responses,
		ErrC:      errC,
	}
	go func() {
		if err := route.Handler(ctx, server); err != nil {
			errC <- err
		}
	}()
	return &StreamClient[I, O]{
		Requests:  requests,
		Responses: responses,
		ErrC:      errC,
	}, nil
}

type StreamClient[I, O transport.Message] struct {
	Requests  chan I
	Responses chan O
	ErrC      chan error
}

func (s *StreamClient[I, O]) Send(req I) error { s.Requests <- req; return nil }

func (s *StreamClient[I, O]) Receive() (O, error) {
	select {
	case resp := <-s.Responses:
		return resp, nil
	case err := <-s.ErrC:
		return nil, err
	}
}

func (s *StreamClient[I, O]) CloseSend() error {
	s.Requests <- io.EOF
	close(s.Requests)
	return nil
}

type StreamServer[I, O transport.Message] struct {
	Requests  chan I
	Responses chan O
	ErrC      chan error
}

func (s *StreamServer[I, O]) Receive() (I, error) {
	select {
	case err := <-s.ErrC:
		return nil, err
	case req := <-s.Requests:
		return req, nil
	}
}

func (s *StreamServer[I, O]) Send(res O) error { s.Responses <- res; return nil }

func (s *StreamServer[I, O]) CloseSend() error {
	s.Responses <- io.EOF
	close(s.Responses)
	return nil
}
