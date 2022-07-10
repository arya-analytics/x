package mock

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/transport"
)

type message[V transport.Message] struct {
	value V
	error error
}

// Stream is a mock implementation of the transport.Stream interface.
type Stream[I, O transport.Message] struct {
	Address    address.Address
	BufferSize int
	Network    *Network[I, O]
	Handler    func(ctx context.Context, srv transport.StreamServer[I, O]) error
}

// Stream implements the transport.Stream interface.
func (s *Stream[I, O]) Stream(
	ctx context.Context,
	target address.Address,
) (transport.StreamClient[I, O], error) {
	route, ok := s.Network.StreamRoutes[target]
	if !ok || route.Handler == nil {
		return nil, address.TargetNotFound(target)
	}
	req, res := make(chan message[I], s.BufferSize), make(chan message[O], route.BufferSize)
	server := &serverStream[I, O]{ctx: ctx, requests: req, responses: res}
	serverClosed := make(chan struct{})
	go func() {
		err := route.Handler(ctx, server)
		if err == nil {
			err = transport.EOF
		}
		res <- message[O]{error: err}
		close(serverClosed)
	}()
	return &clientStream[I, O]{
		ctx:          ctx,
		requests:     req,
		responses:    res,
		serverClosed: serverClosed,
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

type serverStream[I, O transport.Message] struct {
	// ctx is the context the serverStream was started with. Yes, Yes! I know this is a bad
	// practice, but in this case we're essentially using it as a data container,
	// and we have a very good grasp on how it's used.
	ctx       context.Context
	requests  <-chan message[I]
	responses chan<- message[O]
	// inboundFatalErr indicates the request direction of the serverStream
	// closed. This should be a transport.EOF error if the serverStream closed successfully,
	// a context error if ctx was canceled, and any other error if the serverStream died
	// at the hands of the caller.
	inboundFatalErr  error
	outboundFatalErr error
}

// Send implements the transport.StreamSender interface.
func (s *serverStream[I, O]) Send(res O) error {
	// If outboundFatalErr is not nil, it means we've reached the end of the serverStream
	// and should continue returning the error until the client no longer calls
	// Send anymore.
	if s.outboundFatalErr != nil {
		return s.outboundFatalErr
	}
	if s.ctx.Err() != nil {
		s.outboundFatalErr = s.ctx.Err()
		return s.outboundFatalErr
	}
	select {
	case <-s.ctx.Done():
		s.outboundFatalErr = s.ctx.Err()
		return s.outboundFatalErr
	case s.responses <- message[O]{value: res}:
		return nil
	}
}

// Receive implements the transport.StreamClient interface.
func (s *serverStream[I, O]) Receive() (req I, err error) {
	// If inboundFatalErr is not nil, it means we've reached the end of the serverStream (whether
	// successfully or not) and should continue returning the error until the client
	// no longer calls Receive anymore.
	if s.inboundFatalErr != nil {
		return req, s.inboundFatalErr
	}
	select {
	case <-s.ctx.Done():
		s.inboundFatalErr = s.ctx.Err()
		return req, s.inboundFatalErr
	case msg := <-s.requests:
		// Any error message means the serverStream should die.
		if msg.error != nil {
			s.inboundFatalErr = msg.error
		}
		return msg.value, msg.error
	}
}

type clientStream[I, O transport.Message] struct {
	// ctx is the context the serverStream was started with. Yes, Yes! I know this is a bad
	// practice, but in this case we're essentially using it as a data container,
	// and we have a very good grasp on how it's used.
	ctx          context.Context
	requests     chan<- message[I]
	responses    <-chan message[O]
	serverClosed chan struct{}
	// inboundFatalErr indicates the request direction of the serverStream
	// closed. This should be a transport.EOF error if the serverStream closed successfully,
	// a context error if ctx was canceled, and any other error if the serverStream died
	// at the hands of the caller.
	inboundFatalErr  error
	outboundFatalErr error
}

func (c *clientStream[I, O]) Send(req I) error {
	if c.outboundFatalErr != nil {
		return c.outboundFatalErr
	}
	if c.ctx.Err() != nil {
		c.outboundFatalErr = c.ctx.Err()
		return c.outboundFatalErr
	}
	select {
	case <-c.serverClosed:
		c.outboundFatalErr = transport.EOF
		return c.outboundFatalErr
	case <-c.ctx.Done():
		c.outboundFatalErr = c.ctx.Err()
		return c.outboundFatalErr
	case c.requests <- message[I]{value: req}:
		return nil
	}
}

func (c *clientStream[I, O]) Receive() (res O, err error) {
	if c.inboundFatalErr != nil {
		return res, c.inboundFatalErr
	}
	select {
	case <-c.ctx.Done():
		c.inboundFatalErr = c.ctx.Err()
		return res, c.inboundFatalErr
	case msg := <-c.responses:
		if msg.error != nil {
			c.inboundFatalErr = msg.error
			if err := c.CloseSend(); err != nil {
				return res, err
			}
		}
		return msg.value, msg.error
	}
}

// CloseSend implements the transport.StreamCloser interface.
func (c *clientStream[I, O]) CloseSend() error {
	if c.outboundFatalErr != nil {
		return nil
	}
	c.outboundFatalErr = transport.EOF
	c.requests <- message[I]{error: c.outboundFatalErr}
	return nil
}
