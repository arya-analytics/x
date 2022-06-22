package transport

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
)

type Message interface{}

type Transport interface {
	// Stringer returns a string description of the transport.
	fmt.Stringer
}

// Unary is an entity that implements a simple request-response transport between a
// client and a server.
type Unary[I, O Message] interface {
	Transport
	UnaryClient[I, O]
	UnaryServer[I, O]
}

// UnaryClient is the client side interface of Unary transport.
type UnaryClient[I, O Message] interface {
	// Send sends a request to the target server using the given context. The context
	// should be canceled if the client expects the server to discard the request
	// and return an error upon receiving it.
	Send(ctx context.Context, target address.Address, req I) (res O, err error)
}

type UnaryServer[I, O Message] interface {
	// Handle handles a request from the client. THe server is expected to send
	// a response along with any errors encountered during processing. If the provided
	// context is invalid, the server is expected to abort the request and respond
	// with an error (ideally this error should wrap a context error in some form).
	Handle(func(context.Context, I) (O, error))
}

// Stream is an entity that implements streaming transport of messages between a client
// and a server. Stream is a bidirectional transport, meaning that both client and server
// can exchange messages. Unary transport should generally be preferred over Stream,
// as it is simpler. Stream is useful for cases where the client and server need to
// exchange large amounts of messages over extended periods of time (in a non-blocking
// fashion).
type Stream[I, O Message] interface {
	Transport
	// Stream opens a stream to the target server using the given context. The context
	// should be canceled if the client expects the server to discard future requests
	// and abort the stream. Stream will return an error if the server cannot be reached
	// or the stream cannot be opened.
	Stream(ctx context.Context, target address.Address) (StreamClient[I, O], error)
	// Handle handles a stream of requests from the client using the given context. If
	// the provided context is invalidated during operation, the server is expected
	// to immediately abort processing requests and respond with an error (ideally
	// this error should wrap a context error in some form). Transient errors (i.e.
	// errors that may be fatal to a request, but not to the stream) should be returned
	// within the response itself (O). Errors fatal to the stream, however, should
	// cause the server to abort the stream and return an error. The transport
	// implementation is expected to send the fatal error as the final message
	// over the stream.
	Handle(func(ctx context.Context, srv StreamServer[I, O]) error)
}

// StreamClient is the client side interface of Stream transport.
type StreamClient[I, O Message] interface {
	// Send sends a request to the target server. The returned error represents a
	// fatal error encountered during message transfer over the network OR a fatal
	// error encountered by the server while processing the request. In any case,
	// the client is expected to abort future streams when a non-nil error is returned.
	Send(I) error
	// CloseSend closes the stream and sends a (INSERT HERE) message to the server.
	// CloseSend is NOT safe to call concurrently with Send. Send must panic if called
	// after CloseSend.
	CloseSend() error
	// Receive receives a response from the server. The returned error represents a
	// fatal error encountered during message transfer over the network OR a fatal
	// error encountered by the server while processing the response. In any case,
	// the client is expected to abort future streams when a non-nil error is returned.
	Receive() (O, error)
}

type StreamServer[I, O Message] interface {
	// Receive receives a request from the client. The returned error represents a
	// fatal error encountered during message transfer over the network OR a fatal
	// error encountered by the client while processing the request. In any case,
	// the server is expected to abort future streams when a non-nil error is returned.
	Receive() (I, error)
	// Send sends a response to the client. The returned error represents a
	// fatal error encountered during message transfer over the network OR a fatal
	// error encountered by the client while processing the request. In any case,
	// the server is expected to abort the stream when a non-nil error is returned.
	Send(O) error
	// CloseSend closes the stream and sends a (INSERT HERE) message to the client.
	// CloseSend is NOT safe to call concurrently with Send. Send must panic if called
	// after CloseSend.
	CloseSend() error
}
