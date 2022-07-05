package transport

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/address"
)

type Message any

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
// and a server. Stream is bidirectional, meaning that both client and server
// can exchange messages in an asynchronous manner. Unary transport should be preferred over
// Stream as it is simpler. Stream transport is useful for cases where the client and
// server/need to exchange many messages over extended periods of time in a non-blocking
// fashion. The semantics of Stream communication are more complex than Unary, and care
// should be taken when managing the Stream lifecycle.
type Stream[I, O Message] interface {
	Transport
	// Stream opens a stream to the target server using the given context. If the context
	// is canceled, the server should discard unprocessed requests, free resources
	// related to the stream, and return a context error to the caller. If stream cannot
	// be opened (ex. the server cannot be reached), Stream will return an error.
	// For the semantics of stream operation, see the StreamClient interface.
	Stream(ctx context.Context, target address.Address) (StreamClient[I, O], error)
	// Handle is called by the server to handle a request from the client. If the context
	// is cancelled , the server is expected to discard unprocessed requests, free resources
	// related to the stream, and return an error to the caller (ideally this error
	// should wrap a context error in some form).
	//
	// Transient errors (errors that may be fatal to a request, but not to the stream)
	// should be returned as part of the response itself. This is typically in the
	// form of an 'Err' struct field in the response type O.
	//
	// Fatal errors (errors that prevent the server from processing any future requests)
	// should be returned from the handle function itself (f). If the handle function
	// returns nil, the server will close the stream, returning a final transport.EOF
	// error to the client. If the handle function returns an error, the server will
	// close the stream and return the error to the client.
	//
	// For details on the semantics of the handle function, see the StreamServer
	// interface.
	Handle(f func(ctx context.Context, server StreamServer[I, O]) error)
}

// StreamClient is the client side interface of Stream transport.
type StreamClient[I, O Message] interface {
	StreamSenderCloser[I]
	StreamReceiver[O]
}

// StreamServer is the server side interface of Stream transport.
type StreamServer[I, O Message] interface {
	StreamReceiver[I]
	StreamSender[O]
}

type StreamReceiver[M Message] interface {
	// Receive blocks a message is received from the sender or the stream closes. If the
	// server closes the stream without error, Receive will return a transport.EOF error.
	// If the server encountered an error, Receive will return the error. If the stream
	// is closed and Receive is called repeatedly, it will continue to return the
	// same error the stream was closed with. If StreamReceiver is paired with a
	// StreamSender, the sending stream will be closed when the receiving stream is
	// closed. The opposite is NOT true.
	Receive() (M, error)
}

type StreamSender[M Message] interface {
	// Send sends a response to the receiver. If an error is returned, the stream is
	// aborted and closed. Repeated calls to Send after the stream is closed will
	// return the same error that originally caused it to abort. If StreamSender is
	// paired with a StreamReceiver, the sending stream will be closed when the
	// receiving stream is closed. The opposite is NOT true.
	Send(M) error
}

type StreamSenderCloser[M Message] interface {
	StreamSender[M]
	StreamCloser
}

// StreamCloser is a type that can close the sending end of a Stream.
type StreamCloser interface {
	// CloseSend closes the stream and sends a transport.EOF message to the receiver.
	// CloseSend is NOT safe to call concurrently with StreamSender.Send. Send will panic if called
	// after CloseSend. CloseSend is idempotent and can be called repeatedly.
	CloseSend() error
}

// SenderEmptyCloser wraps a StreamSender so that it can satisfy the StreamSenderCloser
// interface. This is useful for types that deal with both StreamServer and StreamClient
// side applications. This allows a StreamServer. StreamSender to be used with client side
// code.
type SenderEmptyCloser[M Message] struct{ StreamSender[M] }

// CloseSend implements the StreamCloser interface.
func (c SenderEmptyCloser[M]) CloseSend() error { return nil }
