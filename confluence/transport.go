package confluence

import (
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
)

// Sender wraps transport.StreamSender to provide a confluence compatible
// interface for sending messages over a network transport.
type Sender[M transport.Message] struct {
	Name   string
	Sender transport.StreamSender[M]
	UnarySink[M]
}

// Flow implements Flow.
func (s *Sender[M]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	ctx.Go(func() error {
		defer func() {
			if err := s.Sender.CloseSend(); err != nil {
				ctx.Transient() <- err
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case res, ok := <-s.UnarySink.In.Outlet():
				if !ok {
					return nil
				}
				if err := s.Sender.Send(res); err != nil {
					ctx.Transient() <- err
					return nil
				}
			}
		}
	}, fo.Signal...)
}

// Receiver wraps transport.StreamReceiver to provide a confluence compatible
// interface for receiving messages from a network transport.
type Receiver[M transport.Message] struct {
	Name     string
	Receiver transport.StreamReceiver[M]
	UnarySource[M]
	flowing bool
}

// Flow implements Flow.
func (r *Receiver[M]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	ctx.Go(func() error {
		for {
			select {
			default:
				res, err := r.Receiver.Receive()
				if errors.Is(err, transport.EOF) {
					return nil
				}
				if err != nil {
					ctx.Transient() <- err
					return nil
				}
				r.UnarySource.Out.Inlet() <- res
			}
		}
	}, fo.Signal...)
}
