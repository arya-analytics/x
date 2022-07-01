package confluence

import (
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
)

// Sender wraps transport.StreamSender to provide a confluence compatible
// interface for sending messages over a network transport.
type Sender[M transport.Message] struct {
	Sender transport.StreamSender[M]
	UnarySink[M]
}

// Flow implements Flow.
func (s *Sender[M]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	ctx.Go(func() error {
		var err error
		defer func() {
			err = errors.CombineErrors(s.Sender.CloseSend(), err)
		}()
	o:
		for {
			select {
			case <-ctx.Done():
				err = errors.CombineErrors(err, ctx.Err())
				break o
			case res, ok := <-s.UnarySink.In.Outlet():
				if !ok {
					break o
				}
				if err := s.Sender.Send(res); err != nil {
					err = errors.CombineErrors(err, err)
					break o
				}
			}
		}
		return err
	}, fo.Signal...)
}

// Receiver wraps transport.StreamReceiver to provide a confluence compatible
// interface for receiving messages from a network transport.
type Receiver[M transport.Message] struct {
	Receiver transport.StreamReceiver[M]
	UnarySource[M]
	flowing bool
}

// Flow implements Flow.
func (r *Receiver[M]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	ctx.Go(func() error {
		var err error
	o:
		for {
			select {
			default:
				res, rErr := r.Receiver.Receive()
				if errors.Is(rErr, transport.EOF) {
					break o
				}
				if rErr != nil {
					err = err
					break o
				}
				r.UnarySource.Out.Inlet() <- res
			}
		}
		return err
	}, fo.Signal...)
}
