package transfluence

import (
	. "github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
)

// Receiver wraps transport.StreamReceiver to provide a confluence compatible
// interface for receiving messages from a network transport.
type Receiver[M transport.Message] struct {
	Receiver transport.StreamReceiver[M]
	AbstractUnarySource[M]
}

// Flow implements Flow.
func (r *Receiver[M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(r)
	ctx.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				msg, rErr := r.Receiver.Receive()
				if errors.Is(rErr, transport.EOF) {
					return nil
				}
				if rErr != nil {
					return rErr
				}
				r.Out.Inlet() <- msg
			}
		}
	}, fo.Signal...)
}

type TransformReceiver[I Value, M transport.Message] struct {
	Receiver transport.StreamReceiver[M]
	AbstractUnarySource[I]
	TransformFunc[M, I]
}

// Flow implements Flow.
func (r *TransformReceiver[I, M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(r)
	ctx.Go(func() error {
	o:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				res, err := r.Receiver.Receive()
				if errors.Is(err, transport.EOF) {
					return nil
				}
				if err != nil {
					return err
				}
				tRes, ok, err := r.ApplyTransform(ctx, res)
				if !ok {
					continue o
				}
				if err != nil {
					return err
				}
				r.Out.Inlet() <- tRes
			}
		}
	}, fo.Signal...)
}
