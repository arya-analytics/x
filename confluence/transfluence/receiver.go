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
		var err error
	o:
		for {
			select {
			case <-ctx.Done():
				err = errors.CombineErrors(err, ctx.Err())
				break o
			default:
				res, rErr := r.Receiver.Receive()
				if errors.Is(rErr, transport.EOF) {
					break o
				}
				if rErr != nil {
					err = rErr
					break o
				}
				r.AbstractUnarySource.Out.Inlet() <- res
			}
		}
		return err
	}, fo.Signal...)
}

type ReceiverTransform[I Value, M transport.Message] struct {
	Receiver transport.StreamReceiver[M]
	AbstractUnarySource[I]
	TransformFunc[M, I]
}

// Flow implements Flow.
func (r *ReceiverTransform[I, M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	fo.AttachInletCloser(r)
	ctx.Go(func() error {
		var err error
	o:
		for {
			select {
			case <-ctx.Done():
				err = errors.CombineErrors(err, ctx.Err())
				break o
			default:
				res, rErr := r.Receiver.Receive()
				if errors.Is(rErr, transport.EOF) {
					break o
				}
				if rErr != nil {
					err = err
					break o
				}
				tRes, ok, err := r.ApplyTransform(ctx, res)
				if !ok {
					continue o
				}
				if err != nil {
					return err
				}
				r.AbstractUnarySource.Out.Inlet() <- tRes
			}
		}
		return err
	}, fo.Signal...)
}
