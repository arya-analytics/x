package transfluence

import (
	. "github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/errutil"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	"github.com/cockroachdb/errors"
)

// Sender wraps transport.StreamSender to provide a confluence compatible
// interface for sending messages over a network transport.
type Sender[M transport.Message] struct {
	Sender transport.StreamSender[M]
	AbstractUnarySink[M]
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
			case res, ok := <-s.AbstractUnarySink.In.Outlet():
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

// SenderTransform wraps transport. StreamSender to provide a confluence compatible
// interface for sending messages over a network transport. SenderTransform adds
// a transform function to the Sender. This is particularly useful in cases
// where network message types are different from the message types used by the
// rest of a program.
type SenderTransform[I Value, M transport.Message] struct {
	Sender transport.StreamSender[M]
	Transform[I, M]
	AbstractUnarySink[I]
}

// Flow implements the Flow interface.
func (s *SenderTransform[I, M]) Flow(ctx signal.Context, opts ...FlowOption) {
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
			case res, ok := <-s.AbstractUnarySink.In.Outlet():
				if !ok {
					break o
				}
				tRes, ok, err := s.Transform(ctx, res)
				if err != nil {
					err = errors.CombineErrors(err, err)
					break o
				}
				if err := s.Sender.Send(tRes); err != nil {
					err = errors.CombineErrors(err, err)
					break o
				}
			}
		}
		return err
	}, fo.Signal...)
}

// MultiSender wraps a slice of transport.StreamSender(s) to provide a confluence
// compatible interface for sending messages over a network transport. MultiSender
// sends a copy of each message received from the Outlet.
type MultiSender[M transport.Message] struct {
	Senders []transport.StreamSender[M]
	AbstractUnarySink[M]
}

// Flow implements the Flow interface.
func (m *MultiSender[M]) Flow(ctx signal.Context, opts ...FlowOption) {
	fo := NewFlowOptions(opts)
	ctx.Go(func() error {
		var err error
		defer func() {
			err = errors.CombineErrors(m.closeSenders(), err)
		}()
	o:
		for {
			select {
			case <-ctx.Done():
				err = errors.CombineErrors(err, ctx.Err())
				break o
			case res, ok := <-m.AbstractUnarySink.In.Outlet():
				if !ok {
					break o
				}
				for _, sender := range m.Senders {
					if err := sender.Send(res); err != nil {
						err = errors.CombineErrors(err, err)
						break o
					}
				}
			}
		}
		return err
	}, fo.Signal...)
}

func (m *MultiSender[M]) closeSenders() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, s := range m.Senders {
		c.Exec(s.CloseSend)
	}
	return c.Error()
}
