package transfluence

import (
	"github.com/arya-analytics/x/address"
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
	UnarySink[M]
}

// Flow implements Flow.
func (s *Sender[M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
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

// SenderTransform wraps transport. StreamSender to provide a confluence compatible
// interface for sending messages over a network transport. SenderTransform adds
// a transform function to the Sender. This is particularly useful in cases
// where network message types are different from the message types used by the
// rest of a program.
type SenderTransform[I Value, M transport.Message] struct {
	Sender transport.StreamSender[M]
	TransformFunc[I, M]
	UnarySink[I]
}

// Flow implements the Flow interface.
func (s *SenderTransform[I, M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
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
				tRes, ok, tErr := s.TransformFunc.ApplyTransform(ctx, res)
				if tErr != nil {
					err = errors.CombineErrors(tErr, err)
					break o
				}
				if sErr := s.Sender.Send(tRes); sErr != nil {
					err = errors.CombineErrors(sErr, err)
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
	UnarySink[M]
}

// Flow implements the Flow interface.
func (m *MultiSender[M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
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
			case res, ok := <-m.UnarySink.In.Outlet():
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

type senderMap[M transport.Message] map[address.Address]transport.StreamSender[M]

func (s senderMap[M]) send(target address.Address, msg M) error {
	sender, ok := s[target]
	if !ok {
		return address.TargetNotFound(target)
	}
	return sender.Send(msg)
}

func (s senderMap[M]) closeSenders() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, s := range s {
		c.Exec(s.CloseSend)
	}
	return c.Error()
}

type SwitchSender[M transport.Message] struct {
	senderMap[M]
	SwitchFunc[M]
	UnarySink[M]
}

func (sw *SwitchSender[M]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	ctx.Go(func() error {
		var err error
		defer func() {
			err = errors.CombineErrors(sw.closeSenders(), err)
		}()
	o:
		for {
			select {
			case <-ctx.Done():
				err = errors.CombineErrors(err, ctx.Err())
				break o
			case msg, ok := <-sw.UnarySink.In.Outlet():
				if !ok {
					break o
				}
				target, ok, aErr := sw.SwitchFunc.ApplySwitch(ctx, msg)
				if !ok || aErr != nil {
					err = errors.CombineErrors(aErr, err)
					break o
				}
				if sErr := sw.send(target, msg); sErr != nil {
					err = errors.CombineErrors(sErr, err)
					break o
				}
			}
		}
		return err
	}, fo.Signal...)
}

type BatchSwitchSender[I, O transport.Message] struct {
	senderMap[O]
	BatchSwitchFunc[I, O]
	UnarySink[I]
}

func (bsw *BatchSwitchSender[I, O]) Flow(ctx signal.Context, opts ...Option) {
	fo := NewOptions(opts)
	addrMap := make(map[address.Address]O)
	ctx.Go(func() error {
		var err error
		defer func() {
			err = errors.CombineErrors(bsw.closeSenders(), err)
		}()
	o:
		for {
			select {
			case <-ctx.Done():
				err = errors.CombineErrors(err, ctx.Err())
				break o
			case msg, ok := <-bsw.UnarySink.In.Outlet():
				if !ok {
					break o
				}
				if aErr := bsw.BatchSwitchFunc.Apply(ctx, msg, addrMap); aErr != nil {
					err = errors.CombineErrors(aErr, err)
					break o
				}
				for target, batch := range addrMap {
					sErr := bsw.send(target, batch)
					delete(addrMap, target)
					if sErr != nil {
						err = errors.CombineErrors(sErr, err)
						break o
					}
				}
			}
		}
		return err
	}, fo.Signal...)
}
