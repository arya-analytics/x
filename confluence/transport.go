package confluence

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/transport"
	"io"
)

// Client wraps transport.StreamClient to provide a confluence compatible
// interface for sending and receiving messages over the network.
type Client[I, O transport.Message] struct {
	Client    transport.StreamClient[I, O]
	Requests  confluence.UnarySink[I]
	Responses confluence.UnarySource[O]
}

// Flow implements confluence.Context.
func (tc *Client[I, O]) Flow(ctx confluence.Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			default:
				res, err := tc.Client.Receive()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					ctx.ErrC <- err
					continue
				}
				tc.Responses.Out.Inlet() <- res
			}
		}
	})
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		defer func() {
			if err := tc.Client.CloseSend(); err != nil {
				ctx.ErrC <- err
			}
		}()
		for {
			select {
			case <-sig:
				return nil
			case req, ok := <-tc.Requests.In.Outlet():
				if !ok {
					return nil
				}
				if err := tc.Client.Send(req); err != nil {
					ctx.ErrC <- err
					continue
				}
			}
		}
	})
}

// Server wraps transport.StreamServer to provide a confluence compatible interface
// for sending and receiving messages over the network.
type Server[I, O transport.Message] struct {
	Server    transport.StreamServer[I, O]
	Requests  confluence.UnarySource[I]
	Responses confluence.UnarySink[O]
}

func (tc *Server[I, O]) Flow(ctx confluence.Context) {
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			default:
				req, err := tc.Server.Receive()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					ctx.ErrC <- err
					continue
				}
				tc.Requests.Out.Inlet() <- req
			}
		}
	})
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		defer func() {
			if err := tc.Server.CloseSend(); err != nil {
				ctx.ErrC <- err
			}
		}()
		for {
			select {
			case <-sig:
				return nil
			case res, ok := <-tc.Responses.In.Outlet():
				if !ok {
					return nil
				}
				if err := tc.Server.Send(res); err != nil {
					ctx.ErrC <- err
					continue
				}
			}
		}
	})
}
