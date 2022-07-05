package transfluence_test

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/confluence/transfluence"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func clientStreamsToSlice(
	clients map[address.Address]transport.StreamSenderCloser[int],
) []transport.StreamSenderCloser[int] {
	slice := make([]transport.StreamSenderCloser[int], 0, len(clients))
	for _, client := range clients {
		slice = append(slice, client)
	}
	return slice
}

var _ = Describe("Sender", func() {
	var net *tmock.Network[int, int]
	BeforeEach(func() {
		net = tmock.NewNetwork[int, int]()
	})
	Context("Single Stream", func() {
		var stream transport.Stream[int, int]
		BeforeEach(func() {
			stream = net.RouteStream("", 10)
		})
		Describe("Sender", func() {
			It("Should operate correctly", func() {
				var (
					receiverStream = confluence.NewStream[int](0)
					senderStream   = confluence.NewStream[int](0)
				)
				stream.Handle(func(ctx context.Context, server transport.StreamServer[int, int]) error {
					sCtx, cancel := signal.WithCancel(ctx)
					defer cancel()
					receiver := &transfluence.Receiver[int]{}
					receiver.Receiver = server
					receiver.OutTo(receiverStream)
					receiver.Flow(sCtx, confluence.CloseInletsOnExit())
					return sCtx.WaitOnAll()
				})
				sCtx, cancel := signal.WithCancel(context.TODO())
				client, err := stream.Stream(sCtx, "localhost:0")
				Expect(err).ToNot(HaveOccurred())
				sender := &transfluence.Sender[int]{Sender: client}
				sender.InFrom(senderStream)
				sender.Flow(sCtx)
				senderStream.Inlet() <- 1
				v := <-receiverStream.Outlet()
				Expect(v).To(Equal(1))
				senderStream.Inlet() <- 2
				v = <-receiverStream.Outlet()
				cancel()
				Expect(sCtx.WaitOnAll()).To(Equal(context.Canceled))
				_, ok := <-receiverStream.Outlet()
				Expect(ok).To(BeFalse())
			})
		})
		Describe("TransformSender", func() {
			It("Should transform values before sending them", func() {
				var (
					receiverStream = confluence.NewStream[int](0)
					senderStream   = confluence.NewStream[int](0)
				)
				stream.Handle(func(ctx context.Context, server transport.StreamServer[int, int]) error {
					sCtx, cancel := signal.WithCancel(ctx)
					defer cancel()
					receiver := &transfluence.Receiver[int]{}
					receiver.Receiver = server
					receiver.OutTo(receiverStream)
					receiver.Flow(sCtx, confluence.CloseInletsOnExit())
					return sCtx.WaitOnAll()
				})
				sCtx, cancel := signal.WithCancel(context.TODO())
				client, err := stream.Stream(sCtx, "localhost:0")
				Expect(err).ToNot(HaveOccurred())
				sender := &transfluence.TransformSender[int, int]{}
				sender.Sender = client
				sender.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
					return v * 2, true, nil
				}
				sender.InFrom(senderStream)
				sender.Flow(sCtx)
				senderStream.Inlet() <- 1
				v := <-receiverStream.Outlet()
				Expect(v).To(Equal(2))
				cancel()
				Expect(sCtx.WaitOnAll()).To(Equal(context.Canceled))
				_, ok := <-receiverStream.Outlet()
				Expect(ok).To(BeFalse())
			})
		})

	})
	Context("Multiple Streams", func() {
		var (
			sCtx            signal.Context
			cancel          context.CancelFunc
			nStreams        = 5
			senderStream    confluence.Stream[int]
			receiverStreams map[address.Address]confluence.Stream[int]
			clientStreams   map[address.Address]transport.StreamSenderCloser[int]
		)
		BeforeEach(func() {
			sCtx, cancel = signal.WithCancel(context.TODO())
			senderStream = confluence.NewStream[int](nStreams)
			clientTransport := net.RouteStream("", 0)
			clientStreams = make(map[address.Address]transport.StreamSenderCloser[int], nStreams)
			receiverStreams = make(map[address.Address]confluence.Stream[int], nStreams)
			for i := 0; i < nStreams; i++ {
				stream := net.RouteStream("", 0)
				receiverStream := confluence.NewStream[int](1)
				stream.Handle(func(ctx context.Context, server transport.StreamServer[int, int]) error {
					serverCtx, cancel := signal.WithCancel(ctx)
					defer cancel()
					receiver := &transfluence.Receiver[int]{}
					receiver.Receiver = server
					receiver.OutTo(receiverStream)
					receiver.Flow(serverCtx, confluence.CloseInletsOnExit())
					return serverCtx.WaitOnAll()
				})
				clientStream, err := clientTransport.Stream(sCtx, stream.Address)
				Expect(err).ToNot(HaveOccurred())
				clientStreams[stream.Address] = clientStream
				receiverStreams[stream.Address] = receiverStream
			}
		})
		AfterEach(func() { cancel() })
		Describe("MultiSender", func() {
			It("Should forward values to all streams", func() {
				sender := &transfluence.MultiSender[int]{}
				sender.Senders = clientStreamsToSlice(clientStreams)
				sender.InFrom(senderStream)
				sender.Flow(sCtx)
				senderStream.Inlet() <- 2
				for addr := range clientStreams {
					v := <-receiverStreams[addr].Outlet()
					Expect(v).To(Equal(2))
				}
				senderStream.Close()
				Expect(sCtx.WaitOnAll()).To(Succeed())
			})
		})
		Describe("SwitchSender", func() {
			It("Should route values to the correct stream", func() {
				sender := &transfluence.SwitchSender[int]{}
				sender.Senders = clientStreams
				sender.ApplySwitch = func(ctx signal.Context, v int) (address.Address, bool, error) {
					addr := address.NewF("localhost:%v", v)
					return addr, true, nil
				}
				sender.InFrom(senderStream)
				sender.Flow(sCtx)
				for i := 1; i < nStreams+1; i++ {
					senderStream.Inlet() <- i
					addr := address.NewF("localhost:%v", i)
					v := <-receiverStreams[addr].Outlet()
					Expect(v).To(Equal(i))
				}
				senderStream.Close()
				Expect(sCtx.WaitOnAll()).To(Succeed())
			})
		})
		Describe("BatchSwitchSender", func() {
			It("Should route values to the correct stream", func() {
				sender := &transfluence.BatchSwitchSender[int, int]{}
				sender.Senders = clientStreams
				sender.ApplySwitch = func(ctx signal.Context, v int, o map[address.Address]int) error {
					addr := address.NewF("localhost:%v", v)
					o[addr] = v
					addr2 := address.NewF("localhost:%v", v+1)
					o[addr2] = v + 1
					return nil
				}
				sender.InFrom(senderStream)
				sender.Flow(sCtx)
				senderStream.Inlet() <- 2
				Expect(<-receiverStreams["localhost:2"].Outlet()).To(Equal(2))
				Expect(<-receiverStreams["localhost:3"].Outlet()).To(Equal(3))
				senderStream.Close()
				Expect(sCtx.WaitOnAll()).To(Succeed())
			})
		})
	})
})
