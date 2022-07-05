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
		Context("Multiple Streams", func() {
			var (
				nStreams = 5
				streams  map[address.Address]transport.Stream[int, int]
			)
			BeforeEach(func() {
				streams = make(map[address.Address]transport.Stream[int, int])
				for i := 0; i < nStreams; i++ {
					stream := net.RouteStream("", 10)
					streams[stream.Address] = stream
				}
			})
			Describe("MultiSender", func() {
				var (
					senderStream   = confluence.NewStream[int](nStreams)
					receiverStream = confluence.NewStream[int](nStreams)
				)
				It("Should forward values to all streams", func() {
					for addr, stream := range streams {
						if addr == "localhost:0" {
							continue
						}
						stream.Handle(func(ctx context.Context, server transport.StreamServer[int, int]) error {
							sCtx, cancel := signal.WithCancel(ctx)
							defer cancel()
							receiver := &transfluence.Receiver[int]{}
							receiver.Receiver = server
							receiver.OutTo(receiverStream)
							return sCtx.WaitOnAll()
						})
					}
					sCtx, cancel := signal.WithCancel(context.TODO())
					defer cancel()
					var clients []transport.StreamSenderCloser[int]
					clientStream := streams["localhost:0"]
					for addr, _ := range streams {
						if addr == "localhost:0" {
							continue
						}
						client, err := clientStream.Stream(sCtx, addr)
						Expect(err).ToNot(HaveOccurred())
						clients = append(clients, client)
					}
					sender := &transfluence.MultiSender[int]{}
					sender.Senders = clients
					sender.InFrom(senderStream)
					sender.Flow(sCtx)
					senderStream.Inlet() <- 2
					for range clients {
						v := <-receiverStream.Outlet()
						Expect(v).To(Equal(2))
					}
					senderStream.Close()
					Expect(sCtx.WaitOnAll()).To(Succeed())
				})
			})
		})
	})
})

func mapStreamsToList(streams map[address.Address]transport.Stream[int, int]) []transport.Stream[int, int] {
	var list []transport.Stream[int, int]
	for _, stream := range streams {
		list = append(list, stream)
	}
	return list
}
