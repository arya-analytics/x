package transfluence_test

import (
	"context"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/confluence/transfluence"
	"github.com/arya-analytics/x/signal"
	"github.com/arya-analytics/x/transport"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sender", func() {
	var (
		net    *tmock.Network[int, int]
		stream transport.Stream[int, int]
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[int, int]()
		stream = net.RouteStream("", 10)
	})
	Describe("Sender", func() {
		var (
			receiverStream = confluence.NewStream[int](0)
			senderStream   = confluence.NewStream[int](0)
		)
		It("Should operate correctly", func() {
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
})
