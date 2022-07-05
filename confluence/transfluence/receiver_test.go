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

var _ = Describe("Receiver", func() {
	var (
		net    *tmock.Network[int, int]
		stream transport.Stream[int, int]
	)
	BeforeEach(func() {
		net = tmock.NewNetwork[int, int]()
		stream = net.RouteStream("", 10)
	})
	It("Should operate correctly", func() {
		var receivedValues []int
		receiverStream := confluence.NewStream[int](10)
		stream.Handle(func(ctx context.Context, server transport.StreamServer[int, int]) error {
			sCtx, cancel := signal.WithCancel(ctx)
			defer cancel()
			receiver := &transfluence.Receiver[int]{Receiver: server}
			receiver.OutTo(receiverStream)
			receiver.Flow(sCtx, confluence.CloseInletsOnExit())
			By("Receiving values from the input stream")
			receivedValues = append(receivedValues, <-receiverStream.Outlet())
			return nil
		})
		client, err := stream.Stream(context.TODO(), "localhost:0")
		Expect(err).ToNot(HaveOccurred())
		Expect(client.Send(1)).To(Succeed())
		Expect(client.CloseSend()).To(Succeed())
		By("Closing the network pipe on return")
		_, err = client.Receive()
		Expect(err).To(Equal(transport.EOF))
		Expect(receivedValues).To(Equal([]int{1}))
		By("Closing the receive stream on context cancellation")
		_, ok := <-receiverStream.Outlet()
		Expect(ok).To(BeFalse())
	})
})
