package mock_test

import (
	"context"
	"github.com/arya-analytics/x/transport"
	tmock "github.com/arya-analytics/x/transport/mock"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stream", func() {
	var net *tmock.Network[int, int]
	BeforeEach(func() { net = tmock.NewNetwork[int, int]() })
	It("Should correctly exchange messages between a client and server", func() {
		t1 := net.RouteStream("localhost:0", 20)
		t2 := net.RouteStream("localhost:1", 20)
		t2.Handle(func(ctx context.Context, srv transport.StreamServer[int, int]) error {
			defer GinkgoRecover()
			for {
				msg, err := srv.Receive()
				if errors.Is(err, transport.EOF) {
					return nil
				}
				Expect(err).ToNot(HaveOccurred())
				Expect(srv.Send(msg + 1)).To(Succeed())
			}
		})
		client, err := t1.Stream(ctx, "localhost:1")
		Expect(err).ToNot(HaveOccurred())
		Expect(client.Send(1)).To(Succeed())
		Expect(client.Send(2)).To(Succeed())
		Expect(client.Send(3)).To(Succeed())
		res, err := client.Receive()
		Expect(err).ToNot(HaveOccurred())
		Expect(res).To(Equal(2))
		res, err = client.Receive()
		Expect(err).ToNot(HaveOccurred())
		Expect(res).To(Equal(3))
		res, err = client.Receive()
		Expect(err).ToNot(HaveOccurred())
		Expect(res).To(Equal(4))
		Expect(client.CloseSend()).To(Succeed())
		_, err = client.Receive()
		Expect(err).To(MatchError(transport.EOF))
	})
	It("Should close both client and server streams when the context is cancelled", func() {
		t1 := net.RouteStream("localhost:0", 20)
		t2 := net.RouteStream("localhost:1", 20)
		t2.Handle(func(ctx context.Context, srv transport.StreamServer[int, int]) error {
			defer GinkgoRecover()
			_, err := srv.Receive()
			Expect(err).To(Equal(context.Canceled))
			_, err = srv.Receive()
			Expect(err).To(Equal(context.Canceled))
			return nil
		})
		ctx, cancel := context.WithCancel(ctx)
		client, err := t1.Stream(ctx, "localhost:1")
		Expect(err).ToNot(HaveOccurred())
		cancel()
		Expect(client.Send(1)).To(MatchError(context.Canceled))
	})
	It("Should return a transport.EOF to client when server handler exits", func() {
		t1 := net.RouteStream("localhost:0", 20)
		t2 := net.RouteStream("localhost:1", 20)
		t2.Handle(func(ctx context.Context, srv transport.StreamServer[int, int]) error {
			defer GinkgoRecover()
			return nil
		})
		client, err := t1.Stream(ctx, "localhost:1")
		Expect(err).ToNot(HaveOccurred())
		_, err = client.Receive()
		Expect(err).To(MatchError(transport.EOF))
		Expect(client.Send(1)).To(MatchError(transport.EOF))
		Expect(client.CloseSend()).To(Succeed())
	})
})
