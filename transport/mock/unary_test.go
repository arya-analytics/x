package mock_test

import (
	"context"
	"github.com/arya-analytics/x/address"
	tmock "github.com/arya-analytics/x/transport/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Unary", func() {
	var net *tmock.Network[int, int]
	BeforeEach(func() { net = tmock.NewNetwork[int, int]() })
	It("Should correctly handle an exchange client and server", func() {
		t1 := net.RouteUnary("localhost:0")
		t2 := net.RouteUnary("localhost:1")
		t1.Handle(func(ctx context.Context, in int) (out int, err error) {
			return in + 1, nil
		})
		res, err := t2.Send(ctx, "localhost:0", 1)
		Expect(err).To(BeNil())
		Expect(res).To(Equal(2))
	})
	It("Should return the a TargetNotFound error when no route is found", func() {
		t1 := net.RouteUnary("localhost:0")
		t1.Handle(func(ctx context.Context, in int) (out int, err error) {
			return in + 1, nil
		})
		res, err := t1.Send(ctx, "localhost:1", 1)
		Expect(err).To(MatchError(address.NotFound))
		Expect(res).To(Equal(0))
	})
})
