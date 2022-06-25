package confluence_test

import (
	"github.com/arya-analytics/x/confluence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("transform", func() {
	It("Should transform values correctly", func() {
		inlet := confluence.NewStream[int](3)
		outlet := confluence.NewStream[int](4)
		square := &confluence.Transform[int]{Transform: func(ctx confluence.Context, i int) (int, bool) { return i * i, true }}
		square.InFrom(inlet)
		square.OutTo(outlet)
		ctx := confluence.WrapContext()
		square.Flow(ctx)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		Expect(<-outlet.Outlet()).To(Equal(1))
		Expect(<-outlet.Outlet()).To(Equal(4))
		Expect(ctx.Shutdown.Shutdown()).To(Succeed())
	})
})
