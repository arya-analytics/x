package confluence_test

import (
	"github.com/arya-analytics/x/confluence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Filter", func() {
	It("Should filter values correctly", func() {
		inlet := confluence.NewStream[int](3)
		outlet := confluence.NewStream[int](3)
		filter := confluence.Filter[int]{Filter: func(x int) bool { return x%3 == 0 }}
		filter.InFrom(inlet)
		filter.OutTo(outlet)
		ctx := confluence.DefaultContext()
		filter.Flow(ctx)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		inlet.Inlet() <- 3
		Expect(<-outlet.Outlet()).To(Equal(3))
		Expect(ctx.Shutdown.Shutdown()).To(Succeed())
	})
})
