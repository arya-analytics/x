package confluence_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
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
		sd := shutdown.New()
		filter.Flow(sd)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		inlet.Inlet() <- 3
		Expect(<-outlet.Outlet()).To(Equal(3))
		Expect(sd.Shutdown()).To(Succeed())
	})
})
