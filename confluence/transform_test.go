package confluence_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Transform", func() {
	It("Should transform values correctly", func() {
		inlet := confluence.NewStream[int](3)
		outlet := confluence.NewStream[int](4)
		square := &confluence.Transform[int]{Transform: func(i int) int { return i * i }}
		square.InFrom(inlet)
		square.OutTo(outlet)
		sd := shutdown.New(shutdown.WithThreshold(1 * time.Millisecond))
		square.Flow(sd)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		Expect(<-outlet.Outlet()).To(Equal(1))
		Expect(<-outlet.Outlet()).To(Equal(4))
		Expect(sd.Shutdown()).To(Succeed())
	})
})
