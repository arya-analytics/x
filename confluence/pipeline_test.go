package confluence_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipeline", func() {
	FIt("Should execute the composite correctly", func() {
		inlet, outlet := confluence.NewStream[int](3), confluence.NewStream[int](3)
		router := &confluence.Switch[int]{Route: func(i int) address.Address {
			if i%2 == 0 {
				return "single"
			} else {
				return "double"
			}
		}}
		square := &confluence.Transform[int]{Transform: func(i int) int { return i * i }}
		doubleSquare := &confluence.Transform[int]{Transform: func(i int) int { return i * i * 2 }}
		t := &confluence.Pipeline[int]{}
		t.InFrom(inlet)
		t.OutTo(outlet)
		t.SetSegment("router", router)
		Expect(t.RouteInletTo("router")).To(Succeed())
		t.SetSegment("single", square)
		t.SetSegment("double", doubleSquare)
		Expect(t.Route("router", "double", 1)).To(Succeed())
		Expect(t.Route("router", "single", 1)).To(Succeed())
		Expect(t.RouteOutletFrom("double")).To(Succeed())
		Expect(t.RouteOutletFrom("single")).To(Succeed())
		ctx := confluence.DefaultContext()
		t.Flow(ctx)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		inlet.Inlet() <- 3
		Expect(<-outlet.Outlet()).To(Equal(1))
		Expect(<-outlet.Outlet()).To(Equal(4))
		Expect(<-outlet.Outlet()).To(Equal(9))
		Expect(ctx.Shutdown.Shutdown()).To(Succeed())
	})
})
