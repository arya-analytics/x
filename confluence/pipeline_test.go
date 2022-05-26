package confluence_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sort"
)

var _ = Describe("Pipeline", func() {
	It("Should execute the composite correctly", func() {
		pipe := confluence.NewPipeline[int]()
		inlet, outlet := confluence.NewStream[int](3), confluence.NewStream[int](3)
		pipe.InFrom(inlet)
		pipe.OutTo(outlet)
		pipe.Segment("router", &confluence.Switch[int]{Route: func(i int) address.Address {
			if i%2 == 0 {
				return "single"
			} else {
				return "double"
			}
		}})
		Expect(pipe.RouteInletTo("router")).To(Succeed())
		pipe.Segment("single", &confluence.Transform[int]{Transform: func(i int) int { return i * i }})
		pipe.Segment("double", &confluence.Transform[int]{Transform: func(i int) int { return i * i * 2 }})
		Expect(pipe.Route(confluence.MultiRouter[int]{
			FromAddresses: []address.Address{"router"},
			ToAddresses:   []address.Address{"single", "double"},
			Stitch:        confluence.StitchWeave,
		})).To(Succeed())
		Expect(pipe.RouteOutletFrom("single", "double")).To(Succeed())
		ctx := confluence.DefaultContext()
		pipe.Flow(ctx)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		inlet.Inlet() <- 3
		var values []int
		for v := range outlet.Outlet() {
			values = append(values, v)
			if len(values) == 3 {
				break
			}
		}
		Expect(ctx.Shutdown.Shutdown()).To(Succeed())
		Expect(values).To(HaveLen(3))
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		Expect(values).To(Equal([]int{2, 4, 18}))
	})
})
