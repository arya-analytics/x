package confluence_test

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
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
		pipe.Segment("router", &confluence.Switch[int]{Switch: func(ctx signal.Context,
			i int) (address.Address, error) {
			if i%2 == 0 {
				return "single", nil
			} else {
				return "double", nil
			}
		}})
		Expect(pipe.RouteInletTo("router")).To(Succeed())
		t1 := &confluence.Transform[int]{Transform: func(ctx signal.Context,
			i int) (int, bool, error) {
			return i * i, true, nil
		}}
		t2 := &confluence.Transform[int]{Transform: func(ctx signal.Context,
			i int) (int, bool, error) {
			return i * i * 2, true, nil
		}}
		pipe.Segment("single", t1)
		pipe.Segment("double", t2)
		Expect(pipe.Route(confluence.MultiRouter[int]{
			FromAddresses: []address.Address{"router"},
			ToAddresses:   []address.Address{"single", "double"},
			Stitch:        confluence.StitchWeave,
		})).To(Succeed())
		Expect(pipe.RouteOutletFrom("single", "double")).To(Succeed())
		ctx, cancel := signal.WithCancel(context.Background())
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
		cancel()
		Expect(errors.Is(ctx.WaitOnAll(), context.Canceled)).To(BeTrue())
		Expect(values).To(HaveLen(3))
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		Expect(values).To(Equal([]int{2, 4, 18}))
	})
})
