package confluence_test

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	//log "github.com/sirupsen/logrus"
	"time"
)

var _ = Describe("Switch", func() {
	It("Should route values to the correct inlets", func() {
		stream := confluence.NewStream[int](3)
		double := confluence.NewStream[int](3)
		double.SetInletAddress("double")
		single := confluence.NewStream[int](3)
		single.SetInletAddress("single")
		router := &confluence.Switch[int]{
			Switch: func(ctx confluence.Context, i int) (address.Address, error) {
				if i%2 == 0 {
					return "single", nil
				} else {
					return "double", nil
				}
			},
		}
		router.InFrom(stream)
		router.OutTo(double)
		router.OutTo(single)
		ctx, cancel := confluence.DefaultContext()
		router.Flow(ctx)
		stream.Inlet() <- 1
		stream.Inlet() <- 2
		stream.Inlet() <- 3
		time.Sleep(2 * time.Millisecond)
		cancel()
		Expect(errors.Is(ctx.WaitOnAll(), context.Canceled)).To(BeTrue())
		Expect(<-double.Outlet()).To(Equal(1))
		Expect(<-single.Outlet()).To(Equal(2))
		Expect(<-double.Outlet()).To(Equal(3))
	})
	It("Should route values from multiple inlets correctly", func() {
		stream1 := confluence.NewStream[int](3)
		stream2 := confluence.NewStream[int](3)
		single := confluence.NewStream[int](5)
		single.SetInletAddress("single")
		router := &confluence.Switch[int]{Switch: func(ctx confluence.Context,
			i int) (address.Address, error) {
			return "single", nil
		}}
		router.InFrom(stream1)
		router.InFrom(stream2)
		router.OutTo(single)
		ctx, cancel := confluence.DefaultContext()
		router.Flow(ctx)
		stream1.Inlet() <- 1
		stream1.Inlet() <- 2
		stream2.Inlet() <- 3
		stream2.Inlet() <- 4
		count := 0
		for range single.Outlet() {
			count++
			if count == 4 {
				break
			}
		}
		cancel()
		Expect(errors.Is(ctx.WaitOnAll(), context.Canceled)).To(BeTrue())
		Expect(count).To(Equal(4))
	})
})
