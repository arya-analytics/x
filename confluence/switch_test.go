package confluence_test

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	//log "github.com/sirupsen/logrus"
	"time"
)

var _ = Describe("Apply", func() {
	It("Should route values to the correct inlets", func() {
		input := confluence.NewStream[int](3)

		double := confluence.NewStream[int](3)
		double.SetInletAddress("double")

		single := confluence.NewStream[int](3)
		single.SetInletAddress("single")

		router := &confluence.Switch[int]{
			Apply: func(ctx signal.Context, i int) (address.Address, bool, error) {
				if i%2 == 0 {
					return "single", true, nil
				} else {
					return "double", true, nil
				}
			},
		}
		router.InFrom(input)
		router.OutTo(double)
		router.OutTo(single)
		ctx, cancel := signal.WithCancel(context.Background())
		router.Flow(ctx)
		input.Inlet() <- 1
		input.Inlet() <- 2
		input.Inlet() <- 3
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
		router := &confluence.Switch[int]{Apply: func(ctx signal.Context, i int) (address.Address, bool, error) {
			return "single", true, nil
		}}
		router.InFrom(stream1)
		router.InFrom(stream2)
		router.OutTo(single)
		ctx, cancel := signal.WithCancel(context.Background())
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
