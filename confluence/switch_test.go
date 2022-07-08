package confluence_test

import (
	"context"
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ApplySink", func() {
	Context("Single Inlet", func() {
		var (
			ctx    signal.Context
			cancel context.CancelFunc
			input  confluence.Stream[int]
			double confluence.Stream[int]
			single confluence.Stream[int]
			sw     *confluence.Switch[int]
		)
		BeforeEach(func() {
			ctx, cancel = signal.Background()
			input = confluence.NewStream[int](3)
			double = confluence.NewStream[int](3)
			single = confluence.NewStream[int](3)
			double.SetInletAddress("double")
			single.SetInletAddress("single")
			sw = &confluence.Switch[int]{}
			sw.ApplySwitch = func(ctx signal.Context, i int) (address.Address, bool, error) {
				if i%2 == 0 {
					return "single", true, nil
				} else {
					return "double", true, nil
				}
			}
			sw.InFrom(input)
			sw.OutTo(double)
			sw.OutTo(single)
			sw.Flow(ctx, confluence.CloseInletsOnExit())
		})
		AfterEach(func() {
			cancel()
		})
		It("Should route values to the correct inlets", func() {
			input.Inlet() <- 1
			input.Inlet() <- 2
			input.Inlet() <- 3
			input.Close()
			Expect(ctx.Wait()).To(Succeed())
			Expect(<-double.Outlet()).To(Equal(1))
			Expect(<-single.Outlet()).To(Equal(2))
			Expect(<-double.Outlet()).To(Equal(3))
			_, ok := <-double.Outlet()
			Expect(ok).To(BeFalse())
		})
	})
	It("Should route values from multiple outlets correctly", func() {
		stream1 := confluence.NewStream[int](3)
		stream2 := confluence.NewStream[int](3)
		single := confluence.NewStream[int](5)
		single.SetInletAddress("single")
		sw := &confluence.Switch[int]{}
		sw.ApplySwitch = func(ctx signal.Context, i int) (address.Address, bool, error) {
			return "single", true, nil
		}
		sw.InFrom(stream1)
		sw.InFrom(stream2)
		sw.OutTo(single)
		ctx, cancel := signal.WithCancel(context.Background())
		defer cancel()
		sw.Flow(ctx, confluence.CloseInletsOnExit())
		stream1.Inlet() <- 1
		stream1.Inlet() <- 2
		stream2.Inlet() <- 3
		stream2.Inlet() <- 4
		stream2.Close()
		stream1.Close()
		Expect(ctx.Wait()).To(Succeed())
		count := 0
		for range single.Outlet() {
			count++
		}
		Expect(count).To(Equal(4))
	})
})
