package confluence_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Router", func() {
	It("Should route values to the correct inlets", func() {
		stream := confluence.NewStream[int](3)
		double := confluence.NewStream[int](3)
		double.SetInletAddress("double")
		single := confluence.NewStream[int](3)
		single.SetInletAddress("single")
		router := &confluence.Router[int]{
			Route: func(i int) address.Address {
				if i%2 == 0 {
					return "single"
				} else {
					return "double"
				}
			},
		}
		router.InFrom(stream)
		router.OutTo(double)
		router.OutTo(single)
		sd := shutdown.New()
		router.Flow(sd)
		stream.Inlet() <- 1
		stream.Inlet() <- 2
		stream.Inlet() <- 3
		Expect(sd.ShutdownAfter(2 * time.Millisecond)).To(Succeed())
		Expect(<-double.Outlet()).To(Equal(1))
		Expect(<-single.Outlet()).To(Equal(2))
		Expect(<-double.Outlet()).To(Equal(3))
	})
	It("Should route values from multiple inlets correctly", func() {
		stream1 := confluence.NewStream[int](3)
		stream2 := confluence.NewStream[int](3)
		single := confluence.NewStream[int](5)
		single.SetInletAddress("single")
		router := &confluence.Router[int]{Route: func(i int) address.Address {
			return "single"
		}}
		router.InFrom(stream1)
		router.InFrom(stream2)
		router.OutTo(single)
		sd := shutdown.New()
		router.Flow(sd)
		stream1.Inlet() <- 1
		stream1.Inlet() <- 2
		stream2.Inlet() <- 3
		stream2.Inlet() <- 4
		Expect(sd.ShutdownAfter(2 * time.Millisecond)).To(Succeed())
		count := 0
		for range single.Outlet() {
			count++
			if count == 4 {
				break
			}
		}
		Expect(count).To(Equal(4))
	})
})
