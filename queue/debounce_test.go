package queue_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/queue"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Debounce", func() {
	var (
		req confluence.Stream[[]int]
		res confluence.Stream[[]int]
		d   *queue.Debounce[int]
		ctx confluence.Context
	)
	BeforeEach(func() {
		d = &queue.Debounce[int]{
			Interval:  30 * time.Millisecond,
			Threshold: 15,
		}
		req = confluence.NewStream[[]int](10)
		res = confluence.NewStream[[]int](10)
		ctx = confluence.WrapContext()
		d.InFrom(req)
		d.OutTo(res)
		d.Flow(ctx)
	})
	It("Should flush the queue at a specified interval", func() {
		req.AcquireInlet() <- []int{1, 2, 3, 4, 5}
		req.AcquireInlet() <- []int{6, 7, 8, 9, 10}
		time.Sleep(50 * time.Millisecond)
		Expect(ctx.Shutdown.Shutdown()).To(Succeed())
		responses := <-res.Output()
		Expect(responses).To(Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
	})
	It("Should flush teh queue when the threshold is reached", func() {
		req.AcquireInlet() <- []int{1, 2, 3, 4, 5}
		req.AcquireInlet() <- []int{6, 7, 8, 9, 10}
		req.AcquireInlet() <- []int{11, 12, 13, 14, 15}
		Expect(ctx.Shutdown.Shutdown()).To(Succeed())
		responses := <-res.Output()
		Expect(responses).To(Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}))
	})
})
