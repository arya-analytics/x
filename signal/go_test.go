package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Go", func() {
	Describe("GoRange", func() {
		It("Should range through the value of a channel before shutting down", func() {
			ch := make(chan int)
			c := signal.New(context.Background())
			resV := make([]int, 2)
			signal.GoRange(c, ch, func(v int) error {
				resV[v] = v
				return nil
			})
			ch <- 0
			ch <- 1
			close(ch)
			Expect(c.WaitOnAll()).To(BeNil())
			Expect(resV).To(Equal([]int{0, 1}))
		})
		It("Should shut down the routine when a non-nil error is returned", func() {
			ch := make(chan int)
			c := signal.New(context.Background())
			resV := make([]int, 2)
			signal.GoRange(c, ch, func(v int) error {
				if v == 1 {
					return errors.New("error")
				}
				resV[v] = v
				return nil
			})
			ch <- 0
			ch <- 1
			Expect(errors.Is(c.WaitOnAll(), errors.New("error"))).To(BeTrue())
		})
		It("Should shut down the routine when the context is cancelled", func() {
			ch := make(chan int, 3)
			ctx, cancel := context.WithCancel(context.Background())
			c := signal.New(ctx)
			resV := make(chan int, 2)
			signal.GoRange(c, ch, func(v int) error {
				resV <- v
				return nil
			})
			ch <- 0
			ch <- 1
			Expect(<-resV).To(Equal(0))
			Expect(<-resV).To(Equal(1))
			cancel()
			Expect(errors.Is(c.WaitOnAll(), context.Canceled)).To(BeTrue())
		})
	})
	Describe("GoTick", func() {
		It("Should execute the function every time the ticker fires", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1250*time.Microsecond)
			defer cancel()
			c := signal.New(ctx)
			count := 0
			signal.GoTick(c, 500*time.Microsecond, func(t time.Time) error {
				count++
				return nil
			})
			Expect(errors.Is(c.WaitOnAll(), context.DeadlineExceeded)).To(BeTrue())
			Expect(count).To(Equal(2))
		})
	})
})