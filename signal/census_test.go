package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Census", func() {
	Describe("Count", func() {
		It("Should return the number of running goroutines", func() {
			ctx, cancel := context.WithCancel(context.Background())
			c := signal.New(ctx)
			Expect(c.Count()).To(Equal(0))
			c.Go(func(sig signal.Signal) error {
				<-sig.Done()
				return sig.Err()
			})
			Expect(c.Count()).To(Equal(1))
			done := make(chan struct{})
			c.Go(func(sig signal.Signal) error {
				<-done
				return nil
			})
			Expect(c.Count()).To(Equal(2))
			done <- struct{}{}
			Expect(c.WaitOnAny(true)).To(BeNil())
			Expect(c.Count()).To(Equal(1))
			cancel()
			Expect(errors.Is(c.WaitOnAny(false), context.Canceled)).To(BeTrue())
			Expect(c.Count()).To(Equal(0))
		})
	})
	Describe("GoCount", func() {
		It("Should return the total number of goroutines forked", func() {
			ctx, cancel := context.WithCancel(context.Background())
			c := signal.New(ctx)
			Expect(c.GoCount()).To(Equal(0))
			c.Go(func(sig signal.Signal) error {
				<-sig.Done()
				return sig.Err()
			})
			Expect(c.GoCount()).To(Equal(1))
			c.Go(func(sig signal.Signal) error {
				<-sig.Done()
				return sig.Err()
			})
			cancel()
			Expect(errors.Is(c.WaitOnAll(), context.Canceled)).To(BeTrue())
			Expect(c.GoCount()).To(Equal(2))
		})
	})

})
