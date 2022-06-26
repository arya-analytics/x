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
			sig := signal.New(ctx)
			Expect(sig.Count()).To(Equal(0))
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			Expect(sig.Count()).To(Equal(1))
			done := make(chan struct{})
			sig.Go(func() error {
				<-done
				return nil
			})
			Expect(sig.Count()).To(Equal(2))
			done <- struct{}{}
			Expect(sig.WaitOnAny(true)).To(BeNil())
			Expect(sig.Count()).To(Equal(1))
			cancel()
			Expect(errors.Is(sig.WaitOnAny(false), context.Canceled)).To(BeTrue())
			Expect(sig.Count()).To(Equal(0))
		})
	})
	Describe("GoCount", func() {
		It("Should return the total number of goroutines forked", func() {
			ctx, cancel := context.WithCancel(context.Background())
			sig := signal.New(ctx)
			Expect(sig.GoCount()).To(Equal(0))
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			Expect(sig.GoCount()).To(Equal(1))
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			cancel()
			Expect(errors.Is(sig.WaitOnAll(), context.Canceled)).To(BeTrue())
			Expect(sig.GoCount()).To(Equal(2))
		})
	})

})
