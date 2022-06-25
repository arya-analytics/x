package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Signal", func() {
	Describe("Shutting Down", func() {
		It("Should close running goroutines when a context is cancelled", func() {
			ctx, cancel := context.WithCancel(context.Background())
			c := signal.New(ctx)
			c.Go(func(sig signal.Signal) error {
				<-sig.Done()
				return sig.Err()
			})
			cancel()
			Expect(c.WaitOnAll()).To(MatchError(context.Canceled))
		})
		It("Should prioritize a non-context error", func() {
			ctx, cancel := context.WithCancel(context.Background())
			c := signal.New(ctx)
			c.Go(func(sig signal.Signal) error {
				<-sig.Done()
				return sig.Err()
			})
			c.Go(func(sig signal.Signal) error {
				<-sig.Done()
				return errors.New("error")
			})
			cancel()
			Expect(errors.Is(c.WaitOnAll(), errors.New("error"))).To(BeTrue())
		})
	})
	Describe("WaitOnAny", func() {
		Context("allowNil is false", func() {
			It("Should return the first non-nil error routine to shutdown", func() {
				c := signal.New(context.Background())
				c.Go(func(sig signal.Signal) error { return nil })
				c.Go(func(sig signal.Signal) error {
					time.Sleep(500 * time.Microsecond)
					return errors.New("error")
				})
				Expect(errors.Is(c.WaitOnAny(false), errors.New("error"))).To(BeTrue())
			})
		})
		Context("allowNil is true", func() {
			It("Should return the first nil error routine to shutdown", func() {
				c := signal.New(context.Background())
				c.Go(func(sig signal.Signal) error { return nil })
				c.Go(func(sig signal.Signal) error {
					time.Sleep(500 * time.Microsecond)
					return errors.New("error")
				})
				Expect(c.WaitOnAny(true)).To(BeNil())
			})

		})

	})
})
