package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Conductor", func() {
	Describe("Shutting Down", func() {
		It("Should close running goroutines when a context is cancelled", func() {
			sig, cancel := signal.WithCancel(context.Background())
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			cancel()
			Expect(sig.WaitOnAll()).To(MatchError(context.Canceled))
		})
		It("Should prioritize a non-context error", func() {
			sig, cancel := signal.WithCancel(context.Background())
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			sig.Go(func() error {
				<-sig.Done()
				return errors.New("error")
			})
			cancel()
			Expect(errors.Is(sig.WaitOnAll(), errors.New("error"))).To(BeTrue())
		})
		It("Should allow all goroutines to exit even if a wg method is not called", func() {
			sig, cancel := signal.WithCancel(context.Background())
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			sig.Go(func() error {
				<-sig.Done()
				return sig.Err()
			})
			Expect(sig.NumRunning()).To(Equal(int32(2)))
			cancel()
			time.Sleep(1 * time.Millisecond)
			Expect(sig.NumRunning()).To(Equal(int32(0)))
		})
	})
	Describe("WaitOnAny", func() {
		Context("allowNil is false", func() {
			It("Should return the first non-nil error routine to shutdown", func() {
				c, cancel := signal.WithCancel(context.Background())
				defer cancel()
				c.Go(func() error { return nil })
				c.Go(func() error {
					time.Sleep(500 * time.Microsecond)
					return errors.New("error")
				})
				Expect(errors.Is(c.WaitOnAny(false), errors.New("error"))).To(BeTrue())
			})
		})
		Context("allowNil is true", func() {
			It("Should return the first nil error routine to shutdown", func() {
				c, cancel := signal.WithCancel(context.Background())
				defer cancel()
				c.Go(func() error { return nil })
				c.Go(func() error {
					time.Sleep(500 * time.Microsecond)
					return errors.New("error")
				})
				Expect(c.WaitOnAny(true)).To(BeNil())
			})

		})
	})
})
