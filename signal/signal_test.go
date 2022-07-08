package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
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
			Expect(sig.Wait()).To(MatchError(context.Canceled))
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
})
