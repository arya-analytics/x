package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

func waitForCancel(ctx signal.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func errOut(ctx signal.Context) error {
	return errors.New("error")
}

var _ = Describe("Context", func() {
	It("Should close running goroutines when the context is cancelled", func() {
		sig, cancel := signal.TODO()
		sig.Go(waitForCancel, signal.WithKey("waitForCancel"))
		cancel()
		Expect(sig.Wait()).To(MatchError(context.Canceled))
	})
	It("Should close running goroutines when the context times out", func() {
		sig, cancel := signal.WithTimeout(context.TODO(), 500*time.Microsecond)
		defer cancel()
		sig.Go(waitForCancel)
		Expect(sig.Wait()).To(MatchError(context.DeadlineExceeded))
	})
	It("Should cancel the context when the first goroutine returns an error", func() {
		sig, cancel := signal.TODO()
		defer cancel()
		sig.Go(waitForCancel)
		sig.Go(errOut)
		Expect(sig.Wait()).To(MatchError("error"))
	})
	It("Should prevent another goroutine from being started when the context is cancelled", func() {
		sig, cancel := signal.TODO()
		c := 0
		cancel()
		sig.Go(func(ctx signal.Context) error {
			c++
			return nil
		})
		Expect(c).To(Equal(0))
	})
	It("Should close the Stopped channel when all goroutines exit", func() {
		sig, cancel := signal.TODO()
		defer cancel()
		sig.Go(func(ctx signal.Context) error {
			cancel()
			return nil
		})
		Expect(sig.Wait()).ToNot(HaveOccurred())
		Expect(sig.Stopped()).To(BeClosed())
	})
})
