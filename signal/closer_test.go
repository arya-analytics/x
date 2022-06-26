package signal_test

import (
	"context"
	"github.com/arya-analytics/x/signal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Closer", func() {
	It("Should run the closers even if no goroutines are forked", func() {
		ctx, cancel := signal.WithCancel(context.Background())
		closed := 0
		ctx.AddCloser(func() error { closed++; return nil })
		cancel()
		Expect(closed).To(Equal(1))
	})
})
