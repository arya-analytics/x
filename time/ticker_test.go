package time_test

import (
	xtime "github.com/arya-analytics/x/time"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Ticker", func() {
	Describe("ScaledTicker", func() {
		It("Should scale the duration between ticks", func() {
			t := xtime.NewScaledTicker(1*time.Millisecond, 2)
			t0 := time.Now()
			Expect(<-t.C).To(Equal(1 * time.Millisecond))
			Expect(<-t.C).To(Equal(2 * time.Millisecond))
			te := time.Since(t0)
			Expect(te).To(BeNumerically("<", 4*time.Millisecond))
			Expect(te).To(BeNumerically(">", 3*time.Millisecond))
			t.Stop()
		})
	})
})
