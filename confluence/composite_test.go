package confluence_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/shutdown"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Composite", func() {
	It("Should execute the composite correctly", func() {
		source := &confluence.PoolSource[int]{Values: []int{1, 2, 3}}
		router := &confluence.Router[int]{Route: func(i int) address.Address {
			if i%2 == 0 {
				return "single"
			} else {
				return "double"
			}
		}}
		square := &confluence.Transform[int]{Transform: func(i int) int { return i * i }}
		doubleSquare := &confluence.Transform[int]{Transform: func(i int) int { return i * i * 2 }}
		sink := &confluence.PoolSink[int]{}
		t := &confluence.Composite[int]{}
		t.SetSegment("inFrom", source)
		t.SetSegment("router", router)
		Expect(t.Route("inFrom", "router", 1)).To(Succeed())
		t.SetSegment("single", square)
		t.SetSegment("double", doubleSquare)
		Expect(t.Route("router", "double", 1)).To(Succeed())
		Expect(t.Route("router", "single", 1)).To(Succeed())
		t.SetSegment("outTo", sink)
		Expect(t.Route("double", "outTo", 1)).To(Succeed())
		Expect(t.Route("single", "outTo", 1)).To(Succeed())
		sd := shutdown.New()
		t.Flow(sd)
		Expect(sd.ShutdownAfter(1 * time.Millisecond)).To(Succeed())
		Expect(sink.Values).To(HaveLen(3))
	})
})
