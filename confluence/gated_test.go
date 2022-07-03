package confluence_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type ungatedSegment struct {
	flowCount int
}

func (s *ungatedSegment) OutTo(inlets ...confluence.Inlet[int]) {}

func (s *ungatedSegment) InFrom(outlets ...confluence.Outlet[int]) {}

func (s *ungatedSegment) Flow(ctx signal.Context, opts ...confluence.FlowOption) {
	s.flowCount++
}

var _ = Describe("Gated", func() {
	It("Should prevent a segment from flowing if the gate is closed", func() {
		s := &ungatedSegment{}
		gated := confluence.Gate[int, int](s)
		ctx, cancel := signal.Background()
		defer cancel()
		gated.Flow(ctx)
		Expect(s.flowCount).To(Equal(1))
		gated.Flow(ctx)
		Expect(s.flowCount).To(Equal(1))
	})
	Describe("gatedSource", func() {
		t := &ungatedSegment{}
		gated := confluence.GateSource[int](t)
		ctx, cancel := signal.Background()
		defer cancel()
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
	})
	Describe("gatedSink", func() {
		t := &ungatedSegment{}
		gated := confluence.GateSink[int](t)
		ctx, cancel := signal.Background()
		defer cancel()
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
	})
})
