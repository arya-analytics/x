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

type ungatedTranslator struct {
	flowCount int
}

func (s *ungatedTranslator) OutTo(inlets ...confluence.Inlet[int32]) {}

func (s *ungatedTranslator) InFrom(outlets ...confluence.Outlet[int]) {}

func (s *ungatedTranslator) Flow(ctx signal.Context, opts ...confluence.FlowOption) {
	s.flowCount++
}

var _ = Describe("Gated", func() {
	It("Should prevent a segment from flowing if the gate is closed", func() {
		s := &ungatedSegment{}
		gated := confluence.Gate[int](s)
		ctx, cancel := signal.Background()
		defer cancel()
		gated.Flow(ctx)
		Expect(s.flowCount).To(Equal(1))
		gated.Flow(ctx)
		Expect(s.flowCount).To(Equal(1))
		gated.Gate.Close()
		gated.Flow(ctx)
		Expect(s.flowCount).To(Equal(2))
	})
	Describe("GatedSource", func() {
		t := &ungatedTranslator{}
		gated := confluence.GateSource[int32](t)
		ctx, cancel := signal.Background()
		defer cancel()
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
		gated.Gate.Close()
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(2))
	})
	Describe("GatedSink", func() {
		t := &ungatedTranslator{}
		gated := confluence.GateSink[int](t)
		ctx, cancel := signal.Background()
		defer cancel()
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(1))
		gated.Gate.Close()
		gated.Flow(ctx)
		Expect(t.flowCount).To(Equal(2))
	})
})
