package confluence_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Delta", func() {
	var (
		inputOne  confluence.Stream[int]
		inputTwo  confluence.Stream[int]
		outputOne confluence.Stream[int]
		outputTwo confluence.Stream[int]
	)
	BeforeEach(func() {
		inputOne = confluence.NewStream[int](1)
		inputTwo = confluence.NewStream[int](1)
		outputOne = confluence.NewStream[int](0)
		outputOne.SetInletAddress("outputOne")
		outputTwo = confluence.NewStream[int](0)
		outputTwo.SetInletAddress("outputTwo")
	})
	Describe("DeltaMultiplier", func() {
		It("Should multiply input values to outputs", func() {
			delta := &confluence.DeltaMultiplier[int]{}
			delta.OutTo(outputOne, outputTwo)
			delta.InFrom(inputOne, inputTwo)
			ctx, cancel := signal.Background()
			defer cancel()
			delta.Flow(ctx)
			inputOne.Inlet() <- 1
			v1 := <-outputOne.Outlet()
			v2 := <-outputTwo.Outlet()
			Expect(v1).To(Equal(1))
			Expect(v2).To(Equal(1))
		})
		It("Should close inlets when the delta is closed", func() {
			delta := &confluence.DeltaMultiplier[int]{}
			delta.OutTo(outputOne)
			delta.InFrom(inputOne)
			ctx, cancel := signal.Background()
			defer cancel()
			delta.Flow(ctx, confluence.CloseInletsOnExit())
			inputOne.Inlet() <- 1
			inputOne.Close()
			v1 := <-outputOne.Outlet()
			Expect(v1).To(Equal(1))
			_, ok := <-outputOne.Outlet()
			Expect(ok).To(BeFalse())
		})
	})
	Describe("DeltaTransformMultiplier", func() {
		It("Should multiply input values to outputs", func() {
			delta := &confluence.DeltaTransformMultiplier[int, int]{}
			delta.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			delta.OutTo(outputOne, outputTwo)
			delta.InFrom(inputOne, inputTwo)
			ctx, cancel := signal.Background()
			defer cancel()
			delta.Flow(ctx)
			inputOne.Inlet() <- 1
			v1 := <-outputOne.Outlet()
			v2 := <-outputTwo.Outlet()
			Expect(v1).To(Equal(2))
			Expect(v2).To(Equal(2))
		})
		It("Should close inlets when the delta is closed", func() {
			delta := &confluence.DeltaTransformMultiplier[int, int]{}
			delta.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			delta.OutTo(outputOne)
			delta.InFrom(inputOne)
			ctx, cancel := signal.Background()
			defer cancel()
			delta.Flow(ctx, confluence.CloseInletsOnExit())
			inputOne.Inlet() <- 1
			inputOne.Close()
			v1 := <-outputOne.Outlet()
			Expect(v1).To(Equal(2))
			_, ok := <-outputOne.Outlet()
			Expect(ok).To(BeFalse())
		})
	})
})
