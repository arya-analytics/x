package plumber_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/confluence/plumber"
	"github.com/arya-analytics/x/signal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipeline", func() {
	var pipeline *plumber.Pipeline
	BeforeEach(func() { pipeline = plumber.New() })
	Describe("Basic Usage", func() {
		It("Should set and get a source", func() {
			emitter := &confluence.Emitter[int]{}
			plumber.SetSource[int](pipeline, "source", emitter)
			source, err := plumber.GetSource[int](pipeline, "source")
			Expect(err).ToNot(HaveOccurred())
			Expect(source).To(Equal(emitter))
		})

		It("Should set and get a sink", func() {
			unarySink := &confluence.UnarySink[int]{}
			plumber.SetSink[int](pipeline, "sink", unarySink)
			sink, err := plumber.GetSink[int](pipeline, "sink")
			Expect(err).ToNot(HaveOccurred())
			Expect(sink).To(Equal(unarySink))
		})

		It("Should set and get a segment", func() {
			trans := &confluence.LinearTransform[int, int]{}
			plumber.SetSegment[int, int](pipeline, "segment", trans)
			segment, err := plumber.GetSegment[int, int](pipeline, "segment")
			Expect(err).ToNot(HaveOccurred())
			Expect(segment).To(Equal(segment))
		})

	})

	Describe("Shutdown Chain", func() {

		It("Should shutdown the pipeline as segments close their inlets", func() {
			t1 := &confluence.LinearTransform[int, int]{}
			t1.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			plumber.SetSegment[int, int](pipeline, "t1", t1, confluence.CloseInletsOnExit())

			t2 := &confluence.LinearTransform[int, int]{}
			t2.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			plumber.SetSegment[int, int](pipeline, "t2", t2, confluence.CloseInletsOnExit())

			Expect(plumber.UnaryRouter[int]{
				SourceTarget: "t1",
				SinkTarget:   "t2",
			}.Route(pipeline))

			seg := &plumber.Segment[int, int]{Pipeline: pipeline}
			Expect(seg.RouteInletTo("t1")).To(Succeed())
			Expect(seg.RouteOutletFrom("t2")).To(Succeed())

			input := confluence.NewStream[int](1)
			output := confluence.NewStream[int](0)
			seg.InFrom(input)
			seg.OutTo(output)

			ctx, cancel := signal.Background()
			defer cancel()
			seg.Flow(ctx)

			Expect(ctx.NumRunning()).To(Equal(int32(2)))

			input.Inlet() <- 1
			input.Close()
			v := <-output.Outlet()
			Expect(v).To(Equal(4))
			_, ok := <-output.Outlet()
			Expect(ok).To(BeFalse())

			Expect(ctx.NumRunning()).To(Equal(int32(0)))
		})

	})
})
