package plumber_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/confluence/plumber"
	"github.com/arya-analytics/x/errutil"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Pipeline", func() {
	var pipe *plumber.Pipeline
	BeforeEach(func() { pipe = plumber.New() })
	Describe("Basic Usage", func() {
		It("Should set and get a source", func() {
			emitter := &confluence.Emitter[int]{}
			plumber.SetSource[int](pipe, "source", emitter)
			source, err := plumber.GetSource[int](pipe, "source")
			Expect(err).ToNot(HaveOccurred())
			Expect(source).To(Equal(emitter))
		})

		It("Should set and get a sink", func() {
			unarySink := &confluence.UnarySink[int]{}
			plumber.SetSink[int](pipe, "sink", unarySink)
			sink, err := plumber.GetSink[int](pipe, "sink")
			Expect(err).ToNot(HaveOccurred())
			Expect(sink).To(Equal(unarySink))
		})

		It("Should set and get a segment", func() {
			trans := &confluence.LinearTransform[int, int]{}
			plumber.SetSegment[int, int](pipe, "segment", trans)
			segment, err := plumber.GetSegment[int, int](pipe, "segment")
			Expect(err).ToNot(HaveOccurred())
			Expect(segment).To(Equal(segment))
		})

	})

	Describe("Shutdown Chain", func() {

		It("Should shutdown the pipe as segments close their inlets", func() {
			t1 := &confluence.LinearTransform[int, int]{}
			t1.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			plumber.SetSegment[int, int](pipe, "t1", t1, confluence.CloseInletsOnExit())

			t2 := &confluence.LinearTransform[int, int]{}
			t2.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			plumber.SetSegment[int, int](pipe, "t2", t2, confluence.CloseInletsOnExit())

			Expect(plumber.UnaryRouter[int]{
				SourceTarget: "t1",
				SinkTarget:   "t2",
			}.Route(pipe))

			seg := &plumber.Segment[int, int]{Pipeline: pipe}
			Expect(seg.RouteInletTo("t1")).To(Succeed())
			Expect(seg.RouteOutletFrom("t2")).To(Succeed())

			input := confluence.NewStream[int](1)
			output := confluence.NewStream[int](0)
			seg.InFrom(input)
			seg.OutTo(output)

			ctx, cancel := signal.TODO()
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

	Describe("Complex Pipeline", func() {

		It("Should construct and operate the pipe correctly", func() {
			emitterOne := &confluence.Emitter[int]{Interval: 1 * time.Millisecond}
			c1 := 0
			emitterOne.Emit = func(ctx signal.Context) (int, error) {
				c1++
				if c1 == 5 {
					return 0, errors.New("done counting")
				}
				return c1, nil
			}
			plumber.SetSource[int](pipe, "emitterOne", emitterOne)

			emitterTwo := &confluence.Emitter[int]{Interval: 1 * time.Millisecond}
			c2 := 0
			emitterTwo.Emit = func(ctx signal.Context) (int, error) {
				c2++
				if c2 == 6 {
					return 0, errors.New("done counting")
				}
				return c2, nil
			}
			plumber.SetSource[int](pipe, "emitterTwo", emitterTwo)

			t1 := &confluence.LinearTransform[int, int]{}
			t1.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 2, true, nil
			}
			plumber.SetSegment[int, int](pipe, "t1", t1)

			t2 := &confluence.LinearTransform[int, int]{}
			t2.ApplyTransform = func(ctx signal.Context, v int) (int, bool, error) {
				return v * 3, true, nil
			}
			plumber.SetSegment[int, int](pipe, "t2", t2)

			var (
				evens []int
				odds  []int
			)

			evenSink := &confluence.UnarySink[int]{}
			evenSink.Sink = func(ctx signal.Context, v int) error {
				evens = append(evens, v)
				return nil
			}
			plumber.SetSink[int](pipe, "even", evenSink)

			oddSink := &confluence.UnarySink[int]{}
			oddSink.Sink = func(ctx signal.Context, v int) error {
				odds = append(odds, v)
				return nil
			}
			plumber.SetSink[int](pipe, "odd", oddSink)

			sw := &confluence.Switch[int]{}
			sw.ApplySwitch = func(ctx signal.Context, v int) (address.Address, bool, error) {
				if v%2 == 0 {
					return "even", true, nil
				}
				return "odd", true, nil
			}
			plumber.SetSegment[int, int](pipe, "switch", sw)

			catch := errutil.NewCatchSimple()

			catch.Exec(plumber.MultiRouter[int]{
				SourceTargets: []address.Address{"emitterOne", "emitterTwo"},
				SinkTargets:   []address.Address{"t1", "t2"},
				Stitch:        plumber.StitchUnary,
			}.PreRoute(pipe))

			catch.Exec(plumber.MultiRouter[int]{
				SourceTargets: []address.Address{"t1", "t2"},
				SinkTargets:   []address.Address{"switch"},
				Stitch:        plumber.StitchUnary,
			}.PreRoute(pipe))

			catch.Exec(plumber.MultiRouter[int]{
				SourceTargets: []address.Address{"switch"},
				SinkTargets:   []address.Address{"even", "odd"},
				Stitch:        plumber.StitchWeave,
			}.PreRoute(pipe))

			if catch.Error() != nil {
				Fail("Failed to construct pipeline: " + catch.Error().Error())
			}

			ctx, cancel := signal.TODO()
			defer cancel()
			pipe.Flow(ctx, confluence.CloseInletsOnExit())

			Expect(ctx.Wait()).To(MatchError("done counting"))

			Expect(len(evens) + len(odds)).To(Equal(9))
		})

	})

})
