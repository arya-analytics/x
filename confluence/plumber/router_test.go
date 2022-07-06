package plumber_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/confluence/plumber"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func parseOutletAddrMap[V confluence.Value](
	outlets []confluence.Outlet[V],
) map[address.Address]confluence.Outlet[V] {
	outletAddrMap := make(map[address.Address]confluence.Outlet[V])
	for _, outlet := range outlets {
		outletAddrMap[outlet.OutletAddress()] = outlet
	}
	return outletAddrMap
}

var _ = Describe("Router", func() {
	var p *plumber.Pipeline
	BeforeEach(func() { p = plumber.New() })

	Describe("UnaryRouter", func() {
		It("Should establish a channel between two addresses", func() {
			source := &confluence.Emitter[int]{}
			sink := &confluence.UnarySink[int]{}
			plumber.SetSource[int](p, "source", source)
			plumber.SetSink[int](p, "sink", sink)
			router := &plumber.UnaryRouter[int]{
				SourceTarget: "source",
				SinkTarget:   "sink",
				Capacity:     1,
			}
			Expect(router.Route(p)).To(Succeed())
			source.Out.Inlet() <- 1
			Expect(sink.In.Outlet()).To(Receive(Equal(1)))
		})
	})

	Describe("MultiRouter", func() {
		Describe("StitchUnary", func() {
			It("Should wire a single channel between multiple addresses", func() {
				source := &confluence.Emitter[int]{}
				sinkOne := &confluence.UnarySink[int]{}
				sinkTwo := &confluence.UnarySink[int]{}
				plumber.SetSource[int](p, "source", source)
				plumber.SetSink[int](p, "sinkOne", sinkOne)
				plumber.SetSink[int](p, "sinkTwo", sinkTwo)
				router := &plumber.MultiRouter[int]{
					SourceTargets: []address.Address{"source"},
					SinkTargets:   []address.Address{"sinkOne", "sinkTwo"},
					Stitch:        plumber.StitchUnary,
					Capacity:      1,
				}
				Expect(router.Route(p)).To(Succeed())
				source.Out.Inlet() <- 1
				Expect(sinkOne.In.Outlet()).To(Receive(Equal(1)))
				source.Out.Inlet() <- 1
				Expect(sinkTwo.In.Outlet()).To(Receive(Equal(1)))
			})
		})

		Describe("StitchWeave", func() {
			It("Should wire a separate channel for each address pair", func() {
				sourceOne := &confluence.Switch[int]{}
				sourceTwo := &confluence.Switch[int]{}
				sinkOne := &confluence.MultiSink[int]{}
				sinkTwo := &confluence.MultiSink[int]{}
				plumber.SetSource[int](p, "sourceOne", sourceOne)
				plumber.SetSource[int](p, "sourceTwo", sourceTwo)
				plumber.SetSink[int](p, "sinkOne", sinkOne)
				plumber.SetSink[int](p, "sinkTwo", sinkTwo)
				router := &plumber.MultiRouter[int]{
					SourceTargets: []address.Address{"sourceOne", "sourceTwo"},
					SinkTargets:   []address.Address{"sinkOne", "sinkTwo"},
					Stitch:        plumber.StitchWeave,
					Capacity:      1,
				}
				Expect(router.Route(p)).To(Succeed())
				sinkOneOutlets := parseOutletAddrMap(sinkOne.In)
				sinkTwoOutlets := parseOutletAddrMap(sinkTwo.In)
				sourceOne.Out["sinkOne"].Inlet() <- 1
				Expect(sinkOneOutlets["sourceOne"].Outlet()).To(Receive(Equal(1)))
				sourceTwo.Out["sinkOne"].Inlet() <- 1
				Expect(sinkOneOutlets["sourceTwo"].Outlet()).To(Receive(Equal(1)))
				sourceOne.Out["sinkTwo"].Inlet() <- 1
				Expect(sinkTwoOutlets["sourceOne"].Outlet()).To(Receive(Equal(1)))
				sourceTwo.Out["sinkTwo"].Inlet() <- 1
				Expect(sinkTwoOutlets["sourceTwo"].Outlet()).To(Receive(Equal(1)))
			})
		})

		Describe("StitchDivergent", func() {
			It("Should wire a separate for each source", func() {
				sourceOne := &confluence.Emitter[int]{}
				sourceTwo := &confluence.Emitter[int]{}
				sinkOne := &confluence.MultiSink[int]{}
				sinkTwo := &confluence.MultiSink[int]{}
				plumber.SetSource[int](p, "sourceOne", sourceOne)
				plumber.SetSource[int](p, "sourceTwo", sourceTwo)
				plumber.SetSink[int](p, "sinkOne", sinkOne)
				plumber.SetSink[int](p, "sinkTwo", sinkTwo)
				router := &plumber.MultiRouter[int]{
					SourceTargets: []address.Address{"sourceOne", "sourceTwo"},
					SinkTargets:   []address.Address{"sinkOne", "sinkTwo"},
					Stitch:        plumber.StitchDivergent,
					Capacity:      1,
				}
				Expect(router.Route(p)).To(Succeed())
				sinkOneOutlets := parseOutletAddrMap(sinkOne.In)
				sinkTwoOutlets := parseOutletAddrMap(sinkTwo.In)
				sourceOne.Out.Inlet() <- 1
				Expect(sinkOneOutlets["sourceOne"].Outlet()).To(Receive(Equal(1)))
				sourceOne.Out.Inlet() <- 1
				Expect(sinkTwoOutlets["sourceOne"].Outlet()).To(Receive(Equal(1)))
				sourceTwo.Out.Inlet() <- 1
				Expect(sinkOneOutlets["sourceTwo"].Outlet()).To(Receive(Equal(1)))
				sourceTwo.Out.Inlet() <- 1
				Expect(sinkTwoOutlets["sourceTwo"].Outlet()).To(Receive(Equal(1)))
			})
		})

		Describe("StitchConvergent", func() {
			It("Should wire a separate channel for each sink", func() {

				sourceOne := &confluence.Switch[int]{}
				sourceTwo := &confluence.Switch[int]{}
				sinkOne := &confluence.UnarySink[int]{}
				sinkTwo := &confluence.UnarySink[int]{}
				plumber.SetSource[int](p, "sourceOne", sourceOne)
				plumber.SetSource[int](p, "sourceTwo", sourceTwo)
				plumber.SetSink[int](p, "sinkOne", sinkOne)
				plumber.SetSink[int](p, "sinkTwo", sinkTwo)
				router := &plumber.MultiRouter[int]{
					SourceTargets: []address.Address{"sourceOne", "sourceTwo"},
					SinkTargets:   []address.Address{"sinkOne", "sinkTwo"},
					Stitch:        plumber.StitchConvergent,
					Capacity:      1,
				}
				Expect(router.Route(p)).To(Succeed())
				sourceOne.Out["sinkOne"].Inlet() <- 1
				Expect(sinkOne.In.Outlet()).To(Receive(Equal(1)))
				sourceOne.Out["sinkTwo"].Inlet() <- 1
				Expect(sinkTwo.In.Outlet()).To(Receive(Equal(1)))
				sourceTwo.Out["sinkOne"].Inlet() <- 1
				Expect(sinkOne.In.Outlet()).To(Receive(Equal(1)))
				sourceTwo.Out["sinkTwo"].Inlet() <- 1
				Expect(sinkTwo.In.Outlet()).To(Receive(Equal(1)))

			})
		})
	})
})
