package plumber_test

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/confluence/plumber"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipeline", func() {
	Describe("Basic Usage", func() {
		var pipeline *plumber.Pipeline
		BeforeEach(func() { pipeline = plumber.New() })

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
})
