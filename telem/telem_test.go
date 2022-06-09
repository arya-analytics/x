package telem_test

import (
	"github.com/arya-analytics/x/telem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Telem", func() {

	Describe("TimeStamp", func() {

		Describe("Now", func() {
			It("Should return the current time", func() {
				Expect(telem.Now().Time()).To(BeTemporally("~", time.Now(), time.Millisecond))
			})
		})

		Describe("New", func() {
			It("Should initialize a new timestamp based on the provided time", func() {
				t := time.Now()
				t0 := telem.NewTimeStamp(t)
				Expect(t0.Time()).To(BeTemporally("~", t, time.Millisecond))
			})
		})

		Describe("IsZero", func() {
			It("Should return true if the timestamp is zero", func() {
				Expect(telem.TimeStampMin.IsZero()).To(BeTrue())
				Expect(telem.TimeStampMax.IsZero()).To(BeFalse())
			})
		})

		Describe("After", func() {
			It("Should return true if the timestamp is after the provided one", func() {
				Expect(telem.TimeStampMin.After(telem.TimeStampMax)).To(BeFalse())
				Expect(telem.TimeStampMax.After(telem.TimeStampMin)).To(BeTrue())
			})
			It("Should return false if the timestamp is equal to the provided one", func() {
				Expect(telem.TimeStampMin.After(telem.TimeStampMin)).To(BeFalse())
				Expect(telem.TimeStampMax.After(telem.TimeStampMax)).To(BeFalse())
			})
		})

		Describe("Before", func() {
			It("Should return true if the timestamp is before the provided one", func() {
				Expect(telem.TimeStampMin.Before(telem.TimeStampMax)).To(BeTrue())
				Expect(telem.TimeStampMax.Before(telem.TimeStampMin)).To(BeFalse())
			})
			It("Should return false if the timestamp is equal to the provided one", func() {
				Expect(telem.TimeStampMin.Before(telem.TimeStampMin)).To(BeFalse())
				Expect(telem.TimeStampMax.Before(telem.TimeStampMax)).To(BeFalse())
			})
		})

		Describe("Add", func() {
			It("Should return a new timestamp with the provided timespan added to it", func() {
				t0 := telem.TimeStamp(0)
				t1 := t0.Add(telem.Second)
				Expect(t1).To(Equal(telem.TimeStamp(1 * telem.Second)))
			})
		})

		Describe("Sub", func() {
			It("Should return a new timestamp with the provided timespan subtracted from it", func() {
				t0 := telem.TimeStamp(0)
				t1 := t0.Sub(telem.Second)
				Expect(t1).To(Equal(telem.TimeStamp(-1 * telem.Second)))
			})
		})

		Describe("SpanRange", func() {
			It("Should return the correct time range", func() {
				t0 := telem.TimeStamp(0)
				r := t0.SpanRange(telem.Second)
				Expect(r.Start).To(Equal(t0))
				Expect(r.End).To(Equal(t0.Add(telem.Second)))
			})
		})

		Describe("Range", func() {
			It("Should return the correct time range", func() {
				t0 := telem.TimeStamp(0)
				t1 := t0.Add(telem.Second)
				r := t0.Range(t1)
				Expect(r.Start).To(Equal(t0))
				Expect(r.End).To(Equal(t1))
			})
		})

	})

	Describe("TimeRange", func() {

		Describe("Span", func() {
			It("Should return the correct time span", func() {
				tr := telem.TimeRange{
					Start: telem.TimeStamp(0),
					End:   telem.TimeStamp(telem.Second),
				}
				Expect(tr.Span()).To(Equal(telem.Second))
			})
		})

		Describe("IsZero", func() {
			It("Should return true if the time range is zero", func() {
				Expect(telem.TimeRangeMin.IsZero()).To(BeFalse())
				Expect(telem.TimeRangeMax.IsZero()).To(BeFalse())
				Expect(telem.TimeRangeZero.IsZero()).To(BeTrue())
			})
		})

		Describe("BoundBy", func() {
			It("Should bound the time range to the provided constraints", func() {
				tr := telem.TimeRange{
					Start: telem.TimeStamp(telem.Second),
					End:   telem.TimeStamp(telem.Second * 4),
				}
				bound := telem.TimeRange{
					Start: telem.TimeStamp(2 * telem.Second),
					End:   telem.TimeStamp(telem.Second * 3),
				}
				bounded := tr.BoundBy(bound)
				Expect(bounded.Start).To(Equal(bound.Start))
				Expect(bounded.End).To(Equal(bounded.End))
			})
		})

		Describe("ContainsStamp", func() {
			It("Should return true when the range contains the timestamp", func() {
				tr := telem.TimeStamp(0).SpanRange(5 * telem.Second)
				Expect(tr.ContainsStamp(telem.TimeStamp(4 * telem.Second))).To(BeTrue())
				By("Being inclusive at the lower bound")
				Expect(tr.ContainsStamp(telem.TimeStamp(0 * telem.Second))).To(BeTrue())
				By("Being exclusive at the upper bound")
				Expect(tr.ContainsStamp(telem.TimeStamp(5 * telem.Second))).To(BeFalse())
			})
		})

		Describe("OverlapsWith", func() {
			It("Should return true when the ranges overlap with one another", func() {
				tr := telem.TimeStamp(0).SpanRange(5 * telem.Second)
				Expect(tr.ContainsRange(telem.TimeStamp(1).SpanRange(2 * telem.Second))).To(BeTrue())
			})
			It("Should return false when the start of one range is the end of another", func() {
				tr := telem.TimeStamp(0).SpanRange(5 * telem.Second)
				tr2 := telem.TimeStamp(5 * telem.Second).SpanRange(5 * telem.Second)
				Expect(tr.ContainsRange(tr2)).To(BeFalse())
				Expect(tr2.ContainsRange(tr)).To(BeFalse())
			})
			It("Should return true if checked against itself", func() {
				tr := telem.TimeStamp(0).SpanRange(5 * telem.Second)
				Expect(tr.ContainsRange(tr))
			})
		})

	})

	Describe("TimeSpan", func() {
		Describe("Duration", func() {
			It("Should return the correct time span", func() {
				ts := telem.Second
				Expect(ts.Duration()).To(Equal(time.Second))
			})
		})
		Describe("Seconds", func() {
			It("Should return the correct number of seconds in the span", func() {
				ts := telem.Millisecond
				Expect(ts.Seconds()).To(Equal(0.001))
			})
		})
		Describe("IsZero", func() {
			It("Should return true if the time span is zero", func() {
				Expect(telem.TimeSpanMax.IsZero()).To(BeFalse())
				Expect(telem.TimeSpanZero.IsZero()).To(BeTrue())
			})
		})
		Describe("IsMax", func() {
			It("Should return true if the time span is the maximum", func() {
				Expect(telem.TimeSpanMax.IsMax()).To(BeTrue())
				Expect(telem.TimeSpanZero.IsMax()).To(BeFalse())
			})
		})
	})
	Describe("Size", func() {
		Describe("String", func() {
			It("Should return the correct string", func() {
				s := telem.Size(0)
				Expect(s.String()).To(Equal("0B"))
			})
		})
	})
	Describe("DataRate", func() {
		Describe("Period", func() {
			It("Should return the correct period for the data rate", func() {
				Expect(telem.DataRate(1).Period()).To(Equal(telem.Second))
			})
		})
		Describe("SampleCount", func() {
			It("Should return the number of samples that fit in the span", func() {
				Expect(telem.DataRate(10).SampleCount(telem.Second)).To(Equal(10))
			})
		})
		Describe("Span", func() {
			It("Should return the span of the provided samples", func() {
				Expect(telem.DataRate(10).Span(10)).To(Equal(telem.Second))
			})
		})
		Describe("SizeSpan", func() {
			It("Should return the span of the provided number of bytes", func() {
				Expect(telem.DataRate(10).SizeSpan(16, telem.Float64)).To(Equal(200 * telem.Millisecond))
			})
		})
	})
})
