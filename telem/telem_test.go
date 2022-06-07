package telem_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Telem", func() {
	Describe("TimeStamp", func() {
		Describe("Now", func() {
			It("Should return the current time", func() {
				Expect(cesium.Now().Time()).To(BeTemporally("~", time.Now(), time.Millisecond))
			})
		})
		Describe("New", func() {
			It("Should initialize a new timestamp based on the provided time", func() {
				t := time.Now()
				t0 := cesium.NewTimeStamp(t)
				Expect(t0.Time()).To(BeTemporally("~", t, time.Millisecond))
			})
		})
		Describe("IsZero", func() {
			It("Should return true if the timestamp is zero", func() {
				Expect(cesium.TimeStampMin.IsZero()).To(BeTrue())
				Expect(cesium.TimeStampMax.IsZero()).To(BeFalse())
			})
		})
		Describe("After", func() {
			It("Should return true if the timestamp is after the provided one", func() {
				Expect(cesium.TimeStampMin.After(cesium.TimeStampMax)).To(BeFalse())
				Expect(cesium.TimeStampMax.After(cesium.TimeStampMin)).To(BeTrue())
			})
			It("Should return false if the timestamp is equal to the provided one", func() {
				Expect(cesium.TimeStampMin.After(cesium.TimeStampMin)).To(BeFalse())
				Expect(cesium.TimeStampMax.After(cesium.TimeStampMax)).To(BeFalse())
			})
		})
		Describe("Before", func() {
			It("Should return true if the timestamp is before the provided one", func() {
				Expect(cesium.TimeStampMin.Before(cesium.TimeStampMax)).To(BeTrue())
				Expect(cesium.TimeStampMax.Before(cesium.TimeStampMin)).To(BeFalse())
			})
			It("Should return false if the timestamp is equal to the provided one", func() {
				Expect(cesium.TimeStampMin.Before(cesium.TimeStampMin)).To(BeFalse())
				Expect(cesium.TimeStampMax.Before(cesium.TimeStampMax)).To(BeFalse())
			})
		})
		Describe("Add", func() {
			It("Should return a new timestamp with the provided timespan added to it", func() {
				t0 := cesium.TimeStamp(0)
				t1 := t0.Add(cesium.Second)
				Expect(t1).To(Equal(cesium.TimeStamp(1 * cesium.Second)))
			})
		})
		Describe("Sub", func() {
			It("Should return a new timestamp with the provided timespan subtracted from it", func() {
				t0 := cesium.TimeStamp(0)
				t1 := t0.Sub(cesium.Second)
				Expect(t1).To(Equal(cesium.TimeStamp(-1 * cesium.Second)))
			})
		})
		Describe("SpanRange", func() {
			It("Should return the correct time range", func() {
				t0 := cesium.TimeStamp(0)
				r := t0.SpanRange(cesium.Second)
				Expect(r.Start).To(Equal(t0))
				Expect(r.End).To(Equal(t0.Add(cesium.Second)))
			})
		})
		Describe("Range", func() {
			It("Should return the correct time range", func() {
				t0 := cesium.TimeStamp(0)
				t1 := t0.Add(cesium.Second)
				r := t0.Range(t1)
				Expect(r.Start).To(Equal(t0))
				Expect(r.End).To(Equal(t1))
			})
		})
		Describe("String", func() {
			It("Should return the correct string", func() {
				t0 := cesium.TimeStamp(0)
				Expect(t0.String()).To(Equal("1969-12-31 16:00:00 -0800 PST"))
			})
		})
	})
	Describe("TimeRange", func() {
		Describe("Span", func() {
			It("Should return the correct time span", func() {
				tr := cesium.TimeRange{
					Start: cesium.TimeStamp(0),
					End:   cesium.TimeStamp(cesium.Second),
				}
				Expect(tr.Span()).To(Equal(cesium.Second))
			})
		})
		Describe("IsZero", func() {
			It("Should return true if the time range is zero", func() {
				Expect(cesium.TimeRangeMin.IsZero()).To(BeFalse())
				Expect(cesium.TimeRangeMax.IsZero()).To(BeFalse())
				Expect(cesium.TimeRangeZero.IsZero()).To(BeTrue())
			})
		})
		Describe("Bound", func() {
			It("Should bound the time range to the provided constraints", func() {
				tr := cesium.TimeRange{
					Start: cesium.TimeStamp(time.Second),
					End:   cesium.TimeStamp(time.Second * 4),
				}
				bound := cesium.TimeRange{
					Start: cesium.TimeStamp(2 * time.Second),
					End:   cesium.TimeStamp(time.Second * 3),
				}
				bounded := tr.Bound(bound)
				Expect(bounded.Start).To(Equal(bound.Start))
				Expect(bounded.End).To(Equal(bounded.End))
			})
		})
	})
	Describe("TimeSpan", func() {
		Describe("Duration", func() {
			It("Should return the correct time span", func() {
				ts := cesium.Second
				Expect(ts.Duration()).To(Equal(time.Second))
			})
		})
		Describe("Seconds", func() {
			It("Should return the correct number of seconds in the span", func() {
				ts := cesium.Millisecond
				Expect(ts.Seconds()).To(Equal(0.001))
			})
		})
		Describe("IsZero", func() {
			It("Should return true if the time span is zero", func() {
				Expect(cesium.TimeSpanMax.IsZero()).To(BeFalse())
				Expect(cesium.TimeSpanZero.IsZero()).To(BeTrue())
			})
		})
		Describe("IsMax", func() {
			It("Should return true if the time span is the maximum", func() {
				Expect(cesium.TimeSpanMax.IsMax()).To(BeTrue())
				Expect(cesium.TimeSpanZero.IsMax()).To(BeFalse())
			})
		})
	})
	Describe("Size", func() {
		Describe("String", func() {
			It("Should return the correct string", func() {
				s := cesium.Size(0)
				Expect(s.String()).To(Equal("0B"))
			})
		})
	})
	Describe("DataRate", func() {
		Describe("Period", func() {
			It("Should return the correct period for the data rate", func() {
				Expect(cesium.DataRate(1).Period()).To(Equal(cesium.Second))
			})
		})
		Describe("SampleCount", func() {
			It("Should return the number of samples that fit in the span", func() {
				Expect(cesium.DataRate(10).SampleCount(cesium.Second)).To(Equal(10))
			})
		})
		Describe("Span", func() {
			It("Should return the span of the provided samples", func() {
				Expect(cesium.DataRate(10).Span(10)).To(Equal(cesium.Second))
			})
		})
		Describe("SizeSpan", func() {
			It("Should return the span of the provided number of bytes", func() {
				Expect(cesium.DataRate(10).ByteSpan(16, cesium.Float64)).To(Equal(200 * cesium.Millisecond))
			})
		})
	})
})
