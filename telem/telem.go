package telem

import (
	"strconv"
	"time"
)

// |||||| TIME STAMP ||||||

const (
	// TimeStampMin represents the minimum value for a TimeStamp
	TimeStampMin = TimeStamp(0)
	// TimeStampMax represents the maximum value for a TimeStamp
	TimeStampMax = TimeStamp(^uint64(0) >> 1)
)

// TimeStamp stores an epoch time in nanoseconds.
type TimeStamp int64

// Now returns the current time as a TimeStamp.
func Now() TimeStamp { return NewTimeStamp(time.Now()) }

// NewTimeStamp creates a new TimeStamp from a time.Time.
func NewTimeStamp(t time.Time) TimeStamp { return TimeStamp(t.UnixNano()) }

// Time returns the time.Time representation of the TimeStamp.
func (ts TimeStamp) Time() time.Time { return time.Unix(0, int64(ts)) }

// IsZero returns true if the TimeStamp is TimeStampMin.
func (ts TimeStamp) IsZero() bool { return ts == TimeStampMin }

// After returns true if the TimeStamp is greater than the provided one.
func (ts TimeStamp) After(t TimeStamp) bool { return ts > t }

// Before returns true if the TimeStamp is less than the provided one.
func (ts TimeStamp) Before(t TimeStamp) bool { return ts < t }

// Add returns a new TimeStamp with the provided TimeSpan added to it.
func (ts TimeStamp) Add(tspan TimeSpan) TimeStamp { return TimeStamp(int64(ts) + int64(tspan)) }

// Sub returns a new TimeStamp with the provided TimeSpan subtracted from it.
func (ts TimeStamp) Sub(tspan TimeSpan) TimeStamp { return TimeStamp(int64(ts) - int64(tspan)) }

// SpanRange constructs a new TimeRange with the TimeStamp and provided TimeSpan.
func (ts TimeStamp) SpanRange(span TimeSpan) TimeRange { return ts.Range(ts.Add(span)) }

// Range constructs a new TimeRange with the TimeStamp and provided TimeStamp.
func (ts TimeStamp) Range(ts2 TimeStamp) TimeRange { return TimeRange{ts, ts2} }

// String implements fmt.Stringer.
func (ts TimeStamp) String() string { return ts.Time().String() }

// |||||| TIME RANGE ||||||

// TimeRange represents a range of time between two TimeStamp.
type TimeRange struct {
	// Start is the start of the range.
	Start TimeStamp
	// End is the end of the range.
	End TimeStamp
}

// Span returns the TimeSpan that the TimeRange occupies.
func (tr TimeRange) Span() TimeSpan { return TimeSpan(tr.End - tr.Start) }

// IsZero returns true if the TimeSpan of TimeRange is empty.
func (tr TimeRange) IsZero() bool { return tr.Span().IsZero() }

// Bound limits the time range to the provided bounds.
func (tr TimeRange) Bound(otr TimeRange) TimeRange {
	if otr.Start.After(tr.Start) {
		tr.Start = otr.Start
	}
	if otr.End.Before(tr.End) {
		tr.End = otr.End
	}
	return tr
}

var (
	// TimeRangeMax represents the maximum possible value for a TimeRange.
	TimeRangeMax = TimeRange{Start: TimeStampMin, End: TimeStampMax}
	// TimeRangeMin represents the minimum possible value for a TimeRange.
	TimeRangeMin = TimeRange{Start: TimeStampMax, End: TimeStampMin}
	// TimeRangeZero represents the zero value for a TimeRange.
	TimeRangeZero = TimeRange{Start: TimeStampMin, End: TimeStampMin}
)

// |||||| TIME SPAN ||||||

// TimeSpan represents a duration of time in nanoseconds.
type TimeSpan int64

const (
	// TimeSpanZero represents the zero value for a TimeSpan.
	TimeSpanZero = TimeSpan(0)
	// TimeSpanMax represents the maximum possible TimeSpan.
	TimeSpanMax = TimeSpan(^uint64(0) >> 1)
)

// Duration converts TimeSpan to a values.Duration.
func (ts TimeSpan) Duration() time.Duration { return time.Duration(ts) }

// Seconds returns a float64 value representing the number of seconds in the TimeSpan.
func (ts TimeSpan) Seconds() float64 { return ts.Duration().Seconds() }

// IsZero returns true if the TimeSpan is TimeSpanZero.
func (ts TimeSpan) IsZero() bool { return ts == TimeSpanZero }

// IsMax returns true if the TimeSpan is the maximum possible value.
func (ts TimeSpan) IsMax() bool { return ts == TimeSpanMax }

func (ts TimeSpan) ByteSize(dataRate DataRate, dataType DataType) Size {
	return Size(ts / dataRate.Period() * TimeSpan(dataType))
}

const (
	Nanosecond  = TimeSpan(1)
	Microsecond = 1000 * Nanosecond
	Millisecond = 1000 * Microsecond
	Second      = 1000 * Millisecond
	Minute      = 60 * Second
	Hour        = 60 * Minute
)

// |||||| SIZE ||||||

// Size represents the size of an element in bytes.
type Size uint64

const Kilobytes Size = 1024

// String implements fmt.Stringer.
func (s Size) String() string { return strconv.Itoa(int(s)) + "B" }

// |||||| DATA RATE ||||||

// DataRate represents a data rate in Hz.
type DataRate float64

// Period returns a TimeSpan representing the period of the DataRate.
func (dr DataRate) Period() TimeSpan { return TimeSpan(1 / float64(dr) * float64(Second)) }

// SampleCount returns n integer representing the number of samples in the provided Span.
func (dr DataRate) SampleCount(t TimeSpan) int { return int(t.Seconds() * float64(dr)) }

// Span returns a TimeSpan representing the number of samples that occupy the provided Span.
func (dr DataRate) Span(sampleCount int) TimeSpan { return dr.Period() * TimeSpan(sampleCount) }

// SizeSpan returns a TimeSpan representing the number of samples that occupy a provided number of bytes.
func (dr DataRate) SizeSpan(size Size, dataType Density) TimeSpan {
	return dr.Span(int(size) / int(dataType))
}

// Hz represents a data rate of 1 Hz.
const Hz DataRate = 1

// |||||| DENSITY ||||||

type (
	// Density represents the density of a data type in bytes per sample.
	Density uint16
	// DataType is an alias for Density. It's mostly to provide more semantic naming in certain use cases.
	DataType = Density
)

const (
	Float64 Density = 8
	Int64   Density = 8
	Uint64  Density = 8
	Float32 Density = 4
	Int32   Density = 4
	Uint32  Density = 4
	Int16   Density = 2
	Uint16  Density = 2
	Int8    Density = 1
	Uint8   Density = 1
)
