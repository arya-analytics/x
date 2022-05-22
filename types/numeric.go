package types

import "time"

// Numeric represents a generic numeric value.
type Numeric interface {
	int | float64 | float32 | int64 | int32 | int16 | int8 | uint64 | ~uint32 | uint16 | uint8 | time.Duration
}
