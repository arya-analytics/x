package time

import (
	"time"
)

// ScaledTicker is a ticker that scales the duration between ticks.
// It provides an identical interface to a time.Ticker.
type ScaledTicker struct {
	C     <-chan time.Duration
	Dur   time.Duration
	Scale float64
	stop  chan struct{}
}

// Stop stops the ticker
func (s *ScaledTicker) Stop() { close(s.stop) }

func (s *ScaledTicker) tick(c chan time.Duration) {
	for {
		t := time.NewTimer(s.Dur)
		select {
		case <-s.stop:
			return
		case <-t.C:
			c <- s.Dur
			s.Dur = time.Duration(float64(s.Dur) * s.Scale)
		}
	}
}

// NewScaledTicker returns a new ScaledTicker that ticks at the given duration and scale.
func NewScaledTicker(d time.Duration, scale float64) *ScaledTicker {
	c := make(chan time.Duration)
	t := &ScaledTicker{Dur: d, Scale: scale, stop: make(chan struct{}), C: c}
	go t.tick(c)
	return t
}
