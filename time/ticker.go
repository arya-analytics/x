package time

import (
	"time"
)

type ScaledTicker struct {
	C     <-chan time.Duration
	Dur   time.Duration
	Scale float64
	stop  chan struct{}
}

func (s *ScaledTicker) Stop() {
	close(s.stop)
}

func NewScaledTicker(d time.Duration, scale float64) *ScaledTicker {
	c := make(chan time.Duration)
	s := make(chan struct{})
	t := &ScaledTicker{
		C:     c,
		Dur:   d,
		Scale: 1,
		stop:  s,
	}
	go func() {
		for {
			select {
			case <-s:
				return
			default:
				time.Sleep(t.Dur)
				t.Dur = time.Duration(float64(t.Dur) * scale)
				c <- t.Dur
			}

		}
	}()
	return t
}