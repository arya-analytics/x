package confluence

import (
	"github.com/arya-analytics/x/signal"
	"time"
)

func goRange[V Value](
	ctx signal.Context,
	outlet Outlet[V],
	f func(v V) error,
	opts ...FlowOption,
) {
	signal.GoRange(ctx, outlet.Outlet(), f, NewFlowOptions(opts).Signal...)
}

func goTick(ctx signal.Context, interval time.Duration, f func(time.Time) error, opts ...FlowOption) {
	signal.GoTick(ctx, interval, f, NewFlowOptions(opts).Signal...)
}

func goRangeEach[V Value](
	ctx signal.Context,
	outlets []Outlet[V],
	f func(v V) error,
	opts ...FlowOption,
) {
	for i, outlet := range outlets {
		outlet = outlet
		if i == 0 {
			goRange(ctx, outlet, func(v V) error { return f(v) }, opts...)
		} else {
			goRange(ctx, outlet, func(v V) error { return f(v) })
		}
	}
}

func selectAndSend[V Value](inlets []Inlet[V], v V) {
	for _, inlet := range inlets {
		select {
		case inlet.Inlet() <- v:
			return
		default:
		}
	}
}

func multiply[V Value](inlets []Inlet[V], v V) {
	for _, inlet := range inlets {
		inlet.Inlet() <- v
	}
}
