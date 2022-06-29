package confluence

import "github.com/arya-analytics/x/signal"

type FlowOptions struct {
	Signal []signal.GoOption
}

func NewFlowOptions(opts []FlowOption) *FlowOptions {
	fo := &FlowOptions{}
	for _, opt := range opts {
		opt(fo)
	}
	return fo
}

type FlowOption func(fo *FlowOptions)

func Defer(f func()) FlowOption {
	return func(fo *FlowOptions) { fo.Signal = append(fo.Signal, signal.Defer(f)) }
}
