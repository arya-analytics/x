package confluence

import "github.com/arya-analytics/x/signal"

type flowOptions struct {
	signal []signal.GoOption
}

func newFlowOptions(opts []FlowOption) flowOptions {
	fo := flowOptions{}
	for _, opt := range opts {
		opt(&fo)
	}
	return fo
}

type FlowOption func(fo *flowOptions)

func Defer(f func()) FlowOption {
	return func(fo *flowOptions) { fo.signal = append(fo.signal, signal.Defer(f)) }
}
