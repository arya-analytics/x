package signal

import (
	"fmt"
)

// |||||| GO OPTIONS ||||||

type GoOption func(o *goOptions)

func WithDefer(f func()) GoOption {
	return func(o *goOptions) { o.deferals = append(o.deferals, f) }
}

func WithName(name string) GoOption {
	return func(o *goOptions) { o.name = name }
}

type goOptions struct {
	name     string
	deferals []func()
}

func newGoOptions(c Census, opts []GoOption) *goOptions {
	o := &goOptions{}
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaultGoOptions(c, o)
	return o
}

func mergeDefaultGoOptions(
	c Census,
	o *goOptions,
) {

	// |||| KEY ||||

	if o.name == "" {
		o.name = defaultKey(c)
	}
}

func defaultKey(c Census) string {
	return fmt.Sprintf("routine-%d", c.GoCount())
}

// |||||| OPTIONS ||||||

type Option func(o *options)

type options struct {
	closeBufferSize uint
	defaultGoOpts   *goOptions
}

func newOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaultOptions(o)
	return o
}

func mergeDefaultOptions(o *options) {
	o.closeBufferSize = 100

	if o.defaultGoOpts == nil {
		o.defaultGoOpts = &goOptions{}
	}
}
