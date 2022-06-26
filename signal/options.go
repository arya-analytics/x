package signal

// |||||| GO OPTIONS ||||||

type GoOption func(o *goOptions)

func WithDefer(f func()) GoOption {
	return func(o *goOptions) { o.deferals = append(o.deferals, f) }
}

type goOptions struct {
	deferals []func()
}

func newGoOptions(opts []GoOption) *goOptions {
	o := &goOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
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
