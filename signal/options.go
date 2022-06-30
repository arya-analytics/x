package signal

// |||||| GO OPTIONS ||||||

type GoOption func(o *goOptions)

func Defer(f func()) GoOption { return func(o *goOptions) { o.deferals = append(o.deferals, f) } }

func WithKey(key string) GoOption {
	return func(o *goOptions) { o.key = key }
}

type goOptions struct {
	key      string
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

func WithBufferCap(transient, fatal uint) Option {
	return func(o *options) {
		o.transientCap = &transient
		o.fatalCap = &fatal
	}
}

type options struct {
	transientCap  *uint
	fatalCap      *uint
	defaultGoOpts *goOptions
}

func newOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaultOptions(o)
	return o
}

const (
	defaultTransientCap = 0
	defaultFatalCap     = 0
)

func mergeDefaultOptions(o *options) {
	if o.transientCap == nil {
		o.transientCap = new(uint)
		*o.transientCap = defaultTransientCap
	}
	if o.fatalCap == nil {
		o.fatalCap = new(uint)
		*o.fatalCap = defaultFatalCap
	}

	if o.defaultGoOpts == nil {
		o.defaultGoOpts = &goOptions{}
	}
}
