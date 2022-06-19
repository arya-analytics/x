package shutdown

import (
	"context"
	"github.com/arya-analytics/x/errutil"
	"sync"
	"time"
)

type Signal int

const DefaultShutdownThreshold = 10 * time.Second

type Shutdown interface {
	Go(f func(chan Signal) error, opts ...GoOption)
	GoTick(interval time.Duration, f func() error, opts ...GoOption)
	Routines() map[string]int
	NumRoutines() int
	Shutdown() error
	ShutdownAfter(d time.Duration) error
}

func New(opts ...Option) Shutdown {
	return &base{
		signal:   make(chan Signal),
		opts:     newOptions(opts...),
		routines: make(map[string]int),
		errors:   make(chan error, 10),
	}
}

type base struct {
	signal   chan Signal
	mu       sync.Mutex
	routines map[string]int
	count    int
	errors   chan error
	opts     *options
}

func (s *base) Go(f func(chan Signal) error, opts ...GoOption) {
	goOpt := newGoOpts(opts...)
	go func() {
		err := f(s.signal)
		s.closeRoutine(goOpt.key, err)
	}()
	s.addRoutine(goOpt.key)
}

func (s *base) GoTick(interval time.Duration, f func() error, opts ...GoOption) {
	var (
		ticker = time.NewTicker(interval)
		opt    = newGoOpts(opts...)
		c      = errutil.NewCatchSimple()
	)
	if opt.pipe != nil {
		c = errutil.NewCatchSimple(errutil.WithHooks(errutil.NewPipeHook(opt.pipe)))
	}
	s.Go(func(sig chan Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case <-ticker.C:
				if opt.ctx != nil {
					c.Exec(opt.ctx.Err)
				}
				c.Exec(f)
				if c.Error() != nil && opt.pipe == nil {
					return c.Error()
				}
			}
		}
	})
}

func (s *base) Shutdown() error {
	close(s.signal)
	t := time.NewTimer(s.opts.shutdownThreshold)
	var errors []error
o:
	for {
		select {
		case err := <-s.errors:
			errors = append(errors, err)
			if len(errors) >= s.count {
				break o
			}
		case <-t.C:
			panic("[base.Shutdown] graceful base timeout exceeded")
		}
	}
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *base) ShutdownAfter(d time.Duration) error {
	time.Sleep(d)
	return s.Shutdown()
}

func (s *base) Routines() map[string]int {
	nRoutines := make(map[string]int)
	s.mu.Lock()
	for k, v := range s.routines {
		nRoutines[k] = v
	}
	return nRoutines
}

func (s *base) NumRoutines() int {
	return s.count
}

const defaultRoutineKey = "unkeyed"

func (s *base) addRoutine(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" {
		key = defaultRoutineKey
	}
	s.count++
	s.routines[key]++
}

func (s *base) closeRoutine(key string, err error) {
	if key == "" {
		key = defaultRoutineKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routines[key]--
	s.count--
	if s.routines[key] == 0 {
		delete(s.routines, key)
	}
	s.errors <- err
}

// |||||| OPTIONS ||||||

type options struct {
	shutdownThreshold time.Duration
}

type Option func(*options)

func newOptions(opts ...Option) *options {
	o := &options{shutdownThreshold: DefaultShutdownThreshold}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithThreshold(threshold time.Duration) Option {
	return func(o *options) { o.shutdownThreshold = threshold }
}

// |||||| GO OPTIONS ||||||

type goOptions struct {
	key  string
	ctx  context.Context
	pipe chan error
}

func newGoOpts(opts ...GoOption) goOptions {
	var goOpt goOptions
	for _, o := range opts {
		o(&goOpt)
	}
	return goOpt
}

type GoOption func(*goOptions)

func WithKey(key string) GoOption { return func(opt *goOptions) { opt.key = key } }

func WithContext(ctx context.Context) GoOption { return func(opt *goOptions) { opt.ctx = ctx } }

func WithErrPipe(errC chan error) GoOption { return func(opt *goOptions) { opt.pipe = errC } }
