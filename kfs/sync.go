package kfs

import (
	"github.com/arya-analytics/x/errutil"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
	"time"
)

// Sync is a synchronization utility that periodically flushes the contents of "idle" files to disk.
// It synchronizes files on two conditions:
//
// 	1. When the file is "idle" i.e. the file is not locked.
//  2. The files "age" i.e. the time since the file was last synced exceeds Sync.MaxAge.
//
// Sequential struct fields must be initialized before the Sync is started using Sync.Start().
type Sync[T comparable] struct {
	// FS is the file system to sync.
	FS FS[T]
	// Interval is the time between syncs.
	Interval time.Duration
	// MaxAge sets the maximum age of a file before it is synced.
	MaxAge time.Duration
	// Conductor is used to fork and close goroutines.
}

// Start starts a goroutine that periodically calls Sync.
// Shuts down based on the Sync.Shutter.
// When sync.Shutter.Shutdown is called, the Sync executes a forced sync ON all files and then exits.
func (s *Sync[T]) Start(ctx signal.Context) <-chan error {
	errs := make(chan error)
	c := errutil.NewCatchSimple(errutil.WithHooks(errutil.NewPipeHook(errs)))
	t := time.NewTicker(s.Interval)
	ctx.Go(func(ctx signal.Context) error {
		for {
			select {
			case <-ctx.Done():
				return errors.CombineErrors(s.forceSync(), ctx.Err())
			case <-t.C:
				c.Exec(s.sync)
			}
		}
	})
	return errs
}

func (s *Sync[T]) sync() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, v := range s.FS.OpenFiles() {
		if v.Age() > s.MaxAge && v.TryAcquire() {
			c.Exec(v.Sync)
			v.Release()
		}
	}
	return c.Error()
}

func (s *Sync[T]) forceSync() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, v := range s.FS.OpenFiles() {
		v.Acquire()
		c.Exec(v.Sync)
		v.Release()
	}
	return c.Error()
}
