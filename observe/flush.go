package observe

import (
	"github.com/arya-analytics/x/kv"
	"time"
)

// |||||| FLUSH ||||||

// FlushSubscriber is used to flush an observable whose contents implement
//  the kv.Flusher interface.
type FlushSubscriber[S kv.Flusher] struct {
	// Key is the key to flush the contents of the observable into.
	Key []byte
	// Store is the store to flush the contents of the observable into.
	Store kv.KV
	// MinInterval specifies the minimum interval between flushes. If the observable
	// updates more quickly than min interval, the FlushSubscriber will not flush the
	// contents.
	MinInterval time.Duration
	// LastFlush stores the last time the observable was flushed.
	LastFlush time.Time
	// ErrC is used to signal errors.
	ErrC chan error
}

// Flush is the handler to bind to the Observable.
func (f *FlushSubscriber[S]) Flush(state S) {
	if time.Since(f.LastFlush) < f.MinInterval {
		return
	}
	go func() {
		err := kv.Flush(f.Store, f.Key, state)
		if err != nil && f.ErrC != nil {
			f.ErrC <- err
		} else {
			f.LastFlush = time.Now()
		}
	}()
}
