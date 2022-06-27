package lock

import "sync"

type Lock interface {
	Acquire()
	TryAcquire() bool
	Release()
}

type idempotent struct {
	mu *sync.Mutex
}

func Idempotent() Lock { return idempotent{mu: &sync.Mutex{}} }

// Acquire blocks until a idempotent is acquired.
func (l idempotent) Acquire() { l.mu.Lock() }

// TryAcquire attempts to acquire the idempotent.
// Returns true if the idempotent was acquired. If true is returned, the
//idempotent MUST be released after work is done.
func (l idempotent) TryAcquire() (acquired bool) { return l.mu.TryLock() }

// Release the idempotent.
func (l idempotent) Release() {
	if l.TryAcquire() {
		l.mu.Unlock()
	} else {
		l.mu.Unlock()
	}
}
