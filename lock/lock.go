package lock

import "sync"

type Lock interface {
	// Acquire blocks until the lock is acquired.
	Acquire()
	// TryAcquire attempts to acquire the lock. Returns true if the lock was acquired.
	// If true is returned, the lock must be released after work is done.
	TryAcquire() bool
	// Release releases the lock. In some implementations, this operation may be
	// idempotent. In other implementations, releasing an open lock may panic.
	Release()
}

// Idempotent is a lock that can be released even if it has not been acquired.
func Idempotent() Lock { return idempotent{Mutex: &sync.Mutex{}} }

type idempotent struct{ *sync.Mutex }

// Acquire implements Lock.
func (l idempotent) Acquire() { l.Lock() }

// TryAcquire implements Lock.
func (l idempotent) TryAcquire() (acquired bool) { return l.TryLock() }

// Release implements Lock.
func (l idempotent) Release() { l.TryAcquire(); l.Unlock() }
