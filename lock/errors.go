package lock

import "github.com/cockroachdb/errors"

var (
	// ErrLocked is returned when a lock is already held.
	ErrLocked = errors.New("[lock] - already held")
)
