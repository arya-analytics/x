// Package kv defines a general interface for a key-value store that provides support for get/set/delete operations
// as well as basic read-iteration. This package should be used as a boundary for separating an application from a
// specific storage implementation.
//
// For a general implementation of KV, see the pebblekv package.
// For an in-memory implementation of KV, see the memkv package.
//
package kv

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

// |||||| ENGINE ||||||

// ErrNotFound is returned when a key is not found in the KV store.
var ErrNotFound = pebble.ErrNotFound

type IterValidityState = pebble.IterValidityState

type IterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

// Reader is a readable key-value store.
type Reader interface {
	// Get returns the value for the given key.
	Get(key []byte, opts ...interface{}) ([]byte, error)
	IteratorEngine
}

// Writer is a writeable key-value store.
type Writer interface {
	// Set sets the value for the given key. It is safe to modify the contents of key
	// and value after Set returns.
	Set(key []byte, value []byte, opts ...interface{}) error
	// Delete removes the value for the given key. It is safe to modify the contents
	// of key after Delete returns.
	Delete(key []byte) error
}

// Closer is a closeable key-value store, which blocks until all pending
// operations have persisted to disk.
type Closer interface {
	// Close closes the KV store.
	Close() error
}

// KV represents a general key-value store.
type KV interface {
	Writer
	Reader
	Closer
	// Stringer returns a string description of the KV. Used for logging and configuration.
	fmt.Stringer
}

type IteratorEngine interface {
	IterPrefix(prefix []byte) Iterator
	IterRange(start []byte, end []byte) Iterator
	NewIterator(opts IterOptions) Iterator
}

type Iterator interface {
	First() bool
	Last() bool
	Next() bool
	Prev() bool
	NextWithLimit(limit []byte) IterValidityState
	Key() []byte
	Valid() bool
	Value() []byte
	Close() error
	Error() error
	SeekLT(key []byte) bool
	SeekGE(key []byte) bool
}
