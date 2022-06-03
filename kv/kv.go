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

// Reader is a readable key-value store.
type Reader interface {
	// Get returns the value for the given key.
	Get(key []byte) ([]byte, error)
	IteratorEngine
}

// Writer is a writeable key-value store.
type Writer interface {
	// Set sets the value for the given key.
	Set(key []byte, value []byte) error
	// Delete removes the value for the given key.
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
}

type Iterator interface {
	First() bool
	Last() bool
	Next() bool
	Key() []byte
	Valid() bool
	Value() []byte
	Close() error
}
