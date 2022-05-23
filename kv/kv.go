package kv

import "github.com/cockroachdb/pebble"

// |||||| ENGINE ||||||

var (
	ErrNotFound = pebble.ErrNotFound
)

// Reader is a readable key-value store.
type Reader interface {
	Get(key []byte) ([]byte, error)
	IteratorEngine
}

// Writer is a writeable key-value store.
type Writer interface {
	Set(key []byte, value []byte) error
	Delete(key []byte) error
}

// Closer is a key-value store that can be closed,
// which blocks until all pending operations have persisted to disk.
type Closer interface {
	Close() error
}

type KV interface {
	Writer
	Reader
	Closer
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
