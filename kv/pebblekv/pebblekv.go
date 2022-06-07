// Package pebblekv implements a wrapper around cockroachdb's pebble storage engine that implements
// the kv.KV interface. To use it, open a new pebble.DB and call Wrap() to wrap it.
package pebblekv

import (
	kvc "github.com/arya-analytics/x/kv"
	"github.com/cockroachdb/pebble"
)

type pebbleKV struct{ *pebble.DB }

// Wrap wraps a pebble.DB to satisfy the kv.KV interface.
func Wrap(db *pebble.DB) kvc.KV { return &pebbleKV{DB: db} }

// Get implements the kv.KV interface.
func (kv pebbleKV) Get(key []byte, opts ...interface{}) ([]byte, error) {
	v, c, err := kv.DB.Get(key)
	if err != nil {
		return v, err
	}
	return v, c.Close()
}

// Set implements the kv.KV interface.
func (kv pebbleKV) Set(key []byte, value []byte, opts ...interface{}) error {
	return kv.DB.Set(key, value, pebble.NoSync)
}

// Delete implements the kv.KV interface.
func (kv pebbleKV) Delete(key []byte) error { return kv.DB.Delete(key, pebble.NoSync) }

// Close implements the kv.KV interface.
func (kv pebbleKV) Close() error { return kv.DB.Close() }

// IterPrefix implements the kv.KV interface.
func (kv pebbleKV) IterPrefix(prefix []byte) kvc.Iterator {
	upper := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil
	}
	opts := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{LowerBound: prefix, UpperBound: upper(prefix)}
	}
	return kv.DB.NewIter(opts(prefix))
}

// IterRange implements the kv.KV interface.
func (kv pebbleKV) IterRange(start, end []byte) kvc.Iterator {
	return kv.DB.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
}

func (kv pebbleKV) NewIterator(opts kvc.IterOptions) kvc.Iterator {
	return kv.DB.NewIter(&pebble.IterOptions{LowerBound: opts.LowerBound, UpperBound: opts.UpperBound})
}

// String implements the kv.KV interface.
func (kv pebbleKV) String() string { return "pebbleKV" }
