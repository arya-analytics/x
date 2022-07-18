// Package gorp exposes a simple, type-safe ORM that wraps an underlying key-value store.
//
package gorp

import (
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
	"github.com/cockroachdb/errors"
)

// |||||| QUERY ||||||

// Retrieve is a query that retrieves Entries from the DB.
type Retrieve[K Key, E Entry[K]] struct{ query.Query }

// NewRetrieve opens a new Retrieve query.
func NewRetrieve[K Key, E Entry[K]]() Retrieve[K, E] { return Retrieve[K, E]{query.New()} }

// Where adds the provided filter to the query. If filtering by the key of the Entry, use the far more performance
// WhereKeys method instead.
func (r Retrieve[K, E]) Where(filter func(*E) bool) Retrieve[K, E] {
	addFilter[K, E](r, filter)
	return r
}

// WhereKeys queries the DB for Entries with the provided keys. Although more targeted, this lookup is substantially
// faster than a general Where query.
func (r Retrieve[K, E]) WhereKeys(keys ...K) Retrieve[K, E] {
	setWhereKeys(r, keys...)
	return r
}

// Entries binds a slice that the Query will fill results into. Calls to Entry will override All previous calls to
// Entries or Entry.
func (r Retrieve[K, E]) Entries(entries *[]E) Retrieve[K, E] {
	SetEntries[K, E](r, entries)
	return r
}

// Entry binds the entry that the Query will fill results into. Calls to Entry will override All previous calls to
// Entries or Entry. If  multiple results are returned by the Query, entry will be set to the last result.
func (r Retrieve[K, E]) Entry(entry *E) Retrieve[K, E] {
	SetEntry[K, E](r, entry)
	return r
}

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (r Retrieve[K, E]) Exec(txn Txn) error { return (&retrieve[K, E]{Txn: txn}).Exec(r) }

func (r Retrieve[K, E]) Exists(txn Txn) (bool, error) {
	return (&retrieve[K, E]{Txn: txn}).Exists(r)
}

// |||||| FILTERS ||||||

const filtersKey query.OptionKey = "filters"

type filters[K Key, E Entry[K]] []func(*E) bool

func (f filters[K, E]) exec(entry *E) bool {
	if len(f) == 0 {
		return true
	}
	for _, filter := range f {
		if filter(entry) {
			return true
		}
	}
	return false
}

func addFilter[K Key, E Entry[K]](q query.Query, filter func(*E) bool) {
	var f filters[K, E]
	rf, ok := q.Get(filtersKey)
	if !ok {
		f = filters[K, E]{}
	} else {
		f = rf.(filters[K, E])
	}
	f = append(f, filter)
	q.Set(filtersKey, f)
}

func getFilters[K Key, E Entry[K]](q query.Query) filters[K, E] {
	rf, ok := q.Get(filtersKey)
	if !ok {
		return filters[K, E]{}
	}
	return rf.(filters[K, E])
}

// |||||| WHERE KEYS ||||||

const whereKeysKey query.OptionKey = "whereKeys"

type whereKeys[K Key] []K

func (w whereKeys[K]) Bytes(encoder binary.Encoder) ([][]byte, error) {
	byteWhereKeys := make([][]byte, len(w))
	for i, key := range w {
		var err error
		byteWhereKeys[i], err = encoder.Encode(key)
		if err != nil {
			return nil, err
		}
	}
	return byteWhereKeys, nil
}

func setWhereKeys[K Key](q query.Query, keys ...K) { q.Set(whereKeysKey, whereKeys[K](keys)) }

func getWhereKeys[K Key](q query.Query) (whereKeys[K], bool) {
	keys, ok := q.Get(whereKeysKey)
	if !ok {
		return nil, false
	}
	return keys.(whereKeys[K]), true
}

// |||||| EXECUTOR ||||||

type retrieve[K Key, E Entry[K]] struct{ Txn }

func (r *retrieve[K, E]) Exec(q query.Query) error {
	if _, ok := getWhereKeys[K](q); ok {
		return r.whereKeys(q)
	}
	return r.filter(q)
}

func (r *retrieve[K, E]) Exists(q query.Query) (bool, error) {
	if keys, ok := getWhereKeys[K](q); ok {
		entries := make([]E, 0, len(keys))
		SetEntries[K, E](q, &entries)
		err := r.whereKeys(q)
		return len(entries) == len(keys), err
	}
	entries := make([]E, 0, 1)
	SetEntries[K, E](q, &entries)
	err := r.filter(q)
	return len(entries) > 0, err

}

func (r *retrieve[K, E]) whereKeys(q query.Query) error {
	opts := r.options()
	var (
		keys, _ = getWhereKeys[K](q)
		f       = getFilters[K, E](q)
		entries = GetEntries[K, E](q)
		prefix  = typePrefix[K, E](opts)
	)
	byteKeys, err := keys.Bytes(opts.encoder)
	if err != nil {
		return err
	}
	var entry *E
	for _, key := range byteKeys {
		b, _err := r.Get(append(prefix, key...))
		if _err != nil {
			if _err == kv.NotFound {
				err = query.NotFound
			} else {
				err = _err
			}
			continue
		}
		if _err = opts.decoder.Decode(b, &entry); err != nil {
			return _err
		}
		if f.exec(entry) {
			entries.Add(*entry)
		}
	}
	return err
}

func (r *retrieve[K, E]) filter(q query.Query) error {
	opts := r.options()
	var (
		f       = getFilters[K, E](q)
		entries = GetEntries[K, E](q)
		iter    = r.NewIterator(kv.PrefixIter(typePrefix[K, E](opts)))
	)
	var entry *E
	for iter.First(); iter.Valid(); iter.Next() {
		if err := opts.decoder.Decode(iter.Value(), &entry); err != nil {
			return errors.Wrap(err, "[gorp] - failed to decode entry")
		}
		if f.exec(entry) {
			entries.Add(*entry)
		}
	}
	return iter.Close()
}
