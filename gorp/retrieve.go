// Package gorp exposes a simple, type-safe ORM that wraps an underlying key-value store.
//
package gorp

import (
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
)

// |||||| QUERY ||||||

// Retrieve is a query that retrieves entriesOpt from the DB.
type Retrieve[K Key, E Entry[K]] struct{ query.Query }

// NewRetrieve opens a new Retrieve query.
func NewRetrieve[K Key, E Entry[K]]() Retrieve[K, E] { return Retrieve[K, E]{query.New()} }

// Where adds the provided filter to the query. If filtering by the key of the Entry, use the far more performance
// WhereKeys method instead.
func (r Retrieve[K, E]) Where(filter func(E) bool) Retrieve[K, E] {
	addFilter[K, E](r, filter)
	return r
}

// WhereKeys queries the DB for entriesOpt with the provided keys. Although more targeted, this lookup is substantially
// faster than a general Where query.
func (r Retrieve[K, E]) WhereKeys(keys ...K) Retrieve[K, E] {
	setWhereKeys(r, keys...)
	return r
}

// Entries binds a slice that the Query will fill results into. Calls to Entry will override all previous calls to
// Entries or Entry.
func (r Retrieve[K, E]) Entries(entries *[]E) Retrieve[K, E] { setEntries[K, E](r, entries); return r }

// Entry binds the entry that the Query will fill results into. Calls to Entry will override all previous calls to
// Entries or Entry. If  multiple results are returned by the Query, entry will be set to the last result.
func (r Retrieve[K, E]) Entry(entry *E) Retrieve[K, E] { setEntry[K, E](r, entry); return r }

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (r Retrieve[K, E]) Exec(db *DB) error { return (&retrieve[K, E]{DB: db}).Exec(r) }

// |||||| FILTERS ||||||

const filtersKey query.OptionKey = "filters"

type filters[K Key, E Entry[K]] []func(E) bool

func (f filters[K, E]) exec(entry E) bool {
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

func addFilter[K Key, E Entry[K]](q query.Query, filter func(E) bool) {
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

// |||||| ENTRIES ||||||

const entriesKey query.OptionKey = "entriesOpt"

type entriesOpt[K Key, E Entry[K]] struct {
	entry   *E
	entries *[]E
}

func (e *entriesOpt[K, E]) add(entry E) {
	if e.entry != nil {
		*e.entry = entry
	} else {
		*e.entries = append(*e.entries, entry)
	}
}

func (e *entriesOpt[K, E]) all() []E {
	if e.entry != nil {
		return []E{*e.entry}
	}
	return *e.entries
}

func setEntry[K Key, E Entry[K]](q query.Query, entry *E) {
	q.Set(entriesKey, entriesOpt[K, E]{entry: entry})
}

func setEntries[K Key, E Entry[K]](q query.Query, e *[]E) {
	q.Set(entriesKey, entriesOpt[K, E]{entries: e})
}

func getEntriesOpt[K Key, E Entry[K]](q query.Query) entriesOpt[K, E] {
	return q.GetRequired(entriesKey).(entriesOpt[K, E])
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

type retrieve[K Key, E Entry[K]] struct{ *DB }

func (r *retrieve[K, E]) Exec(q query.Query) error {
	if _, ok := getWhereKeys[K](q); ok {
		return r.whereKeys(q)
	}
	return r.filter(q)
}

func (r *retrieve[K, E]) whereKeys(q query.Query) error {
	var (
		keys, _ = getWhereKeys[K](q)
		f       = getFilters[K, E](q)
		entries = getEntriesOpt[K, E](q)
		prefix  = typePrefix[K, E](r.DB, r.encoder)
	)
	byteKeys, err := keys.Bytes(r.encoder)
	if err != nil {
		return err
	}
	for _, key := range byteKeys {
		b, err := r.kv.Get(append(prefix, key...))
		if err == kv.ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		var entry E
		if err = r.decoder.Decode(b, &entry); err != nil {
			return err
		}
		if f.exec(entry) {
			entries.add(entry)
		}
	}
	return nil
}

func (r *retrieve[K, E]) filter(q query.Query) error {
	var (
		f       = getFilters[K, E](q)
		entries = getEntriesOpt[K, E](q)
		prefix  = typePrefix[K, E](r.DB, r.encoder)
		iter    = r.kv.IterPrefix(prefix)
	)
	for iter.First(); iter.Valid(); iter.Next() {
		var entry E
		if err := r.decoder.Decode(iter.Value(), &entry); err != nil {
			return err
		}
		if f.exec(entry) {
			entries.add(entry)
		}
	}
	return iter.Close()
}
