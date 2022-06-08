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
type Retrieve[E Entry] struct{ query.Query }

// NewRetrieve opens a new Retrieve query.
func NewRetrieve[E Entry]() Retrieve[E] { return Retrieve[E]{query.New()} }

// Where adds the provided filter to the query. If filtering by the key of the Entry, use the far more performance
// WhereKeys method instead.
func (r Retrieve[E]) Where(filter func(E) bool) Retrieve[E] { addFilter[E](r, filter); return r }

// WhereKeys queries the DB for entriesOpt with the provided keys. Although more targeted, this lookup is substantially
// faster than a general Where query.
func (r Retrieve[E]) WhereKeys(keys ...interface{}) Retrieve[E] { setWhereKeys(r, keys...); return r }

// Entries binds a slice that the Query will fill results into. Calls to Entry will override all previous calls to
// Entries or Entry.
func (r Retrieve[E]) Entries(entries *[]E) Retrieve[E] { setEntries(r, entries); return r }

// Entry binds the entry that the Query will fill results into. Calls to Entry will override all previous calls to
// Entries or Entry. If  multiple results are returned by the Query, entry will be set to the last result.
func (r Retrieve[E]) Entry(entry *E) Retrieve[E] { setEntry(r, entry); return r }

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (r Retrieve[E]) Exec(db *DB) error { return (&retrieve[E]{DB: db}).Exec(r) }

// |||||| FILTERS ||||||

const filtersKey query.OptionKey = "filters"

type filters[E Entry] []func(E) bool

func (f filters[E]) exec(entry E) bool {
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

func addFilter[E Entry](q query.Query, filter func(E) bool) {
	var f filters[E]
	rf, ok := q.Get(filtersKey)
	if !ok {
		f = filters[E]{}
	} else {
		f = rf.(filters[E])
	}
	f = append(f, filter)
	q.Set(filtersKey, f)
}

func getFilters[E Entry](q query.Query) filters[E] {
	rf, ok := q.Get(filtersKey)
	if !ok {
		return filters[E]{}
	}
	return rf.(filters[E])
}

// |||||| ENTRIES ||||||

const entriesKey query.OptionKey = "entriesOpt"

type entriesOpt[E Entry] struct {
	entry   *E
	entries *[]E
}

func (e *entriesOpt[E]) add(entry E) {
	if e.entry != nil {
		*e.entry = entry
	} else {
		*e.entries = append(*e.entries, entry)
	}
}

func (e *entriesOpt[E]) all() []E {
	if e.entry != nil {
		return []E{*e.entry}
	}
	return *e.entries
}

func setEntry[E Entry](q query.Query, entry *E) { q.Set(entriesKey, entriesOpt[E]{entry: entry}) }

func setEntries[E Entry](q query.Query, e *[]E) { q.Set(entriesKey, entriesOpt[E]{entries: e}) }

func getEntriesOpt[E Entry](q query.Query) entriesOpt[E] {
	return q.GetRequired(entriesKey).(entriesOpt[E])
}

// |||||| WHERE KEYS ||||||

const whereKeysKey query.OptionKey = "whereKeys"

type whereKeys []interface{}

func (w whereKeys) Bytes(encoder binary.Encoder) ([][]byte, error) {
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

func setWhereKeys(q query.Query, keys ...interface{}) { q.Set(whereKeysKey, whereKeys(keys)) }

func getWhereKeys(q query.Query) (whereKeys, bool) {
	keys, ok := q.Get(whereKeysKey)
	if !ok {
		return nil, false
	}
	return keys.(whereKeys), true
}

// |||||| EXECUTOR ||||||

type retrieve[E Entry] struct{ *DB }

func (r *retrieve[E]) Exec(q query.Query) error {
	if _, ok := getWhereKeys(q); ok {
		return r.whereKeys(q)
	}
	return r.filter(q)
}

func (r *retrieve[E]) whereKeys(q query.Query) error {
	var (
		keys, _ = getWhereKeys(q)
		f       = getFilters[E](q)
		entries = getEntriesOpt[E](q)
		prefix  = typePrefix[E](r.DB, r.encoder)
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

func (r *retrieve[E]) filter(q query.Query) error {
	var (
		f       = getFilters[E](q)
		entries = getEntriesOpt[E](q)
		prefix  = typePrefix[E](r.DB, r.encoder)
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
