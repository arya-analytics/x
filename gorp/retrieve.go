// Package gorp exposes a simple, type-safe ORM that wraps an underlying key-value store.
//
package gorp

import (
	"context"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
)

// |||||| QUERY ||||||

// Retrieve is a query that retrieves entries from the DB.
type Retrieve[E Entry] struct{ query.Query }

// NewRetrieve opens a new Retrieve query.
func NewRetrieve[E Entry]() Retrieve[E] { return Retrieve[E]{query.New()} }

// Where adds the provided filter to the query. If filtering by the key of the Entry, use the far more performance
// WhereKeys method instead.
func (r Retrieve[E]) Where(filter func(E) bool) Retrieve[E] { addFilter[E](r, filter); return r }

// WhereKeys queries the DB for entries with the provided keys. Although more targeted, this lookup is substantially
// faster than a general Where query.
func (r Retrieve[E]) WhereKeys(keys ...interface{}) Retrieve[E] { setWhereKeys(r, keys...); return r }

// Entries binds a slice that the Query will fill results into.
func (r Retrieve[E]) Entries(model *[]E) Retrieve[E] { setEntries(r, model); return r }

// variant implements Query.
func (r Retrieve[E]) Variant() variant { return variantRetrieve }

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (r Retrieve[E]) Exec(ctx context.Context, db *DB) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	query.SetContext(r, ctx)
	return (&retrieve[E]{DB: db}).Exec(r)
}

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

const entriesKey query.OptionKey = "entries"

func setEntries[E Entry](q query.Query, models *[]E) { q.Set(entriesKey, models) }

func getEntries[E Entry](q query.Query) *[]E { return q.GetRequired(entriesKey).(*[]E) }

// |||||| WHERE KEYS ||||||

const whereKeysKey query.OptionKey = "whereKeys"

type whereKeys []interface{}

func (w whereKeys) Bytes(encoder Encoder) ([][]byte, error) {
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
		entries = getEntries[E](q)
		prefix  = typePrefix(*entries, r.encoder)
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
		m := new(E)
		if err = r.decoder.Decode(b, m); err != nil {
			return err
		}
		if f.exec(*m) {
			*entries = append(*entries, *m)
		}
	}
	return nil
}

func (r *retrieve[E]) filter(q query.Query) error {
	var (
		f       = getFilters[E](q)
		entries = getEntries[E](q)
		prefix  = typePrefix(*entries, r.encoder)
		iter    = r.kv.IterPrefix(prefix)
	)
	for iter.First(); iter.Valid(); iter.Next() {
		m := new(E)
		if err := r.decoder.Decode(iter.Value(), m); err != nil {
			return err
		}
		if f.exec(*m) {
			*entries = append(*entries, *m)
		}
	}
	return iter.Close()
}
