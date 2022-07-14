package gorp

import "github.com/arya-analytics/x/query"

type Delete[K Key, E Entry[K]] struct{ query.Query }

func NewDelete[K Key, E Entry[K]]() Delete[K, E] { return Delete[K, E]{Query: query.New()} }

func (d Delete[K, E]) Where(filter func(E) bool) Delete[K, E] {
	addFilter[K, E](d.Query, filter)
	return d
}

func (d Delete[K, E]) WhereKeys(keys ...K) Delete[K, E] {
	setWhereKeys[K](d.Query, keys...)
	return d
}

func (d Delete[K, E]) Exec(db *DB) error {
	return (&del[K, E]{DB: db}).Exec(d)
}

type del[K Key, E Entry[K]] struct{ *DB }

func (d *del[K, E]) Exec(q query.Query) error {
	var entries []E
	if err := (Retrieve[K, E]{Query: q}).Entries(&entries).Exec(d.DB); err != nil {
		return err
	}
	prefix := typePrefix[K, E](d.DB, d.encoder)
	var keys whereKeys[K]
	for _, entry := range entries {
		keys = append(keys, entry.GorpKey())
	}
	byteKeys, err := keys.Bytes(d.encoder)
	if err != nil {
		return err
	}
	for _, key := range byteKeys {
		if err := d.kv.Delete(append(prefix, key...)); err != nil {
			return err
		}
	}
	return nil
}
