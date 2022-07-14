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

func (d Delete[K, E]) Exec(txn Txn) error {
	return (&del[K, E]{Txn: txn}).Exec(d)
}

type del[K Key, E Entry[K]] struct{ Txn }

func (d *del[K, E]) Exec(q query.Query) error {
	opts := d.Txn.options()
	var entries []E
	if err := (Retrieve[K, E]{Query: q}).Entries(&entries).Exec(d); err != nil {
		return err
	}
	prefix := typePrefix[K, E](opts)
	var keys whereKeys[K]
	for _, entry := range entries {
		keys = append(keys, entry.GorpKey())
	}
	byteKeys, err := keys.Bytes(opts.encoder)
	if err != nil {
		return err
	}
	for _, key := range byteKeys {
		if err := d.Delete(append(prefix, key...)); err != nil {
			return err
		}
	}
	return nil
}
