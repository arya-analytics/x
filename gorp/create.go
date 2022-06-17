package gorp

import (
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/query"
	"reflect"
)

// |||||| CREAEE ||||||

// Create is a query that creates entriesOpt in the DB.
type Create[K Key, E Entry[K]] struct{ query.Query }

// NewCreate opens a new Create query.
func NewCreate[K Key, E Entry[K]]() Create[K, E] { return Create[K, E]{query.New()} }

// Entries sets the entriesOpt to write to the DB.
func (c Create[K, E]) Entries(entries *[]E) Create[K, E] { setEntries[K, E](c, entries); return c }

// Entry sets the entry to write to the DB.
func (c Create[K, E]) Entry(entry *E) Create[K, E] { setEntry[K, E](c, entry); return c }

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (c Create[K, E]) Exec(db *DB) error { return (&createExecutor[K, E]{DB: db}).Exec(c) }

// |||||| EXECUTOR ||||||

type createExecutor[K Key, E Entry[K]] struct{ *DB }

func (c *createExecutor[K, E]) Exec(q query.Query) error {
	entries := getEntriesOpt[K, E](q)
	prefix := typePrefix[K, E](c.DB, c.encoder)
	for _, entry := range entries.all() {
		data, err := c.encoder.Encode(entry)
		if err != nil {
			return err
		}
		key, err := c.encoder.Encode(entry.GorpKey())
		if err != nil {
			return err
		}
		k := append(prefix, key...)
		if err = c.kv.Set(k, data); err != nil {
			return err
		}
	}
	return nil
}

func typePrefix[K Key, E Entry[K]](db *DB, encoder binary.Encoder) []byte {
	if !db.typePrefix {
		return []byte{}
	}
	mName := reflect.TypeOf(*new(E)).Name()
	b, err := encoder.Encode(mName)
	if err != nil {
		panic(err)
	}
	return b
}
