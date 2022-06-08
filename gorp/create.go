package gorp

import (
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/query"
	"reflect"
)

// |||||| CREATE ||||||

// Create is a query that creates entriesOpt in the DB.
type Create[T Entry] struct{ query.Query }

// NewCreate opens a new Create query.
func NewCreate[T Entry]() Create[T] { return Create[T]{query.New()} }

// Entries sets the entriesOpt to write to the DB.
func (c Create[T]) Entries(entries *[]T) Create[T] { setEntries(c, entries); return c }

// Entry sets the entry to write to the DB.
func (c Create[T]) Entry(entry *T) Create[T] { setEntry(c, entry); return c }

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (c Create[T]) Exec(db *DB) error { return (&createExecutor[T]{DB: db}).Exec(c) }

// |||||| EXECUTOR ||||||

type createExecutor[T Entry] struct{ *DB }

func (c *createExecutor[T]) Exec(q query.Query) error {
	entries := getEntriesOpt[T](q)
	prefix := typePrefix[T](c.DB, c.encoder)
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

func typePrefix[T Entry](db *DB, encoder binary.Encoder) []byte {
	if !db.typePrefix {
		return []byte{}
	}
	mName := reflect.TypeOf(*new(T)).Name()
	b, err := encoder.Encode(mName)
	if err != nil {
		panic(err)
	}
	return b
}
