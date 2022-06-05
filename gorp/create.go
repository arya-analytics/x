package gorp

import (
	"context"
	"github.com/arya-analytics/x/query"
	"reflect"
)

// |||||| CREATE ||||||

// Create is a query that creates entries in the DB.
type Create[T Entry] struct{ query.Query }

// NewCreate opens a new Create query.
func NewCreate[T Entry]() Create[T] { return Create[T]{query.New()} }

// Entries sets the entries to write to the DB.
func (c Create[T]) Entries(model *[]T) Create[T] { setEntries(c, model); return c }

// Variant implements Query.
func (c Create[T]) Variant() Variant { return VariantCreate }

// Exec executes the Query against the provided DB. It returns any errors encountered during execution.
func (c Create[T]) Exec(ctx context.Context, db *DB) error {
	query.SetContext(c, ctx)
	return (&createExecutor[T]{DB: db}).Exec(c)
}

// |||||| EXECUTOR ||||||

type createExecutor[T Entry] struct{ *DB }

func (c *createExecutor[T]) Exec(q query.Query) error {
	entries := *getEntries[T](q)
	prefix := typePrefix(entries, c.encoder)
	for _, entry := range entries {
		data, err := c.encoder.Encode(entry)
		if err != nil {
			return err
		}
		key, err := c.encoder.Encode(entry.Key())
		if err != nil {
			return err
		}
		if err = c.kv.Set(append(prefix, key...), data); err != nil {
			return err
		}
	}
	return nil
}

func typePrefix[T Entry](m []T, encoder Encoder) []byte {
	mName := reflect.TypeOf(m).Elem().Name()
	b, err := encoder.Encode(mName)
	if err != nil {
		panic(err)
	}
	return b
}
