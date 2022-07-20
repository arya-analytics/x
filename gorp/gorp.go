package gorp

import (
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
)

func Wrap(kv kv.DB, opts ...Option) *DB {
	o := newOptions(opts...)
	mergeDefaultOptions(o)
	return &DB{DB: kv, opts: o}
}

type Txn interface {
	kv.Batch
	options() *options
}

type DB struct {
	kv.DB
	opts *options
}

func (db *DB) options() *options { return db.opts }

func (db *DB) BeginTxn() Txn { return txn{Batch: db.NewBatch(), db: db} }

func (db *DB) Commit(opts ...interface{}) error { return nil }

type Query interface {
	query.Query
	Exec(db *DB) error
}

type txn struct {
	// db is the underlying gorp DB the txn is operating on.
	db *DB
	kv.Batch
}

func (t txn) options() *options { return t.db.opts }
