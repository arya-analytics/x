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

type txn struct {
	kv.Batch
	opts *options
}

func (t txn) options() *options { return t.opts }

type DB struct {
	kv.DB
	opts *options
}

func (db *DB) options() *options { return db.opts }

func (db *DB) Begin() Txn { return txn{Batch: db.NewBatch(), opts: db.opts} }

func (db *DB) Commit(opts ...interface{}) error { return nil }

type Query interface {
	query.Query
	Exec(db *DB) error
}
