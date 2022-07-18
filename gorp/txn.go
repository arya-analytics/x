package gorp

import "github.com/arya-analytics/x/kv"

type txn struct {
	// db is the underlying gorp DB the txn is operating on.
	db *DB
	kv.Batch
}

func (t txn) options() *options { return t.db.opts }
