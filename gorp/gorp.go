package gorp

import (
	"github.com/arya-analytics/x/kv"
)

func Wrap(kv kv.KV, opts ...Option) *DB {
	o := newOptions(opts...)
	mergeDefaultOptions(o)
	return &DB{kv: kv, options: o}
}

type DB struct {
	kv kv.KV
	*options
}
