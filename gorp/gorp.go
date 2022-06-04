package gorp

import (
	"github.com/arya-analytics/x/kv"
)

func Wrap(kv kv.KV) *DB { return &DB{kv: kv} }

type DB struct {
	encoder Encoder
	kv      kv.KV
}
