package gorp

import (
	"github.com/arya-analytics/x/query"
)

type Query interface {
	query.Query
	Exec(db *DB) error
}
