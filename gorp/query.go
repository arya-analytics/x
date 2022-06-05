package gorp

import (
	"context"
	"github.com/arya-analytics/x/query"
)

type Query interface {
	query.Query
	Exec(ctx context.Context, db DB) error
}
