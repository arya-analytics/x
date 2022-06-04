package gorp

import (
	"context"
	"github.com/arya-analytics/x/query"
)

type Query interface {
	query.Query
	Variant() Variant
	Exec(ctx context.Context, db DB) error
}

type Variant byte

const (
	VariantRetrieve Variant = iota
	VariantCreate
)
