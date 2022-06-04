package gorp

import (
	"context"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
)

// |||||| CREATE ||||||

type Create[T Model] struct{ query.Query }

func NewCreate[T Model]() Create[T] { return Create[T]{query.New()} }

func (c Create[T]) Model(model *[]T) Create[T] { setModel(c, model); return c }

func (c Create[T]) Variant() Variant { return VariantCreate }

func (c Create[T]) Exec(ctx context.Context, db *DB) error {
	query.SetContext(c, ctx)
	return (&createExecutor[T]{kv: db.kv}).Exec(c)
}

// |||||| EXECUTOR ||||||

type createExecutor[T Model] struct {
	kv      kv.KV
	encoder Encoder
}

func (c *createExecutor[T]) Exec(q query.Query) error {
	for _, model := range *getModel[T](q) {
		data, err := c.encoder.Encode(model)
		if err != nil {
			return err
		}
		key, err := c.encoder.Encode(model.Key())
		if err != nil {
			return err
		}
		if err = c.kv.Set(key, data); err != nil {
			return err
		}
	}
	return nil
}
