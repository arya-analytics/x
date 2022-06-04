package gorp

import (
	"github.com/arya-analytics/x/query"
)

type Model interface {
	Key() interface{}
}

type Retrieve[T Model] struct{ query.Query }

func (r Retrieve[T]) Where(filter func(T) bool) Retrieve[T] { addFilter[T](r, filter); return r }

func (r Retrieve[T]) Model(model *[]T) Retrieve[T] { setModel(r, model); return r }

func (r Retrieve[T]) Variant() Variant { return VariantRetrieve }

const filtersKey query.OptionKey = "filters"

type filters[T Model] struct {
	filters []func(T) bool
}

func addFilter[T Model](q query.Query, filter func(T) bool) {
	var f filters[T]
	rf, ok := q.Get(filtersKey)
	if !ok {
		f = filters[T]{}
	}
	f = rf.(filters[T])
	f.filters = append(f.filters, filter)
	q.Set(filtersKey, f)
}

func getFilters[T Model](q query.Query) filters[T] {
	rf, ok := q.Get(filtersKey)
	if !ok {
		return filters[T]{}
	}
	return rf.(filters[T])
}

const modelKey query.OptionKey = "model"

func setModel[T Model](q query.Query, models *[]T) { q.Set(modelKey, models) }

func getModel[T Model](q query.Query) *[]T { return q.GetRequired(modelKey).(*[]T) }

func modelOptSet[T Model](q query.Query) bool { _, ok := q.Get(modelKey); return ok }
