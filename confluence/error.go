package confluence

import (
	"github.com/arya-analytics/x/util/errutil"
)

type RouteCatcher[V Value] struct {
	errutil.CatchSimple
	Pipeline *Pipeline[V]
}

func NewRouteCatcher[V Value](pipeline *Pipeline[V], opts ...errutil.CatchOpt) *RouteCatcher[V] {
	return &RouteCatcher[V]{CatchSimple: *errutil.NewCatchSimple(opts...)}
}

func (r *RouteCatcher[V]) Route(router Router[V]) {
	r.CatchSimple.Exec(func() error { return r.Pipeline.Route(router) })
}
