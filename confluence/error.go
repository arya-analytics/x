package confluence

import (
	"github.com/arya-analytics/x/errutil"
)

type RouteBuilder[V Value] struct {
	errutil.CatchSimple
	Pipeline *Pipeline[V]
}

func (r *RouteBuilder[V]) Route(router Router[V]) {
	r.CatchSimple.Exec(func() error { return r.Pipeline.Route(router) })
}
