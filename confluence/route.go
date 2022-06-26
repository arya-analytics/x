package confluence

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/errutil"
)

type RouteBuilder[V Value] struct {
	errutil.CatchSimple
	Pipeline *Pipeline[V]
}

func (r *RouteBuilder[V]) Route(router Router[V]) {
	r.CatchSimple.Exec(func() error { return r.Pipeline.Route(router) })
}

func (r *RouteBuilder[V]) RouteInletTo(to ...address.Address) {
	r.CatchSimple.Exec(func() error { return r.Pipeline.RouteInletTo(to...) })
}

func (r *RouteBuilder[V]) RouteOutletFrom(from ...address.Address) {
	r.CatchSimple.Exec(func() error { return r.Pipeline.RouteOutletFrom(from...) })
}

func (r *RouteBuilder[V]) RouteUnary(from, to address.Address, cap int) {
	r.Route(UnaryRouter[V]{FromAddr: from, ToAddr: to, Capacity: cap})
}

func (r *RouteBuilder[V]) PanicIfErr() {
	if r.Error() != nil {
		panic(r.Error())
	}
}
