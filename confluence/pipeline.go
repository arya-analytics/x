package confluence

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/errutil"
	"github.com/arya-analytics/x/signal"
)

// Pipeline is a segment that allows the caller to compose a set of sub-segments in a routed manner.
type Pipeline[V Value] struct {
	segments        map[address.Address]Segment[V]
	sources         map[address.Address]Source[V]
	sinks           map[address.Address]Sink[V]
	routes          map[address.Address]map[address.Address]Stream[V]
	routeInletTo    []address.Address
	routeOutletFrom []address.Address
	Linear[V]
}

// NewPipeline opens a new empty Pipeline.
func NewPipeline[V Value]() *Pipeline[V] {
	return &Pipeline[V]{
		segments: make(map[address.Address]Segment[V]),
		routes:   make(map[address.Address]map[address.Address]Stream[V]),
	}
}

// Route applies the given router to the Pipeline.
func (p *Pipeline[V]) Route(router Router[V]) error { return router.Route(p) }

func (p *Pipeline[V]) NewRouteBuilder() *RouteBuilder[V] {
	return &RouteBuilder[V]{CatchSimple: *errutil.NewCatchSimple(), Pipeline: p}
}

func (p *Pipeline[V]) route(fromTarget, toTarget address.Address, stream Stream[V]) error {
	from, ok := p.getSource(fromTarget)
	if !ok {
		return ErrNotFound
	}
	to, ok := p.getSink(toTarget)
	if !ok {
		return ErrNotFound
	}

	stream.SetInletAddress(toTarget)
	from.OutTo(stream)

	stream.SetOutletAddress(fromTarget)
	to.InFrom(stream)

	p.setStream(fromTarget, toTarget, stream)
	return nil
}

// RouteInletTo routes from the inlet of the Pipeline to the given Segment.
func (p *Pipeline[V]) RouteInletTo(to ...address.Address) error {
	for _, addr := range p.routeInletTo {
		if _, ok := p.getSegment(addr); !ok {
			return ErrNotFound
		}
	}
	p.routeInletTo = append(p.routeInletTo, to...)
	return nil
}

// RouteOutletFrom routes from the given Segment to the outlet of the Pipeline.
func (p *Pipeline[V]) RouteOutletFrom(from ...address.Address) error {
	for _, addr := range from {
		if _, ok := p.getSegment(addr); !ok {
			return ErrNotFound
		}
	}
	p.routeOutletFrom = append(p.routeOutletFrom, from...)
	return nil
}

func (p *Pipeline[V]) constructEndpointRoutes() {
	for _, addr := range p.routeOutletFrom {
		seg, _ := p.getSource(addr)
		seg.OutTo(p.Out)
	}
	for _, addr := range p.routeInletTo {
		seg, _ := p.getSink(addr)
		seg.InFrom(p.In)
	}
}

// Segment sets the Segment at the given address.
func (p *Pipeline[V]) Segment(addr address.Address, seg Segment[V]) {
	p.setSegment(addr, seg)
}

// Source sets the Source at the given address.
func (p *Pipeline[V]) Source(addr address.Address, src Source[V]) {
	p.setSource(addr, src)
}

// Sink sets the Sink at the given address.
func (p *Pipeline[V]) Sink(addr address.Address, sink Sink[V]) {
	p.setSink(addr, sink)
}

// Flow implements the Segment interface.
func (p *Pipeline[V]) Flow(ctx signal.Context) {
	p.constructEndpointRoutes()
	for _, seg := range p.segments {
		seg.Flow(ctx)
	}
	for _, source := range p.sources {
		source.Flow(ctx)
	}
	for _, sink := range p.sinks {
		sink.Flow(ctx)
	}
}

func (p *Pipeline[V]) getStream(from address.Address, to address.Address) (Stream[V], bool) {
	opts := p.routes[from]
	if opts == nil {
		p.routes[from] = make(map[address.Address]Stream[V])
		return nil, false
	}
	stream, ok := opts[to]
	return stream, ok
}

func (p *Pipeline[V]) setStream(from address.Address, to address.Address, stream Stream[V]) {
	if p.routes[from] == nil {
		p.routes[from] = make(map[address.Address]Stream[V])
	}
	p.routes[from][to] = stream
}

func (p *Pipeline[V]) getSegment(addr address.Address) (Segment[V], bool) {
	seg, ok := p.segments[addr]
	return seg, ok
}

func (p *Pipeline[V]) setSegment(addr address.Address, seg Segment[V]) {
	p.segments[addr] = seg
}

func (p *Pipeline[V]) getSource(target address.Address) (Source[V], bool) {
	source, ok := p.sources[target]
	if ok {
		return source, ok
	}
	source, ok = p.getSegment(target)
	return source, ok
}

func (p *Pipeline[V]) setSource(addr address.Address, source Source[V]) {
	p.sources[addr] = source
}

func (p *Pipeline[V]) getSink(addr address.Address) (Sink[V], bool) {
	sink, ok := p.sinks[addr]
	if ok {
		return sink, ok
	}
	sink, ok = p.getSegment(addr)
	return sink, ok
}

func (p *Pipeline[V]) setSink(addr address.Address, sink Sink[V]) {
	p.sinks[addr] = sink
}
