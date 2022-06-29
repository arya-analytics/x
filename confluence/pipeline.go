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

type pipelineEntity[V Value] struct {
	segment  Segment[V]
	source   Source[V]
	sink     Sink[V]
	flowOpts []FlowOption
}

// NewPipeline opens a new empty Pipeline.
func NewPipeline[V Value]() *Pipeline[V] {
	return &Pipeline[V]{
		segments: make(map[address.Address]Segment[V]),
		sinks:    make(map[address.Address]Sink[V]),
		sources:  make(map[address.Address]Source[V]),
		routes:   make(map[address.Address]map[address.Address]Stream[V]),
	}
}

// Route applies the given router to the Pipeline.
func (p *Pipeline[V]) Route(router Router[V]) error { return router.Route(p) }

func (p *Pipeline[V]) NewRouteBuilder() *RouteBuilder[V] {
	return &RouteBuilder[V]{CatchSimple: *errutil.NewCatchSimple(), Pipeline: p}
}

func (p *Pipeline[V]) route(sourceTarget, sinkTarget address.Address, stream Stream[V]) error {
	source, ok := p.getSource(sourceTarget)
	if !ok {
		return notFound(sourceTarget)
	}
	sink, ok := p.getSink(sinkTarget)
	if !ok {
		return notFound(sinkTarget)
	}

	stream.SetInletAddress(sinkTarget)
	source.OutTo(stream)

	stream.SetOutletAddress(sourceTarget)
	sink.InFrom(stream)

	p.setStream(sourceTarget, sinkTarget, stream)
	return nil
}

// RouteInletTo routes from the inlet of the Pipeline to the given Segment.
func (p *Pipeline[V]) RouteInletTo(targets ...address.Address) error {
	for _, addr := range p.routeInletTo {
		if _, ok := p.getSink(addr); !ok {
			return notFound(addr)
		}
	}
	p.routeInletTo = append(p.routeInletTo, targets...)
	return nil
}

// RouteOutletFrom routes from the given Segment to the outlet of the Pipeline.
func (p *Pipeline[V]) RouteOutletFrom(targets ...address.Address) error {
	for _, addr := range targets {
		if _, ok := p.getSource(addr); !ok {
			return notFound(addr)
		}
	}
	p.routeOutletFrom = append(p.routeOutletFrom, targets...)
	return nil
}

func (p *Pipeline[V]) constructEndpointRoutes() {
	for _, addr := range p.routeOutletFrom {
		source, _ := p.getSource(addr)
		source.OutTo(p.Out)
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

func (p *Pipeline[V]) getStream(sourceTarget, sinkTarget address.Address) (Stream[V], bool) {
	opts := p.routes[sourceTarget]
	if opts == nil {
		p.routes[sourceTarget] = make(map[address.Address]Stream[V])
		return nil, false
	}
	stream, ok := opts[sinkTarget]
	return stream, ok
}

func (p *Pipeline[V]) setStream(sourceTarget, sinkTarget address.Address, stream Stream[V]) {
	if p.routes[sourceTarget] == nil {
		p.routes[sourceTarget] = make(map[address.Address]Stream[V])
	}
	p.routes[sourceTarget][sinkTarget] = stream
}

func (p *Pipeline[V]) getSegment(addr address.Address) (Segment[V], bool) {
	seg, ok := p.segments[addr]
	return seg, ok
}

func (p *Pipeline[V]) setSegment(addr address.Address, seg Segment[V]) {
	p.segments[addr] = seg
}

func (p *Pipeline[V]) getSource(addr address.Address) (Source[V], bool) {
	source, ok := p.sources[addr]
	if ok {
		return source, ok
	}
	source, ok = p.getSegment(addr)
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
