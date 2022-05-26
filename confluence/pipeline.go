package confluence

import (
	"github.com/arya-analytics/x/address"
)

// Pipeline is a segment that allows the caller to compose a set of sub-segments in a routed manner.
type Pipeline[V Value] struct {
	segments     map[address.Address]Segment[V]
	routes       map[address.Address]map[address.Address]Stream[V]
	inFromRouter Router[V]
	outToRouter  Router[V]
	Linear[V]
}

func NewPipeline[V]() *Pipeline[V] {
	return &Pipeline[V]{
		segments: make(map[address.Address]Segment[V]),
		routes:   make(map[address.Address]map[address.Address]Stream[V]),
	}
}

// Route applies the given router to the Pipeline.
func (p *Pipeline[V]) Route(router Router[V]) error { return router.Route(p) }

func (p *Pipeline[V]) route(from address.Address, to address.Address, stream Stream[V]) error {
	fromSeg, ok := p.getSegment(from)
	if !ok {
		return ErrNotFound
	}
	toSeg, ok := p.getSegment(to)
	if !ok {
		return ErrNotFound
	}
	stream.SetInletAddress(from)
	stream.SetOutletAddress(to)
	fromSeg.OutTo(stream)
	toSeg.InFrom(stream)
	p.setStream(from, to, stream)
	return nil
}

const (
	inletAddr  = "--inlet--"
	outletAddr = "--outlet--"
)

// RouteInletTo routes from the inlet of the Pipeline to the given Segment.
func (p *Pipeline[V]) RouteInletTo(stitch Stitch, cap int, to ...address.Address) error {
	p.inFromRouter = MultiRouter[V]{
		FromAddresses: []address.Address{inletAddr},
		ToAddresses:   to,
		Stitch:        stitch,
		Capacity:      cap,
	}
}

// RouteOutletFrom routes from the given Segment to the outlet of the Pipeline.
func (p *Pipeline[V]) RouteOutletFrom(stitch Stitch, cap int, from ...address.Address) error {
	p.outToRouter = MultiRouter[V]{
		FromAddresses: from,
		ToAddresses:   []address.Address{outletAddr},
		Stitch:        stitch,
		Capacity:      cap,
	}
	return nil
}

// Segment sets the Segment at the given address.
func (p *Pipeline[V]) Segment(addr address.Address, seg Segment[V]) {
	p.setSegment(addr, seg)
}

// Flow implements the Segment interface.
func (p *Pipeline[V]) Flow(ctx Context) {
	if err := p.runEndpointRouters(); err != nil {
		panic(err)
	}
	for _, seg := range p.segments {
		seg.Flow(ctx)
	}
}

func (p *Pipeline[V]) runEndpointRouters() error {
	if err := p.inFromRouter.Route(p); err != nil {
		return err
	}
	return p.outToRouter.Route(p)
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
