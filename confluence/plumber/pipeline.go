package plumber

import (
	"github.com/arya-analytics/x/address"
	cfs "github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
)

type Segment[I, O cfs.Value] struct {
	*Pipeline
	cfs.UnarySink[I]
	cfs.AbstractUnarySource[O]
	RouteInletsTo    []address.Address
	RouteOutletsFrom []address.Address
}

func (s *Segment[I, O]) constructEndpointRoutes() {
	for _, addr := range s.RouteInletsTo {
		sink, _ := GetSink[I](s.Pipeline, addr)
		sink.InFrom(s.In)
	}
	for _, addr := range s.RouteOutletsFrom {
		source, _ := GetSource[O](s.Pipeline, addr)
		source.OutTo(s.Out)
	}
}

func (s *Segment[I, O]) RouteInletTo(targets ...address.Address) error {
	s.RouteInletsTo = targets
	for _, addr := range s.RouteInletsTo {
		if _, err := GetSink[O](s.Pipeline, addr); err != nil {
			return err
		}
	}
	return nil
}

func (s *Segment[I, O]) RouteOutletFrom(from ...address.Address) error {
	for _, addr := range from {
		if _, err := GetSource[I](s.Pipeline, addr); err != nil {
			return err
		}
	}
	return nil
}

func (s *Segment[I, O]) Flow(ctx signal.Context, opts ...cfs.Option) {
	s.constructEndpointRoutes()
	s.Pipeline.Flow(ctx, opts...)
}

type Pipeline struct {
	Sources map[address.Address]cfs.Flow
	Sinks   map[address.Address]cfs.Flow
}

func (p *Pipeline) Flow(ctx signal.Context, opts ...cfs.Option) {
	for _, s := range p.Sources {
		s.Flow(ctx, opts...)
	}
	for addr, s := range p.Sinks {
		if _, ok := p.Sources[addr]; !ok {
			s.Flow(ctx, opts...)
		}
	}
}

func New() *Pipeline {
	return &Pipeline{
		Sources: make(map[address.Address]cfs.Flow),
		Sinks:   make(map[address.Address]cfs.Flow),
	}
}

func SetSource[V cfs.Value](p *Pipeline, addr address.Address, source cfs.Source[V]) {
	p.Sources[addr] = source
}

func SetSegment[I, O cfs.Value](
	p *Pipeline,
	addr address.Address,
	segment cfs.Segment[I, O],
) {
	SetSource[I](p, addr, segment)
	SetSink[O](p, addr, segment)
}

func SetSink[V cfs.Value](p *Pipeline, addr address.Address, sink cfs.Sink[V]) {
	p.Sinks[addr] = sink
}

func GetSource[V cfs.Value](p *Pipeline, addr address.Address) (cfs.Source[V], error) {
	rs, ok := p.Sources[addr]
	if !ok {
		return nil, notFound(addr)
	}
	s, ok := rs.(cfs.Source[V])
	if !ok {
		return nil, wrongType(addr, s, rs)
	}
	return s, nil
}

func GetSink[V cfs.Value](p *Pipeline, addr address.Address) (cfs.Sink[V], error) {
	rs, ok := p.Sinks[addr]
	if !ok {
		return nil, notFound(addr)
	}
	s, ok := rs.(cfs.Sink[V])
	if !ok {
		return nil, wrongType(addr, s, rs)
	}
	return s, nil
}

func GetSegment[I, O cfs.Value](p *Pipeline, addr address.Address) (cfs.Segment[I, O], error) {
	rs, err := GetSource[I](p, addr)
	if err != nil {
		return nil, err
	}
	s, ok := rs.(cfs.Segment[I, O])
	if !ok {
		return nil, wrongType(addr, s, rs)
	}
	return s, nil
}

func notFound(addr address.Address) error {
	return errors.Newf(
		"[plumber] - entity (segment, source, sink) at address %s not found",
		addr,
	)
}

func wrongType(addr address.Address, expected, actual interface{}) error {
	return errors.Newf(
		"[plumber] - entity (segment, source, sink) at address %s is of type %T, expected %T",
		addr,
		actual,
		expected,
	)
}
