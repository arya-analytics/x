package plumber

import (
	"github.com/arya-analytics/x/address"
	. "github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/errors"
)

type Pipeline struct {
	Sources map[address.Address]Flow
	Sinks   map[address.Address]Flow
}

func (p *Pipeline) Flow(ctx signal.Context, opts ...Option) {
	for _, s := range p.Sources {
		s.Flow(ctx, opts...)
	}
	for _, s := range p.Sinks {
		s.Flow(ctx, opts...)
	}
}

func New() *Pipeline {
	return &Pipeline{
		Sources: make(map[address.Address]Flow),
		Sinks:   make(map[address.Address]Flow),
	}
}

func SetSource[V Value](p *Pipeline, addr address.Address, source Source[V]) {
	p.Sources[addr] = source
}

func SetSegment[I, O Value](p *Pipeline, addr address.Address, segment Segment[I, O]) {
	SetSource[I](p, addr, segment)
	SetSink[O](p, addr, segment)
}

func SetSink[V Value](p *Pipeline, addr address.Address, sink Sink[V]) {
	p.Sinks[addr] = sink
}

func GetSource[V Value](p *Pipeline, addr address.Address) (Source[V], error) {
	rs, ok := p.Sources[addr]
	if !ok {
		return nil, notFound(addr)
	}
	s, ok := rs.(Source[V])
	if !ok {
		return nil, wrongType(addr, s, rs)
	}
	return s, nil
}

func GetSink[V Value](p *Pipeline, addr address.Address) (Sink[V], error) {
	rs, ok := p.Sinks[addr]
	if !ok {
		return nil, notFound(addr)
	}
	s, ok := rs.(Sink[V])
	if !ok {
		return nil, wrongType(addr, s, rs)
	}
	return s, nil
}

func GetSegment[I, O Value](p *Pipeline, addr address.Address) (Segment[I, O], error) {
	rs, err := GetSource[I](p, addr)
	if err != nil {
		return nil, err
	}
	s, ok := rs.(Segment[I, O])
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
