package confluence

// Source is a segment that can send values to inlets
type Source[V Value] interface {
	OutTo(inlets ...Inlet[V])
	Flow[V]
}

// CoreSource is a basic implementation of a Source. It implements the Segment interface,
// but will panic if any outlets are added.
type CoreSource[V Value] struct {
	Out []Inlet[V]
}

func (s *CoreSource[V]) InFrom(_ ...Outlet[V]) {
	panic("[confluence.Source] - cannot receive values")
}

func (s *CoreSource[V]) OutTo(inlets ...Inlet[V]) { s.Out = append(s.Out, inlets...) }

func (s *CoreSource[V]) Flow(ctx Context) { panicAbstract() }

type UnarySource[V Value] struct {
	Out Inlet[V]
}

func (u *UnarySource[V]) InFrom(_ ...Outlet[V]) {
	panic("[confluence.Source] - cannot receive values")
}

func (u *UnarySource[V]) OutTo(inlets ...Inlet[V]) {
	if len(inlets) != 1 {
		panic("unary sources must have exactly one outlet")
	}
	u.Out = inlets[0]
}

func (u *UnarySource[V]) Flow(ctx Context) { panicAbstract() }
