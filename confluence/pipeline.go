package confluence

import (
	"github.com/arya-analytics/x/address"
)

// Pipeline is a segment that allows the caller to compose a set of sub-segments in a routed manner.
type Pipeline[V Value] struct {
	segments      map[address.Address]Segment[V]
	routes        map[address.Address]map[address.Address]Stream[V]
	inFromToAddr  address.Address
	outToFromAddr address.Address
	Linear[V]
}

// Route adds a Stream connecting the Segment at address from to the Segment at address to. Sets the buffer size
// on the Stream to buffer.
func (c *Pipeline[V]) Route(from address.Address, to address.Address, buffer int) error {
	fromSeg, ok := c.getSegment(from)
	if !ok {
		return ErrNotFound
	}
	toSeg, ok := c.getSegment(to)
	if !ok {
		return ErrNotFound
	}
	stream := NewStream[V](buffer)
	stream.SetInletAddress(to)
	stream.SetOutletAddress(from)
	fromSeg.OutTo(stream)
	toSeg.InFrom(stream)
	c.setStream(from, to, stream)
	return nil
}

func (c *Pipeline[V]) route(from address.Address, to address.Address, stream Stream[V]) error {
	fromSeg, ok := c.getSegment(from)
	if !ok {
		return ErrNotFound
	}
	toSeg, ok := c.getSegment(to)
	if !ok {
		return ErrNotFound
	}
	stream.SetInletAddress(from)
	stream.SetOutletAddress(to)
	fromSeg.OutTo(stream)
	toSeg.InFrom(stream)
	c.setStream(from, to, stream)
	return nil
}

// RouteInletTo routes from the inlet of the Pipeline to the given Segment.
func (c *Pipeline[V]) RouteInletTo(to address.Address) error {
	if _, ok := c.getSegment(to); !ok {
		return ErrNotFound
	}
	c.inFromToAddr = to
	return nil
}

// RouteOutletFrom routes from the given Segment to the outlet of the Pipeline.
func (c *Pipeline[V]) RouteOutletFrom(from address.Address) error {
	if _, ok := c.getSegment(from); !ok {
		return ErrNotFound
	}
	c.outToFromAddr = from
	return nil
}

// SetSegment sets the Segment at the given address.
func (c *Pipeline[V]) SetSegment(addr address.Address, seg Segment[V]) {
	c.setSegment(addr, seg)
}

// Flow implements the Segment interface.
func (c *Pipeline[V]) Flow(ctx Context) {
	for _, seg := range c.segments {
		seg.Flow(ctx)
	}
}

func (c *Pipeline[V]) runRouteInFrom() {
	toSeg, _ := c.getSegment(c.inFromToAddr)
	c.inFrom.SetOutletAddress(c.inFromToAddr)
	toSeg.InFrom(c.inFrom)
}

func (c *Pipeline[V]) runRouteOutTo() {
	fromSeg, _ := c.getSegment(c.outToFromAddr)
	c.outTo.SetInletAddress(c.outToFromAddr)
	fromSeg.OutTo(c.outTo)
}

func (c *Pipeline[V]) getStream(from address.Address, to address.Address) (Stream[V], bool) {
	if c.routes == nil {
		c.routes = make(map[address.Address]map[address.Address]Stream[V])
		return nil, false
	}
	opts := c.routes[from]
	if opts == nil {
		c.routes[from] = make(map[address.Address]Stream[V])
		return nil, false
	}
	stream, ok := opts[to]
	return stream, ok
}

func (c *Pipeline[V]) setStream(from address.Address, to address.Address, stream Stream[V]) {
	if c.routes == nil {
		c.routes = make(map[address.Address]map[address.Address]Stream[V])
	}
	if c.routes[from] == nil {
		c.routes[from] = make(map[address.Address]Stream[V])
	}
	c.routes[from][to] = stream
}

func (c *Pipeline[V]) getSegment(addr address.Address) (Segment[V], bool) {
	if c.segments == nil {
		c.segments = make(map[address.Address]Segment[V])
		return nil, false
	}
	seg, ok := c.segments[addr]
	return seg, ok
}

func (c *Pipeline[V]) setSegment(addr address.Address, seg Segment[V]) {
	if c.segments == nil {
		c.segments = make(map[address.Address]Segment[V])
	}
	c.segments[addr] = seg
}
