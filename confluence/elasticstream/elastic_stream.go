package elasticstream

import (
	"github.com/arya-analytics/x/address"
	atomicx "github.com/arya-analytics/x/atomic"
	"github.com/arya-analytics/x/buffer"
	"sync"
)

type Stream[V any] struct {
	inletAddr, outletAddr address.Address
	input, output         chan V
	length                chan int
	capacity, resize      chan int
	size                  int
	buffer                *buffer.RingQueue[V]
	c                     *atomicx.Int32Counter
	once                  sync.Once
}

func New[V any]() *Stream[V] {
	ch := &Stream[V]{
		input:    make(chan V),
		output:   make(chan V),
		length:   make(chan int),
		capacity: make(chan int),
		resize:   make(chan int),
		size:     1,
		buffer:   buffer.NewRingBuffer[V](),
		c:        &atomicx.Int32Counter{},
	}
	go ch.elasticallyResize()
	return ch

}

func (ch *Stream[V]) Inlet() chan<- V { return ch.input }

func (ch *Stream[V]) Outlet() <-chan V { return ch.output }

func (ch *Stream[V]) Length() int { return <-ch.length }

func (ch *Stream[V]) Capacity() int {
	val, ok := <-ch.capacity
	if !ok {
		return ch.size
	}
	return val
}

func (ch *Stream[V]) Resize(cap int) { ch.resize <- cap }

func (ch *Stream[V]) Acquire(n int32) { ch.c.Add(n) }

func (ch *Stream[V]) Close() {
	ch.c.Add(-1)
	if ch.c.Value() <= 0 {
		ch.once.Do(func() {
			close(ch.input)
		})
	}
}

func (ch *Stream[V]) SetInletAddress(addr address.Address) { ch.inletAddr = addr }

func (ch *Stream[V]) InletAddress() address.Address { return ch.inletAddr }

func (ch *Stream[V]) SetOutletAddress(addr address.Address) { ch.outletAddr = addr }

func (ch *Stream[V]) OutletAddress() address.Address { return ch.outletAddr }

func (ch *Stream[V]) elasticallyResize() {
	var (
		input, output, nextInput chan V
		next                     V
	)
	nextInput = ch.input
	input = nextInput
	for input != nil || output != nil {
		select {
		case val, ok := <-input:
			if !ok {
				input = nil
				nextInput = nil
			} else {
				ch.buffer.Add(val)
			}
		case output <- next:
			ch.buffer.Remove()
		case ch.size = <-ch.resize:
		case ch.length <- ch.buffer.Length():
		case ch.capacity <- ch.size:
		}

		if ch.buffer.Length() == 0 {
			output = nil
		} else {
			output = ch.output
			next = ch.buffer.Peek()
		}

		if ch.size != -1 && ch.buffer.Length() >= ch.size {
			input = nil
		} else {
			input = nextInput
		}
	}

	close(ch.output)
	close(ch.resize)
	close(ch.length)
	close(ch.capacity)
}
