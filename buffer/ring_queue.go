package buffer

const minQueueLen = 16

// RingQueue represents a single instance of the queue data structure.
type RingQueue[V any] struct {
	buf               []V
	head, tail, count int
}

// NewRingBuffer constructs and returns a new RingQueue.
func NewRingBuffer[V any]() *RingQueue[V] {
	return &RingQueue[V]{buf: make([]V, minQueueLen)}
}

// Length returns the number of elements currently stored in the queue.
func (q *RingQueue[V]) Length() int { return q.count }

// resizes the queue to fit exactly twice its current contents
// this can result in shrinking if the queue is less than half-full
func (q *RingQueue[V]) resize() {
	newBuf := make([]V, q.count<<1)

	if q.tail > q.head {
		copy(newBuf, q.buf[q.head:q.tail])
	} else {
		n := copy(newBuf, q.buf[q.head:])
		copy(newBuf[n:], q.buf[:q.tail])
	}

	q.head = 0
	q.tail = q.count
	q.buf = newBuf
}

// Add puts an element on the end of the queue.
func (q *RingQueue[V]) Add(elem V) {
	if q.count == len(q.buf) {
		q.resize()
	}

	q.buf[q.tail] = elem
	// bitwise modulus
	q.tail = (q.tail + 1) & (len(q.buf) - 1)
	q.count++
}

// Peek returns the element at the head of the queue. This call panics
// if the queue is empty.
func (q *RingQueue[V]) Peek() V {
	if q.count <= 0 {
		panic("queue: Peek() called on empty queue")
	}
	return q.buf[q.head]
}

// Get returns the element at index i in the queue. If the index is
// invalid, the call will panic. This method accepts both positive and
// negative index values. Index 0 refers to the first element, and
// index -1 refers to the last.
func (q *RingQueue[V]) Get(i int) interface{} {
	// If indexing backwards, convert to positive index.
	if i < 0 {
		i += q.count
	}
	if i < 0 || i >= q.count {
		panic("queue: Get() called with index out of range")
	}
	// bitwise modulus
	return q.buf[(q.head+i)&(len(q.buf)-1)]
}

// Remove removes and returns the element from the front of the queue. If the
// queue is empty, the call will panic.
func (q *RingQueue[V]) Remove() (v V) {
	if q.count <= 0 {
		panic("queue: Remove() called on empty queue")
	}
	ret := q.buf[q.head]
	q.buf[q.head] = v
	// bitwise modulus
	q.head = (q.head + 1) & (len(q.buf) - 1)
	q.count--
	// Resize down if buffer 1/4 full.
	if len(q.buf) > minQueueLen && (q.count<<2) == len(q.buf) {
		q.resize()
	}
	return ret
}
