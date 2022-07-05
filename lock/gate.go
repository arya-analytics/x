package lock

// Gate is an entity that can only be opened once.
// It is used as a lightweight implementation of a thread-unsafe conditional lock.
// Gate is NOT safe for concurrent use.
type Gate struct {
	leave leaveGate
	open  bool
}

type leaveGate uint8

const (
	leaveOpen leaveGate = iota + 1
	leaveClosed
)

// Open attempts to open the Gate. If the Gate is already open, it returns false.
// Otherwise, it sets the Gate open and returns true.
func (g *Gate) Open() bool {
	if g.leave == leaveClosed {
		return false
	}
	if g.leave == leaveOpen {
		return true
	}
	if g.open {
		return false
	}
	g.open = true
	return true
}

// Close closes the Gate. This operation is idempotent.
func (g *Gate) Close() { g.open = false }

// LeaveOpen leaves the Gate open. This means that any calls to Open will return true.
func (g *Gate) LeaveOpen() { g.leave = leaveOpen }

// LeaveClosed leaves the Gate closed. This means that any calls to Open will return false.
func (g *Gate) LeaveClosed() { g.leave = leaveClosed }
