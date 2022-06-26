package signal

// Closer is an interface for attaching functions that should be called when a
// signal.Context is cancelled.
type Closer interface {
	// AddCloser attaches a function that is called when the context is canceled
	// and all running goroutines have exited.
	AddCloser(closer func() error)
}

// AddCloser implements the Closer interface.
func (c *core) AddCloser(closer func() error) { c.closers = append(c.closers, closer) }

func (c *core) runClosers() {
	c.closerErrors = make([]error, len(c.closers))
	for i, closer := range c.closers {
		c.closerErrors[i] = closer()
	}
}
