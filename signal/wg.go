package signal

// WaitGroup provides methods for detecting and waiting for the exit of goroutines
// managed by a signal.Conductor.
type WaitGroup interface {
	// WaitOnAny waits for any of the running goroutines to exit with an error.
	// If allowCtx is false, WaitOnAny will ignore context errors. WaitOnAny is NOT
	// safe to call concurrently with other Wait methods.
	WaitOnAny(allowCtx bool) error
	// WaitOnAll waits for all running goroutines to exit, then proceeds to return
	// the first non-nil error (returns nil if all errors are nil). Returns nil
	// if no goroutines are running. WaitOnAll. is NOT safe to call concurrently
	// with any other wait methods.
	WaitOnAll() error
	// Stopped returns a channel that is closed when the context is canceled and all
	// running goroutines have exited.
	Stopped() <-chan struct{}
}

// WaitOnAny implements the WaitGroup interface.
func (c *core) WaitOnAny(allowCtx bool) error {
	return c.waitForNToExit(1, allowCtx)
}

// WaitOnAll implements the WaitGroup interface.
func (c *core) WaitOnAll() error {
	return c.waitForNToExit(c.numRunning(), true)
}

// Stopped implements the WaitGroup interface.
func (c *core) Stopped() <-chan struct{} { return c.stopped }

func (c *core) waitForNToExit(count int32, allowNil bool) error {
	var (
		numExited int32
		err       error
	)
	if c.numRunning() == 0 {
		c.maybeStop()
		return nil
	}
	for _err := range c.fatal {
		if _err != nil {
			numExited++
			if moreSignificant(_err, err) {
				err = _err
			}
		} else if allowNil {
			numExited++
		}
		if numExited >= count {
			break
		}
	}
	if len(c.closerErrors) > 0 && moreSignificant(c.closerErrors[0], err) {
		return c.closerErrors[0]
	}
	return err
}
