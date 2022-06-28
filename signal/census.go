package signal

type Census interface {
	// NumForked returns the total number of goroutines that have been started
	// by the Context.
	NumForked() int32
	// NumExited returns the total number of goroutines that have exited.
	NumExited() int32
	// NumRunning returns the total of number of goroutines that are still running.
	NumRunning() int32
}

func (c *core) NumForked() int32 { return c.numForked.Value() }

func (c *core) NumRunning() int32 { return c.numRunning() }

func (c *core) NumExited() int32 { return c.numExited.Value() }
