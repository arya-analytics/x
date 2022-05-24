package version

type Counter uint32

func (c Counter) Increment() Counter {
	return c + 1
}
