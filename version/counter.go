package version

type Counter uint64

func (c Counter) Increment() Counter {
	return c + 1
}

// OlderThan returns true if the Counter is higher than other.
func (c Counter) OlderThan(other Counter) bool {
	return c < other
}

// YoungerThan returns true if the counter is lower than other.
func (c Counter) YoungerThan(other Counter) bool {
	return c > other
}
