package pool

type Adapter interface {
	Healthy() bool
}
