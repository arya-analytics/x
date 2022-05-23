package observe

type Observable[T any] interface {
	Subscribe(s Subscriber[T])
	Unsubscribe(s Subscriber[T])
}

type Observer[T any] interface {
	Observable[T]
	Notify(T)
}

type Subscriber[T any] interface {
	Next(T)
}

type base[T any] struct {
	options     *options
	subscribers map[Subscriber[T]]bool
}

func New[T any](opts ...Option) Observer[T] {
	return &base[T]{
		options:     newOptions(opts...),
		subscribers: make(map[Subscriber[T]]bool),
	}
}

func (b *base[T]) Subscribe(s Subscriber[T]) {
	b.subscribers[s] = true
}

func (b *base[T]) Unsubscribe(s Subscriber[T]) {
	delete(b.subscribers, s)
}

func (b *base[T]) Notify(v T) {
	for s := range b.subscribers {
		s.Next(v)
	}
}
