package iter

func InfiniteSlice[T any](values []T) func() T {
	i := 0
	return func() T {
		val := values[i]
		if i < (len(values) - 1) {
			i++
		} else {
			i = 0
		}
		return val
	}
}

func Once[T any](value T) func() (T, bool) {
	c := 0
	return func() (T, bool) { c++; return value, c == 1 }
}

type keyValuePair[K comparable, V any] struct {
	key   K
	value V
}

func Map[K comparable, V any](values map[K]V) func() (K, V, bool) {
	var (
		i     = 0
		pairs = make([]keyValuePair[K, V], len(values))
	)
	for k, v := range values {
		pairs[i] = keyValuePair[K, V]{k, v}
		i++
	}
	i = 0
	return func() (k K, v V, ok bool) {
		if i >= (len(values) - 1) {
			return k, v, false
		}
		val := pairs[i]
		i++
		return val.key, val.value, true
	}
}
