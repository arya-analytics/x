package iter

func InfiniteSlice[T any](values []T) func() (T, bool) {
	i := 0
	return func() (T, bool) {
		val := values[i]
		if i < (len(values) - 1) {
			i++
		} else {
			i = 0
		}
		return val, true
	}
}

func Once[T any](value T) func() (T, bool) {
	c := 0
	return func() (T, bool) { c++; return value, c == 1 }
}
