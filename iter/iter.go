package iter

func Slice[T any](values []T) func() T {
	i := 0
	return func() (val T) {
		val = values[i]
		if i < (len(values) - 1) {
			i++
		} else {
			i = 0
		}
		return val
	}
}
