package filter

func ExcludeMapKeys[K comparable, V any](m map[K]V, keys ...K) map[K]V {
	f := make(map[K]V)
	for k, v := range m {
		if !ElementOf(keys, k) {
			f[k] = v
		}
	}
	return f
}

func ExcludeSliceValues[V comparable](s []V, values ...V) []V {
	f := make([]V, 0, len(s))
	for _, v := range s {
		if !ElementOf(values, v) {
			f = append(f, v)
		}
	}
	return f
}

func ElementOf[V comparable](s []V, e V) bool {
	for _, v := range s {
		if v == e {
			return true
		}
	}
	return false
}
