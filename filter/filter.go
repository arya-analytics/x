package filter

import (
	"github.com/arya-analytics/x/types"
)

func Map[K comparable, V any](m map[K]V, filter func(k K, v V) bool) map[K]V {
	if m == nil {
		return nil
	}
	r := make(map[K]V)
	for k, v := range m {
		if filter(k, v) {
			r[k] = v
		}
	}
	return r
}

func ExcludeMapKeys[K comparable, V any](m map[K]V, keys ...K) map[K]V {
	f := make(map[K]V)
	for k, v := range m {
		if !ElementOf(keys, k) {
			f[k] = v
		}
	}
	return f
}

func MaxMapKey[K types.Numeric, V any](m map[K]V) K {
	var max K
	first := true
	for k, _ := range m {
		if first || k > max {
			first = false
			max = k
		}
	}
	return max
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

func Slice[V any](s []V, filter func(V) bool) (f []V) {
	for _, v := range s {
		if filter(v) {
			f = append(f, v)
		}
	}
	return f
}

func Duplicates[V comparable](in []V) []V {
	keys, set := make(map[V]bool), make([]V, 0, len(in))
	for _, e := range in {
		if _, value := keys[e]; !value {
			keys[e] = true
			set = append(set, e)
		}
	}
	return set
}
