package rand

import (
	"math/rand"
)

func MapKey[K comparable, V any](m map[K]V) (key K) {
	l := len(m)
	if l == 0 {
		return key
	}
	i, ri := 0, rand.Intn(l)
	for k, _ := range m {
		if i == ri {
			key = k
			break
		}
		i++
	}
	return key
}

func MapValue[K comparable, V any](m map[K]V) V {
	return m[MapKey[K, V](m)]
}

func MapElem[K comparable, V any](m map[K]V) (K, V) {
	k := MapKey[K, V](m)
	return k, m[k]
}

func MapSub[K comparable, V any](m map[K]V, n int) map[K]V {
	om := make(map[K]V)
	for len(om) < n {
		k, v := MapElem[K, V](m)
		om[k] = v
	}
	return om
}

func Elem[V any](options ...V) V {
	return Slice[V](options)
}

func Slice[V any](slice []V) V {
	return slice[rand.Intn(len(slice))]
}
