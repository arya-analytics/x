package set

type Set[V comparable] map[V]struct{}

func (s Set[V]) Add(v V) { s[v] = struct{}{} }

func (s Set[V]) Remove(v V) { delete(s, v) }

func (s Set[V]) Contains(v V) bool { _, ok := s[v]; return ok }
