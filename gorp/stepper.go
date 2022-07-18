package gorp

type Chain[K Key, E Entry[K]] struct {
	Links []Retrieve[K, E]
}

func (s *Chain[K, E]) Next() Retrieve[K, E] {
	n := NewRetrieve[K, E]()
	s.Links = append(s.Links, NewRetrieve[K, E]())
	return n
}

func (s *Chain[K, E]) Current() Retrieve[K, E] { return s.Links[len(s.Links)-1] }
