package gorp

type Compound[K Key, E Entry[K]] struct {
	Clauses []Retrieve[K, E]
}

func (s *Compound[K, E]) Next() Retrieve[K, E] {
	n := NewRetrieve[K, E]()
	s.Clauses = append(s.Clauses, NewRetrieve[K, E]())
	return n
}

func (s *Compound[K, E]) Current() Retrieve[K, E] { return s.Clauses[len(s.Clauses)-1] }
