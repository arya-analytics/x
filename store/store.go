package store

import (
	"github.com/arya-analytics/x/observe"
	"sync"
)

type State = any

type Store[S State] interface {
	SetState(S)
	GetState() S
}

type Observable[S State] interface {
	Store[S]
	observe.Observable[S]
}

type core[S State] struct {
	copy  func(S) S
	mu    sync.RWMutex
	state S
}

func New[S State](copy func(S) S) Store[S] {
	return &core[S]{copy: copy}
}

func (c *core[S]) SetState(state S) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = c.copy(state)
}

func (c *core[S]) GetState() S {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.copy(c.state)
}

type observable[S State] struct {
	Store[S]
	observe.Observer[S]
}

func NewObservable[S State](copy func(S) S) Observable[S] {
	return &observable[S]{
		Store:    &core[S]{copy: copy},
		Observer: observe.New[S](),
	}
}

func (o *observable[S]) SetState(state S) {
	o.Store.SetState(state)
	o.Observer.Notify(state)
}
