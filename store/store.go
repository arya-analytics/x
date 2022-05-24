// Package store implements a simple copy-on-read in memory store. It also
// provides various wrappers to extend functionality:
//
//		Observable - allows the caller to observe changes to the store.
//
package store

import (
	"github.com/arya-analytics/x/observe"
	"sync"
)

// State is the contents of a Store.
type State = any

// Store is a simple copy-on-read in memory store.
// To create a new Store, called store.New().
type Store[S State] interface {
	// SetState sets the state of the store. This is NOT a copy-on write operation,
	// so make sure to provide a copy of the state.
	SetState(S)
	// GetState returns a copy of the current state.
	GetState() S
}

// |||||| CORE ||||||

type core[S State] struct {
	copy  func(S) S
	mu    sync.RWMutex
	state S
}

// New opens a new Store. copy is a function that copies the state.
// It's up to the caller to determine the depth of the copy. Store
// serves as a proxy to the state, so it's important to yield access
// control to the Store (i.e. only alter the state through Store.SetState calls).
func New[S State](copy func(S) S) Store[S] {
	return &core[S]{copy: copy}
}

// SetState implements Store.
func (c *core[S]) SetState(state S) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = state
}

// GetState implements Store.
func (c *core[S]) GetState() S {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.copy(c.state)
}

// |||||| OBSERVABLE ||||||

// Observable is a wrapper around a Store that allows the caller to observe
// State changes. To create a new store.Observable, called store.ObservableWrap().
type Observable[S State] interface {
	Store[S]
	observe.Observable[S]
}

type observable[S State] struct {
	Store[S]
	observe.Observer[S]
}

func ObservableWrap[S State](store Store[S]) Observable[S] {
	return &observable[S]{Store: store, Observer: observe.New[S]()}
}

// SetState implements Store.GetState.
func (o *observable[S]) SetState(state S) {
	o.Store.SetState(state)
	o.Observer.Notify(state)
}
