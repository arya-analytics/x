package store_test

import (
	"github.com/arya-analytics/x/store"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type state struct {
	value int
}

func (s state) Copy() state {
	return s
}

func copyState(s state) state {
	return s
}

type subscriber struct {
	state state
}

func (sub *subscriber) Next(s state) {
	sub.state = s
}

var _ = Describe("Store", func() {
	Describe("core", func() {
		It("Should initialize a basic store correctly", func() {
			s := store.New(copyState)
			state := s.GetState()
			Expect(state.value).To(Equal(0))
		})
	})
	Describe("Observable", func() {
		It("Should initialize an observable store correctly", func() {
			s := store.NewObservable(copyState)
			sub := new(subscriber)
			s.Subscribe(sub)
			s.SetState(state{value: 2})
			Expect(sub.state.value).To(Equal(2))
		})
	})
})
