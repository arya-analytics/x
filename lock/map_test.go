package lock_test

import (
	"github.com/arya-analytics/x/lock"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ApplySink", func() {
	It("Should allow the caller to acquire the lock", func() {
		m := lock.NewMap[int]()
		Expect(m.Acquire(1)).To(Succeed())
	})
	It("Should return an error when the caller tries to acquire a lock that is already held", func() {
		m := lock.NewMap[int]()
		Expect(m.Acquire(1)).To(Succeed())
		Expect(errors.Is(m.Acquire(1), errors.New("[lock] - already held"))).To(BeTrue())
	})
	It("Should allow the called to release the lock", func() {
		m := lock.NewMap[int]()
		Expect(m.Acquire(1)).To(Succeed())
		m.Release(1)
		Expect(m.Acquire(1)).To(Succeed())
	})
})
