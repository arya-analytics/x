package lock_test

import (
	"github.com/arya-analytics/x/lock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Describe("Lock", func() {
	It("Should allow the caller to a acquire the lock", func() {
		l := lock.Idempotent()
		l.Acquire()
		Expect(l.TryAcquire()).To(BeFalse())
	})
	It("Should allow the caller to release the lock", func() {
		l := lock.Idempotent()
		l.Acquire()
		l.Release()
		Expect(l.TryAcquire()).To(BeTrue())
	})
	Specify("idempotent release should be idempotent", func() {
		l := lock.Idempotent()
		l.Acquire()
		l.Release()
		l.Release()
		Expect(l.TryAcquire()).To(BeTrue())
	})
	It("Should prevent multiple callers from acquiring the lock at the same time", func() {
		l := lock.Idempotent()
		c := 0
		wg := sync.WaitGroup{}
		wg2 := sync.WaitGroup{}
		wg2.Add(100)
		wg.Add(100)
		for i := 0; i < 100; i++ {
			go func(i int) {
				wg2.Done()
				defer wg.Done()
				wg2.Wait()
				if ok := l.TryAcquire(); ok {
					c++
				}
			}(i)
		}
		wg.Wait()
		Expect(c).To(Equal(1))
	})

})
