package elasticstream_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/arya-analytics/x/confluence/elasticstream"
)

var _ = Describe("Elasticstream", func() {
	es := elasticstream.New[int]()
	It("Should transfer a value from the outlet to the inlet", func() {
		es.Inlet() <- 1
		v := <-es.Outlet()
		Expect(v).To(Equal(1))
	})
	It("Should block when the capacity is reached", func() {
		es.Inlet() <- 1
		select {
		case es.Inlet() <- 2:
			Fail("Should not have been able to write to the inlet")
		default:
		}
		v := <-es.Outlet()
		Expect(v).To(Equal(1))
	})
	It("Should allow the capacity to be resized", func() {
		es.Resize(2)
		es.Inlet() <- 1
		es.Inlet() <- 2
		v := <-es.Outlet()
		Expect(v).To(Equal(1))
		v = <-es.Outlet()
		Expect(v).To(Equal(2))
	})
})
