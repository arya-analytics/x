package confluence_test

import (
	"github.com/arya-analytics/x/address"
	"github.com/arya-analytics/x/confluence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Stream", func() {
	Describe("Address", func() {
		It("Should set the inlet address properly", func() {
			stream := confluence.NewStream[int](0)
			addr := address.Address("inlet")
			stream.SetInletAddress(addr)
			Expect(stream.InletAddress()).To(Equal(addr))
		})
		It("Should set the outlet address properly", func() {
			stream := confluence.NewStream[int](0)
			addr := address.Address("outlet")
			stream.SetOutletAddress(addr)
			Expect(stream.OutletAddress()).To(Equal(addr))
		})
	})
})
