package confluence_test

import (
	"context"
	"github.com/arya-analytics/x/confluence"
	"github.com/cockroachdb/errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("transform", func() {
	It("Should transform values correctly", func() {
		inlet := confluence.NewStream[int](3)
		outlet := confluence.NewStream[int](4)
		square := &confluence.Transform[int]{Transform: func(ctx confluence.Context,
			i int) (int, bool, error) {
			return i * i, true, nil
		}}
		square.InFrom(inlet)
		square.OutTo(outlet)
		ctx, cancel := confluence.DefaultContext()
		square.Flow(ctx)
		inlet.Inlet() <- 1
		inlet.Inlet() <- 2
		Expect(<-outlet.Outlet()).To(Equal(1))
		Expect(<-outlet.Outlet()).To(Equal(4))
		cancel()
		Expect(errors.Is(ctx.WaitOnAll(), context.Canceled)).To(BeTrue())
	})
})
