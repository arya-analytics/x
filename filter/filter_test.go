package filter_test

import (
	"github.com/arya-analytics/x/filter"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("where", func() {
	Describe("Map", func() {
		Describe("Exclude", func() {
			It("Should exclude the given keys", func() {
				m := map[string]interface{}{
					"a": 1,
					"b": 2,
					"c": 3,
				}
				Expect(filter.ExcludeMapKeys(m, "a", "b")).To(Equal(map[string]interface{}{"c": 3}))
			})
		})
	})
	Describe("Slice", func() {
		Describe("Exclude", func() {
			It("Should exclude the given values", func() {
				s := []int{1, 2, 3, 4, 5}
				Expect(filter.ExcludeSliceValues(s, 1, 3, 5)).To(Equal([]int{2, 4}))
			})
		})
	})
})
