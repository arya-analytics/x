package gorp_test

import (
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/memkv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/arya-analytics/x/gorp"
)

var _ = Describe("getAttributes", func() {
	var (
		db      *gorp.DB
		kv      kv.DB
		entries []entry
	)
	BeforeEach(func() {
		kv = memkv.Open()
		db = gorp.Wrap(kv)
		for i := 0; i < 10; i++ {
			entries = append(entries, entry{ID: i, Data: "data"})
		}
		Expect(gorp.NewCreate[int, entry]().Entries(&entries).Exec(db)).To(Succeed())
	})
	AfterEach(func() {
		Expect(kv.Close()).To(Succeed())
	})
	Describe("WhereKeys", func() {
		It("Should retrieve the entry by key", func() {
			var res []entry
			Expect(gorp.NewRetrieve[int, entry]().WhereKeys(entries[0].GorpKey()).Entries(&res).Exec(db)).To(Succeed())
			Expect(res).To(Equal([]entry{entries[0]}))
		})
	})
	Describe("Where", func() {
		It("Should retrieve the entry by a filter parameter", func() {
			var res []entry
			Expect(gorp.NewRetrieve[int, entry]().
				Entries(&res).
				Where(func(e entry) bool { return e.ID == entries[1].ID }).
				Exec(db),
			).To(Succeed())
			Expect(res).To(Equal([]entry{entries[1]}))
		})
		It("Should support multiple filters", func() {
			var res []entry
			Expect(gorp.NewRetrieve[int, entry]().
				Entries(&res).
				Where(func(e entry) bool { return e.ID == entries[1].ID }).
				Where(func(e entry) bool { return e.ID == entries[2].ID }).
				Exec(db),
			).To(Succeed())
			Expect(res).To(Equal([]entry{entries[1], entries[2]}))
		})
	})
})
