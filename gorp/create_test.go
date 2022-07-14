package gorp_test

import (
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/memkv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type entry struct {
	ID   int
	Data string
}

func (m entry) GorpKey() int { return m.ID }

func (m entry) SetOptions() []interface{} { return nil }

var _ = Describe("Create", func() {
	var (
		db *gorp.DB
		kv kv.DB
	)
	BeforeEach(func() {
		kv = memkv.Open()
		db = gorp.Wrap(kv)
	})
	AfterEach(func() {
		Expect(kv.Close()).To(Succeed())
	})
	It("Should create the entry to storage", func() {
		var entries []entry
		for i := 0; i < 10; i++ {
			entries = append(entries, entry{ID: i, Data: "data"})
		}
		Expect(gorp.NewCreate[int, entry]().Entries(&entries).Exec(db)).To(Succeed())
		var res []entry
		Expect(gorp.NewRetrieve[int, entry]().Entries(&res).Exec(db)).To(Succeed())
		Expect(res).To(Equal(entries))
	})
})
