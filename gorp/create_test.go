package gorp_test

import (
	"context"
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

func (m entry) Key() interface{} { return m.ID }

func (m entry) SetOptions() []interface{} { return nil }

var _ = Describe("Create", func() {
	var (
		db *gorp.DB
		kv kv.KV
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
		ctx := context.Background()
		Expect(gorp.NewCreate[entry]().Entries(&entries).Exec(ctx, db)).To(Succeed())
		var res []entry
		Expect(gorp.NewRetrieve[entry]().Entries(&res).Exec(ctx, db)).To(Succeed())
		Expect(res).To(Equal(entries))
	})
})
