package gorp_test

import (
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/memkv"
	"github.com/cockroachdb/errors"
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
		db   *gorp.DB
		kvDB kv.DB
	)
	BeforeEach(func() {
		kvDB = memkv.Open()
		db = gorp.Wrap(kvDB)
	})
	AfterEach(func() {
		Expect(kvDB.Close()).To(Succeed())
	})
	It("Should create the entry in storage", func() {
		var entries []entry
		for i := 0; i < 10; i++ {
			entries = append(entries, entry{ID: i, Data: "data"})
		}
		Expect(gorp.NewCreate[int, entry]().Entries(&entries).Exec(db)).To(Succeed())
		var res []entry
		Expect(gorp.NewRetrieve[int, entry]().Entries(&res).Exec(db)).To(Succeed())
		Expect(res).To(Equal(entries))
	})
	It("Should execute operations in a transaction", func() {
		var entries []entry
		txn := db.Begin()
		for i := 0; i < 10; i++ {
			entries = append(entries, entry{ID: i, Data: "data"})
		}
		Expect(gorp.NewCreate[int, entry]().Entries(&entries).Exec(txn)).To(Succeed())
		var res []entry
		err := gorp.NewRetrieve[int, entry]().Entries(&res).Exec(db)
		Expect(errors.Is(err, kv.NotFound))
		Expect(txn.Commit()).To(Succeed())
		Expect(gorp.NewRetrieve[int, entry]().Entries(&res).Exec(db)).To(Succeed())
		Expect(res).To(Equal(entries))
	})
})
