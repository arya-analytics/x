package gorp_test

import (
	"context"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv/memkv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type Model struct {
	ID   string
	Data string
}

func (m Model) Key() interface{} { return m.ID }

var _ = Describe("Create", func() {
	It("Should save the model to storage", func() {
		kv := memkv.Open()
		defer kv.Close()
		db := gorp.Wrap(kv)
		model := []Model{{ID: "1", Data: "data"}}
		ctx := context.Background()
		Expect(gorp.NewCreate[Model]().Model(&model).Exec(ctx, db)).To(Succeed())
	})
})
