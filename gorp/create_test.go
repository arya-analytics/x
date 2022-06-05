package gorp_test

import (
	"context"
	"fmt"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv/memkv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"time"
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

		var model []Model
		for i := 0; i < 1000; i++ {
			model = append(model, Model{ID: fmt.Sprintf("%v", i), Data: "data"})
		}

		ctx := context.Background()
		Expect(gorp.NewCreate[Model]().Entries(&model).Exec(ctx, db)).To(Succeed())

		t0 := time.Now()
		var resModels []Model
		Expect(gorp.NewRetrieve[Model]().Entries(&resModels).Exec(ctx, db)).To(Succeed())
		Expect(resModels).To(Equal(model))
		logrus.Info(time.Since(t0))

		t0 = time.Now()
		var resTwoModels []Model
		err := gorp.NewRetrieve[Model]().Entries(&resTwoModels).WhereKeys("1", "2", "3").Exec(ctx, db)
		Expect(err).To(Succeed())
		logrus.Info(time.Since(t0))

	})
})
