package bob_test

import (
	"fmt"
	"github.com/arya-analytics/x/kv/memkv"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("Bob", func() {
	It("Should split batch representations easily", func() {
		kv := memkv.Open()
		b := kv.NewBatch()
		for i := 0; i < 10000; i++ {
			b.Set([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%d", i)))
		}
		b.Dl

	})
})
