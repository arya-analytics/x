package pebblekv_test

import (
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/pebblekv"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pebble", func() {
	var (
		kve kv.KV
	)
	BeforeEach(func() {
		db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
		Expect(err).NotTo(HaveOccurred())
		kve = pebblekv.Wrap(db)
	})
	AfterEach(func() {
		Expect(kve.Close()).To(Succeed())
	})
	Describe("Iterator", func() {
		Describe("Prefix", func() {
			It("Should iterate over keys with the same prefix", func() {
				Expect(kve.Set([]byte("foo"), []byte("bar"))).To(Succeed())
				Expect(kve.Set([]byte("foobar"), []byte("baz"))).To(Succeed())
				Expect(kve.Set([]byte("foobaz"), []byte("bar"))).To(Succeed())

				iter := kve.IterPrefix([]byte("foo"))
				c := 0
				for iter.First(); iter.Valid(); iter.Next() {
					c++
				}
				Expect(c).To(Equal(3))
				Expect(iter.Close()).To(Succeed())
			})
		})
		Describe("Range", func() {
			It("Should iterate over a range of keys", func() {
				Expect(kve.Set([]byte("foo"), []byte("bar"))).To(Succeed())
				Expect(kve.Set([]byte("foobar"), []byte("baz"))).To(Succeed())
				Expect(kve.Set([]byte("foobaz"), []byte("bar"))).To(Succeed())

				iter := kve.IterRange([]byte("foo"), []byte("foobar"))
				c := 0
				for iter.First(); iter.Valid(); iter.Next() {
					c++
				}
				Expect(c).To(Equal(1))
				Expect(iter.Close()).To(Succeed())
			})
		})
	})
	Describe("Get", func() {
		It("Should get a value", func() {
			Expect(kve.Set([]byte("foo"), []byte("bar"))).To(Succeed())
			val, err := kve.Get([]byte("foo"))
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]byte("bar")))
		})
	})
	Describe("Delete", func() {
		It("Should delete a value", func() {
			Expect(kve.Set([]byte("foo"), []byte("bar"))).To(Succeed())
			Expect(kve.Delete([]byte("foo"))).To(Succeed())
			_, err := kve.Get([]byte("foo"))
			Expect(err).To(Equal(kv.ErrNotFound))
		})
	})
	Describe("String", func() {
		It("Should return a string", func() {
			Expect(kve.String()).To(Equal("pebbleKV"))
		})
	})

})
