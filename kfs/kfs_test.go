package kfs_test

import (
	"github.com/arya-analytics/x/kfs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Describe("KFS", func() {
	Describe("new", func() {
		It("Should wrap an existing file system without error", func() {
			_, err := kfs.New[int]("testdata")
			Expect(err).ToNot(HaveOccurred())
		})
	})
	Describe("Acquire and Release", func() {
		var (
			baseFS kfs.BaseFS
			fs     kfs.FS[int]
		)
		BeforeEach(func() {
			baseFS = kfs.NewMem()
			var err error
			fs, err = kfs.New[int]("testdata", kfs.WithExtensionConfig(".test"), kfs.WithFS(baseFS))
			Expect(err).ToNot(HaveOccurred())
		})
		AfterEach(func() {
			Expect(fs.RemoveAll()).To(BeNil())
		})
		It("Should Acquire and Release a single file", func() {
			f, err := fs.Acquire(1)
			Expect(err).ToNot(HaveOccurred())
			Expect(f).ToNot(BeNil())
			fs.Release(1)
		})
		It("Should allow multiple goroutines to access the file ", func() {
			wg := sync.WaitGroup{}
			wg.Add(2)
			go func() {
				_, err := fs.Acquire(1)
				Expect(err).ToNot(HaveOccurred())
				fs.Release(1)
				wg.Done()
			}()
			go func() {
				_, err := fs.Acquire(1)
				Expect(err).ToNot(HaveOccurred())
				fs.Release(1)
				wg.Done()
			}()
			wg.Wait()
		})
		It("Should allow multiple goroutines to write to the file", func() {
			wg := sync.WaitGroup{}
			for i := 0; i < 300; i++ {
				count := 100
				wg.Add(count)
				for i := 0; i < count; i++ {
					go func() {
						defer GinkgoRecover()
						defer wg.Done()
						f, err := fs.Acquire(1)
						Expect(err).ToNot(HaveOccurred())
						_, err = f.Write([]byte("hello"))
						Expect(err).ToNot(HaveOccurred())
						fs.Release(1)
					}()
				}
				wg.Wait()
				f, err := baseFS.Open("testdata/1.test")
				Expect(err).ToNot(HaveOccurred())
				b := make([]byte, count*5)
				_, err = f.Read(b)
				Expect(err).ToNot(HaveOccurred())
				for i := 0; i < count*5; i += 5 {
					Expect(b[i : i+5]).To(Equal([]byte("hello")))
				}
				Expect(f.Close()).To(BeNil())
			}

		})
	})
})
