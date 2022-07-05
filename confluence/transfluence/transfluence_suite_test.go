package transfluence_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTransfluence(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transfluence Suite")
}
