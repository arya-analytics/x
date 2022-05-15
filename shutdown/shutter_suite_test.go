package shutdown_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestShutter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Shutdown Suite")
}
