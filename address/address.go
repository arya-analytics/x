package address

import "strings"

type Address string

func (a Address) String() string { return string(a) }

func (a Address) PortString() string {
	str := strings.Split(string(a), ":")
	return ":" + str[1]
}

type Addressable interface {
	Address() Address
}

type NotFoundError struct {
	Address Address
}

func (n NotFoundError) Error() string {
	return "address not found: " + string(n.Address)
}
