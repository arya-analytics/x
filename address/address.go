package address

type Address string

type Addressable interface {
	Address() Address
}

type NotFoundError struct {
	Address Address
}

func (n NotFoundError) Error() string {
	return "address not found: " + string(n.Address)
}
