package gorp

type Encoder interface {
	Encode(value interface{}) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte, value interface{}) error
}
