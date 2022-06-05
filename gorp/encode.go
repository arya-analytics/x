package gorp

import (
	"bytes"
	"encoding/gob"
)

type Encoder interface {
	Encode(value interface{}) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte, value interface{}) error
}

type defaultEncoderDecoder struct{}

func (e *defaultEncoderDecoder) Encode(value interface{}) ([]byte, error) {
	var buff bytes.Buffer
	err := gob.NewEncoder(&buff).Encode(value)
	return buff.Bytes(), err
}

func (e *defaultEncoderDecoder) Decode(data []byte, value interface{}) error {
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(value)
}
