package binary

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"math"
)

func Write(w io.Writer, data interface{}) (err error) {
	return binary.Write(w, Encoding(), data)
}

func Read(r io.Reader, data interface{}) (err error) {
	return binary.Read(r, Encoding(), data)
}

func Marshal(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := Write(buf, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Encoding() binary.ByteOrder {
	return binary.BigEndian
}

func ToFloat64(b []byte) []float64 {
	f64 := make([]float64, len(b)/8)
	for i := 0; i < len(b); i += 8 {
		f64[i/8] = math.Float64frombits(binary.BigEndian.Uint64(b[i:]))
	}
	return f64
}

func Flush(w io.Writer, data interface{}) error { return gob.NewEncoder(w).Encode(data) }

func Load(r io.Reader, data interface{}) error { return gob.NewDecoder(r).Decode(data) }

type Encoder interface {
	Encode(value interface{}) ([]byte, error)
	EncodeStatic(value interface{}) []byte
}

type Decoder interface {
	Decode(data []byte, value interface{}) error
	DecodeStatic(data []byte, value interface{})
}

type EncoderDecoder interface {
	Encoder
	Decoder
}

type GobEncoderDecoder struct{}

func (e *GobEncoderDecoder) Encode(value interface{}) ([]byte, error) {
	if bv, ok := value.([]byte); ok {
		return bv, nil
	}
	var buff bytes.Buffer
	err := gob.NewEncoder(&buff).Encode(value)
	b := buff.Bytes()
	return b, err
}

func (e *GobEncoderDecoder) EncodeStatic(value interface{}) []byte {
	b, err := e.Encode(value)
	if err != nil {
		panic(err)
	}
	return b
}

func (e *GobEncoderDecoder) Decode(data []byte, value interface{}) error {
	if bv, ok := value.(*[]byte); ok {
		*bv = data
	}
	return gob.NewDecoder(bytes.NewBuffer(data)).Decode(value)
}

func (e *GobEncoderDecoder) DecodeStatic(data []byte, value interface{}) {
	if err := e.Decode(data, value); err != nil {
		panic(err)
	}
}

func Copy(value []byte) (copied []byte) {
	copied = make([]byte, len(value))
	copy(copied, value)
	return copied
}
