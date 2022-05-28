package kv

import (
	"bytes"
	"io"
)

type Flusher interface {
	Flush(w io.Writer) error
}

type Loader interface {
	Load(r io.Reader) error
}

type FlushLoader interface {
	Flusher
	Loader
}

func Flush(kv Writer, key []byte, flusher Flusher) error {
	b := new(bytes.Buffer)
	if err := flusher.Flush(b); err != nil {
		return err
	}
	return kv.Set(key, b.Bytes())
}

func Load(kv Reader, key []byte, loader Loader) error {
	b, err := kv.Get(key)
	if err != nil {
		return err
	}
	return LoadBytes(b, loader)
}

func LoadBytes(b []byte, loader Loader) error {
	return loader.Load(bytes.NewReader(b))
}
