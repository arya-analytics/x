package gorp

type options struct {
	encoder Encoder
	decoder Decoder
}

type Option func(o *options)

func WithEncoder(encoder Encoder) Option { return func(opts *options) { opts.encoder = encoder } }

func WithDecoder(decoder Decoder) Option { return func(opts *options) { opts.decoder = decoder } }
