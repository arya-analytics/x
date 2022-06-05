package gorp

import (
	"go.uber.org/zap"
)

type options struct {
	encoder Encoder
	decoder Decoder
	logger  *zap.SugaredLogger
}

type Option func(o *options)

func WithEncoder(encoder Encoder) Option { return func(opts *options) { opts.encoder = encoder } }

func WithDecoder(decoder Decoder) Option { return func(opts *options) { opts.decoder = decoder } }

func newOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func mergeDefaultOptions(o *options) {
	def := defaultOptions()

	if o.logger == nil {
		o.logger = def.logger
	}

	if o.encoder == nil {
		o.encoder = def.encoder
	}

	if o.decoder == nil {
		o.decoder = def.decoder
	}

}

func defaultOptions() *options {
	logger, _ := zap.NewProduction()
	ed := &defaultEncoderDecoder{}
	return &options{
		logger:  logger.Sugar(),
		encoder: ed,
		decoder: ed,
	}
}
