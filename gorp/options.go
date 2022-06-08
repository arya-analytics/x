package gorp

import (
	"github.com/arya-analytics/x/binary"
	"go.uber.org/zap"
)

type options struct {
	encoder    binary.Encoder
	decoder    binary.Decoder
	logger     *zap.SugaredLogger
	typePrefix bool
}

type Option func(o *options)

func WithEncoder(encoder binary.Encoder) Option {
	return func(opts *options) { opts.encoder = encoder }
}

func WithDecoder(decoder binary.Decoder) Option {
	return func(opts *options) { opts.decoder = decoder }
}

func WithoutTypePrefix() Option {
	return func(opts *options) { opts.typePrefix = false }
}

func newOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaultOptions(o)
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
	ed := &binary.GobEncoderDecoder{}
	return &options{
		logger:     logger.Sugar(),
		encoder:    ed,
		decoder:    ed,
		typePrefix: true,
	}
}
