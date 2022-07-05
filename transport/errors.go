package transport

import (
	"github.com/cockroachdb/errors"
	"io"
)

var (
	// EOF is returned when the end of a stream is reached. This error does not
	// indicate an unexpected failure. For an unexpected EOF, use io.ErrUnexpectedEOF.
	EOF = errors.Wrap(io.EOF, "[x.transport] - end of stream")
	// StreamClosed is returned when a stream is closed and the caller attempts to
	// send a message.
	StreamClosed = errors.New("[x.transport] - stream closed")
)
