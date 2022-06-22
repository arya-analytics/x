package transport

import (
	"fmt"
	"github.com/arya-analytics/x/address"
	"github.com/cockroachdb/errors"
	"io"
)

var (
	// TargetNotFound is returned when an addressable target cannot be found in the
	// network. Users of this error should ideally wrap this error with details
	// about the attempted request.
	TargetNotFound = errors.New("[x.transport] - target not found")
	// EOF is returned when the end of a stream is reached. This error does not
	// indicate an unexpected failure. For an unexpected EOF, use io.ErrUnexpectedEOF.
	EOF = errors.Wrap(io.EOF, "[x.transport] - end of stream")
)

func WrapNotFoundWithTarget(target address.Address) error {
	return errors.Wrap(TargetNotFound, fmt.Sprintf("no route to target %s", target))
}
