package kv

// Batch  is an ordered collection of key-value operations on the DB. Batch implements
// the Reader interface, and will read key-value pairs from both the Batch and underlying DB.
// A batch must be committed for its changes to be persisted.
type Batch interface {
	// Set implements the Writer interface, although the extra options are ignored.
	Set(key, value []byte, _ ...interface{}) error
	// Delete implements the Writer interface.
	Delete(key []byte) error
	Reader
	// Commit persists the batch to the underlying DB.
	Commit(opts ...interface{}) error
}
