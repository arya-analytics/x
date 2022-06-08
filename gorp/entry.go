package gorp

// Entry represents a go type that can be queried against a DB.
// All go types must implement the Entry interface so that they can be
// stored. Entry must be able to be serializable by the Encoder and Decoder provided to the DB.
type Entry interface {
	// GorpKey returns a unique key for the entry. gorp.DB will not raise
	// an error if the key is a duplicate. Key must be serializable by Encoder and Decoder.
	GorpKey() interface{}
	// SetOptions returns a slice of options passed to kv.KV.Set.
	SetOptions() []interface{}
}
