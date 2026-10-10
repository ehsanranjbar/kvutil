package kvutil

// RawKVPair is a key-value pair with both key and value as byte slices.
type RawKVPair struct {
	Key   []byte
	Value []byte
}

// NewRawKVPair creates a new RawKVPair.
func NewRawKVPair(key, value []byte) RawKVPair {
	return RawKVPair{Key: key, Value: value}
}
