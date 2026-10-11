package index

// SetKeyHash replaces the hash of the primary keys for a test, and returns the function that
// puts the real one back.
func SetKeyHash(hash func(raw []byte) uint64) (restore func()) {
	old := keyHash
	keyHash = hash
	return func() { keyHash = old }
}
