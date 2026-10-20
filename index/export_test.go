package index

import "github.com/ehsanranjbar/kvutil/internal/keyhash"

// SetKeyHash replaces the hash of the primary keys for a test, and returns the function that
// puts the real one back.
func SetKeyHash(hash func(raw []byte) uint64) (restore func()) {
	return keyhash.Set(hash)
}
