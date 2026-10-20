// Package keyhash has the hash that the lossy sets of primary keys of the package index remember
// keys as. It is a variable so that the tests of the packages above can make keys have the same
// hash, and see that the results do not depend on it.
package keyhash

import "hash/fnv"

// Func is the hash of a primary key, as the store encodes it.
var Func = func(raw []byte) uint64 {
	h := fnv.New64a()
	h.Write(raw)
	return h.Sum64()
}

// Set replaces the hash for a test, and returns the function that puts the previous one back.
func Set(hash func(raw []byte) uint64) (restore func()) {
	old := Func
	Func = hash
	return func() { Func = old }
}
