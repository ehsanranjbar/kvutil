package harness

import "math/rand/v2"

// Rand is the generator of random numbers of a worker.
type Rand = rand.Rand

// NewRand returns a generator that gives the same numbers for the same seed and stream.
func NewRand(seed, stream uint64) *Rand {
	return rand.New(rand.NewPCG(seed, stream))
}
