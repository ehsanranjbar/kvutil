package harness

import (
	"fmt"
	"math"
)

// Dist is the distribution that the keys of a case are drawn from.
type Dist string

const (
	// Uniform draws every key with the same probability.
	Uniform Dist = "uniform"
	// Zipf draws the key of rank k with a probability that is about 1/(k+1) of the first one's
	// (a Zipf law of exponent 1), and scrambles the ranks over the keys, so that the popular keys
	// are not next to one another in the order of the keys.
	Zipf Dist = "zipf"
)

// Valid reports whether the distribution is one that is known.
func (d Dist) Valid() bool { return d == Uniform || d == Zipf }

// Keys draws the keys of the operations of a case, 0 to n-1, from a distribution. It has no state
// but the parameters, so the workers share one and each draws with its own Rand.
type Keys struct {
	dist Dist
	n    int64
	logN float64 // ln(n+1)
	mult int64   // the scramble: key = rank * mult mod n, a bijection when mult and n are coprime
}

// scrambles are primes (the first is the multiplicative constant of Knuth) of which one is
// coprime to any n that is asked for.
var scrambles = []int64{2654435761, 1000003, 999983, 15485863}

// NewKeys returns the keys of n values drawn from the distribution. n is at least 1.
func NewKeys(dist Dist, n int64) *Keys {
	if n < 1 {
		n = 1
	}
	k := &Keys{dist: dist, n: n, logN: math.Log(float64(n) + 1), mult: 1}
	for _, m := range scrambles {
		if n%m != 0 {
			k.mult = m
			break
		}
	}
	return k
}

// N is the number of keys.
func (k *Keys) N() int64 { return k.n }

// Dist is the distribution.
func (k *Keys) Dist() Dist { return k.dist }

// Next draws a key, 0 to n-1.
func (k *Keys) Next(r *Rand) int64 {
	if k.dist != Zipf {
		return r.Int64N(k.n)
	}
	return k.Scramble(k.Rank(r.Float64()))
}

// Rank maps a uniform number in [0, 1) to a rank, 0 to n-1, with the Zipf law: floor(exp(u *
// ln(n+1))) - 1, which has the distribution without a table. The first rank has about ln 2 /
// ln(n+1) of the draws, and the one of rank k about 1/(k+1) of the first's.
func (k *Keys) Rank(u float64) int64 {
	rank := int64(math.Exp(u*k.logN)) - 1
	return min(max(rank, 0), k.n-1)
}

// Scramble maps a rank to its key. It is a bijection on 0 to n-1: a multiplication by a number
// that is coprime to n, and a shift so that the first rank is not the first key.
func (k *Keys) Scramble(rank int64) int64 {
	return (rank*k.mult + k.n/3) % k.n
}

// String describes the keys for a result.
func (k *Keys) String() string {
	return fmt.Sprintf("%s over %d keys", k.dist, k.n)
}
