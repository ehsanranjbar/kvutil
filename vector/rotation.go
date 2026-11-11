package vector

import (
	"math"
	"math/rand/v2"
	"sync"
)

// rotationSeeds are what the signs of the two rounds of a rotation are drawn from, with the
// dimensions of the index as the second number of the generator. They are constants because the
// codes that are stored are only meaningful for the rotation that made them: every program that
// opens the index must draw the same signs.
var rotationSeeds = [2]uint64{0x7261626974, 0x71756e74}

// rotation is a fixed random rotation of the vectors of an index, which spreads what a vector
// carries evenly over its components. A code of one bit per component is only a good estimate of
// a vector if no component carries much more of it than the others, and the components of an
// embedding do not (a few carry most of its variance).
//
// The vector is padded with zeros to the least power of two that is at least its dimensions, and
// then goes through two rounds of multiplying each component by a sign, drawn at random, and a
// fast Walsh-Hadamard transform. Every step is orthogonal, so the inner product of two vectors
// is the same after the rotation. A rotation is safe for use by many goroutines.
type rotation struct {
	dims int
	// size is the length of a rotated vector: the dimensions rounded up to a power of two.
	size int
	// signs are +1 or -1 for each component, one set for each round.
	signs [2][]float64
	// scale is what a round multiplies by to keep the length of the vector, which the transform
	// multiplies by the square root of its size.
	scale float64
	// scratch holds the float64 buffers that apply works in, so that it makes none of its own.
	scratch sync.Pool
}

// newRotation returns the rotation of vectors of some dimensions, which is the same every time.
// It panics if dims is not positive.
func newRotation(dims int) *rotation {
	if dims <= 0 {
		panic("vector: a rotation must have a positive number of dimensions")
	}
	size := 1
	for size < dims {
		size <<= 1
	}

	r := &rotation{dims: dims, size: size, scale: 1 / math.Sqrt(float64(size))}
	for round := range r.signs {
		rng := rand.New(rand.NewPCG(rotationSeeds[round], uint64(dims)))
		r.signs[round] = make([]float64, size)
		for i := range r.signs[round] {
			r.signs[round][i] = 1
			if rng.IntN(2) == 1 {
				r.signs[round][i] = -1
			}
		}
	}
	r.scratch.New = func() any {
		buf := make([]float64, size)
		return &buf
	}
	return r
}

// apply rotates a vector of the dimensions of the rotation into dst, which has the length of a
// rotated vector. The sums are in float64. It panics if the lengths are not right, as that is a
// mistake of the caller.
func (r *rotation) apply(dst, v []float32) {
	if len(v) != r.dims || len(dst) != r.size {
		panic("vector: the vectors do not have the lengths that the rotation is made for")
	}

	buf := r.scratch.Get().(*[]float64)
	defer r.scratch.Put(buf)
	x := *buf
	for i, c := range v {
		x[i] = float64(c)
	}
	clear(x[len(v):])

	for _, signs := range r.signs {
		for i, s := range signs {
			x[i] *= s
		}
		walshHadamard(x)
		for i := range x {
			x[i] *= r.scale
		}
	}
	for i, c := range x {
		dst[i] = float32(c)
	}
}

// walshHadamard transforms x, whose length is a power of two, in place by the fast
// Walsh-Hadamard transform, which is not normalized.
func walshHadamard(x []float64) {
	for h := 1; h < len(x); h <<= 1 {
		for i := 0; i < len(x); i += 2 * h {
			lower, upper := x[i:i+h], x[i+h:i+2*h]
			for j := range lower {
				a, b := lower[j], upper[j]
				lower[j], upper[j] = a+b, a-b
			}
		}
	}
}
