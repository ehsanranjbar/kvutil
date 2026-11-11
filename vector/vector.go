// Package vector provides an index that finds the records of a store by how near a vector of
// theirs, like an embedding, is to a given one.
//
// The index is an extension of a store, kept up to date with it like any other. Searching
// compares the query with every vector that is stored, so the result is exact and the time it
// takes grows with the number of records.
//
// An index can be partitioned (see Partitioned) to search faster for an approximate result: its
// vectors are grouped around centroids when it is built, and a search reads the vectors of the
// nearest groups only. The distances are always exact, as the whole vectors are stored; what is
// approximate is which records are found. The records that are written after the build are in
// no group, and every search reads them, until a Maintainer places them: it keeps the groups
// as records come and go, splitting the ones that grow and dissolving the ones that shrink.
//
// A partitioned index can also be quantized (see Quantized): it keeps a code of one bit per
// dimension of each vector in a partition, which a search reads instead of the vector, and then reads the
// vectors of the best candidates only to give their exact distances and put them in order.
package vector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"
)

var (
	// ErrDimensions is returned for a vector, of a record or of a query, that does not have the
	// number of components that the index is defined with.
	ErrDimensions = errors.New("vector: the vector does not have the dimensions of the index")
	// ErrNotFinite is returned for a vector that has a component that is NaN or infinite, and for
	// one that is so long (about 1e19) that its distances would not be finite numbers.
	ErrNotFinite = errors.New("vector: the vector has a component that is not a finite number")
	// ErrZeroVector is returned for a vector of zeros in an index that measures the cosine, as
	// such a vector has no direction.
	ErrZeroVector = errors.New("vector: a zero vector has no direction to measure a cosine distance by")
	// ErrCorrupt is returned when an entry of the index is not a vector of the dimensions of the
	// index. The index is built again by migrate.Run or Rebuild.
	ErrCorrupt = errors.New("vector: corrupt entry")
)

// Metric is how the distance between two vectors is measured. The less the distance, the nearer.
type Metric int

const (
	// Cosine is 1 minus the cosine of the angle between the vectors: 0 for the same direction,
	// 1 for orthogonal ones, 2 for opposite ones. Lengths do not matter. Vectors are stored
	// normalized to length 1. A zero vector has no direction and is ErrZeroVector.
	Cosine Metric = iota + 1
	// Euclidean is the straight-line (L2) distance.
	Euclidean
	// Dot is the negative of the inner product, so that the greatest inner product is nearest.
	Dot
)

// String returns the name of the metric, which the index records of itself.
func (m Metric) String() string {
	switch m {
	case Cosine:
		return "cosine"
	case Euclidean:
		return "euclidean"
	case Dot:
		return "dot"
	}
	return "Metric(" + strconv.Itoa(int(m)) + ")"
}

// defined reports whether the metric is one of the constants.
func (m Metric) defined() bool {
	return m == Cosine || m == Euclidean || m == Dot
}

// Distance returns the distance between two vectors as the metric measures it, on the vectors
// as they are given (for Cosine it does the normalization itself; a zero vector gives NaN).
// It panics if the vectors do not have the same length or the metric is not a defined one.
func (m Metric) Distance(a, b []float32) float32 {
	if !m.defined() {
		panic(fmt.Sprintf("vector: %v is not a defined metric", m))
	}
	if len(a) != len(b) {
		panic(fmt.Sprintf("vector: the vectors have different lengths, %d and %d", len(a), len(b)))
	}

	// The sums are in float64 here, as this is for a few vectors and not for a search.
	var dot, na, nb, sq float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
		sq += (x - y) * (x - y)
	}

	switch m {
	case Cosine:
		if na == 0 || nb == 0 {
			return float32(math.NaN())
		}
		return float32(max(0, 1-dot/math.Sqrt(na*nb)))
	case Euclidean:
		return float32(math.Sqrt(sq))
	default:
		return float32(-dot)
	}
}

// maxSquaredLength is the greatest squared length of a vector that is not normalized. The
// squared distance between two vectors is at most four times the greater of their squared
// lengths, and their inner product is at most that length itself, so neither overflows a float32.
const maxSquaredLength = math.MaxFloat32 / 4

// prepare validates a vector for the index and returns it as it is to be compared and stored:
// normalized to length 1 for Cosine, which is a copy, and the vector itself for the other
// metrics. The slice it is given is never modified.
func (x *Index[K, V]) prepare(v []float32) ([]float32, error) {
	if len(v) != x.dims {
		return nil, fmt.Errorf("%w: it has %d components and the index has %d", ErrDimensions, len(v), x.dims)
	}
	var sum float64
	for i, c := range v {
		f := float64(c)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, fmt.Errorf("%w: component %d is %v", ErrNotFinite, i, c)
		}
		sum += f * f
	}
	if x.metric != Cosine {
		// The distances are summed in float32. With lengths up to this the sums of two vectors
		// cannot overflow, and so cannot become a NaN that has no place in an order.
		if sum > maxSquaredLength {
			return nil, fmt.Errorf("%w: the vector is too long for its distances to be finite", ErrNotFinite)
		}
		return v, nil
	}

	norm := math.Sqrt(sum)
	if norm == 0 {
		return nil, ErrZeroVector
	}
	unit := make([]float32, len(v))
	for i, c := range v {
		unit[i] = float32(float64(c) / norm)
	}
	return unit, nil
}

// encode returns the stored form of a vector: its components as the bits of 32-bit floats,
// little-endian, one after the other.
func encode(v []float32) []byte {
	b := make([]byte, 4*len(v))
	for i, c := range v {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(c))
	}
	return b
}

// decode reads a stored vector into dst, which has the dimensions of the index. It returns
// ErrCorrupt if the stored bytes are not that long.
func decode(dst []float32, b []byte) error {
	if len(b) != 4*len(dst) {
		return fmt.Errorf("%w: the entry has %d bytes and %d are needed", ErrCorrupt, len(b), 4*len(dst))
	}
	for i := range dst {
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i:]))
	}
	return nil
}

// distance returns the distance of a stored vector from the query, which is prepared. It
// returns ErrCorrupt if the stored bytes are not a vector of the dimensions of the query.
//
// The stored bytes are read as they are summed: decoding them into a vector first took twice
// the time of the sums. The sums are in float32, which is what a search can afford.
func distance(metric Metric, q []float32, stored []byte) (float32, error) {
	if len(stored) != 4*len(q) {
		return 0, fmt.Errorf("%w: the entry has %d bytes and %d are needed", ErrCorrupt, len(stored), 4*len(q))
	}

	switch metric {
	case Cosine:
		// Rounding may take the inner product of two unit vectors past 1.
		return max(0, 1-dotProduct(q, stored)), nil
	case Euclidean:
		return float32(math.Sqrt(float64(squaredDistance(q, stored)))), nil
	default:
		return -dotProduct(q, stored), nil
	}
}

// component returns the component of a stored vector that is in the first four bytes of b.
func component(b []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(b))
}

// The kernels below are what a search spends its time in. dotProduct and squaredDistance are
// them, or the ones of the simd package where a program is built with GOEXPERIMENT=simd and the
// processor has the instructions (kernel_simd.go).

// scalarDotProduct returns the inner product of a vector and a stored one of the same dimensions.
// It sums in four separate accumulators, which the processor can work on at once.
func scalarDotProduct(a []float32, stored []byte) float32 {
	stored = stored[:4*len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		// A slice of exactly the four components, so that their bounds are checked once.
		c := stored[4*i : 4*i+16 : 4*i+16]
		s0 += a[i] * component(c[0:4])
		s1 += a[i+1] * component(c[4:8])
		s2 += a[i+2] * component(c[8:12])
		s3 += a[i+3] * component(c[12:16])
	}
	for ; i < len(a); i++ {
		s0 += a[i] * component(stored[4*i:4*i+4])
	}
	return (s0 + s1) + (s2 + s3)
}

// scalarSquaredDistance returns the sum of the squares of the differences of a vector and a
// stored one of the same dimensions, summed like scalarDotProduct does.
func scalarSquaredDistance(a []float32, stored []byte) float32 {
	stored = stored[:4*len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		c := stored[4*i : 4*i+16 : 4*i+16]
		d0, d1 := a[i]-component(c[0:4]), a[i+1]-component(c[4:8])
		d2, d3 := a[i+2]-component(c[8:12]), a[i+3]-component(c[12:16])
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	for ; i < len(a); i++ {
		d := a[i] - component(stored[4*i:4*i+4])
		s0 += d * d
	}
	return (s0 + s1) + (s2 + s3)
}
