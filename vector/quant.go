package vector

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
)

// quantization names the kind of codes that a quantized index keeps, which it records of itself.
// It is a 1-bit code of a vector after a random rotation, with the estimate of an inner product
// that RaBitQ describes.
const quantization = "rabitq1"

// A code is made of a header and the bits, all little-endian:
//
//	factor  4 bytes  a float32, by which the sum of the signs of the bits is multiplied.
//	norm2   4 bytes  a float32, the squared length of the residual of the vector.
//	cr      4 bytes  a float32, the inner product of the centroid and the residual.
//	bits    8 bytes for each 64 components, rounded up: bit i of the code is bit i%64 of
//	        the word i/64. The components are those of the rotated residual, and the bit is set
//	        if the component is not negative. The bits past the last component are 0.
//
// The residual of a vector is what is left of it when the centroid of its partition is taken
// off, so a code is only meaningful with that centroid, and is made again when the vector is
// moved to another partition.
const (
	codeHeader = 12
	// queryBits is the number of bits that a component of a query is rounded to when it is
	// compared with codes.
	queryBits = 4
)

// codeLen returns the length of the code of a vector.
func (r *rotation) codeLen() int {
	return codeHeader + 8*r.words()
}

// words returns the number of 64-bit words that the bits of a code are in.
func (r *rotation) words() int {
	return (r.size + 63) / 64
}

// codeNorm2 returns the squared length of the residual of the vector that a code is of.
func codeNorm2(code []byte) float32 {
	return component(code[4:8])
}

// codeCR returns the inner product of the centroid and the residual of the vector that a code is
// of.
func codeCR(code []byte) float32 {
	return component(code[8:12])
}

// coder makes the codes of vectors. It holds the buffers that it works in, so a coder is for one
// goroutine at a time.
type coder struct {
	rot *rotation
	// residual is a vector of the dimensions of the index, and rotated one of the length of a
	// rotated vector.
	residual, rotated []float32
}

// newCoder returns a coder of codes for the rotation.
func (r *rotation) newCoder() *coder {
	return &coder{rot: r, residual: make([]float32, r.dims), rotated: make([]float32, r.size)}
}

// encode returns the code of a stored vector in the partition of a stored centroid, which are
// both of the dimensions of the index.
//
// The factor is chosen so that a vector whose rotated residual is multiplied by the code
// estimates its own squared length exactly: the bits give the signs of the components, and the
// factor is the squared length over the sum of their magnitudes.
func (c *coder) encode(stored, centroid []byte) []byte {
	var norm2, cr float64
	for i := range c.residual {
		v, m := float64(component(stored[4*i:])), float64(component(centroid[4*i:]))
		r := float32(v - m)
		c.residual[i] = r
		norm2 += float64(r) * float64(r)
		cr += m * float64(r)
	}
	c.rot.apply(c.rotated, c.residual)

	code := make([]byte, c.rot.codeLen())
	signs := code[codeHeader:]
	var l1 float64
	for i, u := range c.rotated {
		l1 += math.Abs(float64(u))
		if u >= 0 {
			signs[i/8] |= 1 << (i % 8)
		}
	}

	// A residual of zeros has no direction, and estimates an inner product of 0 with any query.
	var factor float64
	if l1 != 0 {
		factor = norm2 / l1
	}
	binary.LittleEndian.PutUint32(code[0:], math.Float32bits(float32(factor)))
	binary.LittleEndian.PutUint32(code[4:], math.Float32bits(float32(norm2)))
	binary.LittleEndian.PutUint32(code[8:], math.Float32bits(float32(cr)))
	return code
}

// codeCost returns how much work the code of a vector is, as the number of centroids that it is
// as much work to compare a vector with, or 0 if the index is not quantized. It is what
// decides whether the codes of a batch are worth computing on several goroutines.
func (x *Index[K, V]) codeCost() int {
	if x.rot == nil {
		return 0
	}
	// Two transforms of size log2(size) steps each, over the dimensions of a comparison.
	return 1 + 2*x.rot.size*bits.Len(uint(x.rot.size))/x.dims
}

// quantQuery is a query as it is compared with the codes of a partition. It is made once for a
// search, and is pointed at each partition that the search reads in turn, reusing its buffers.
//
// A code is of the residual of a vector, which is what is left of it when the centroid of its
// partition is taken off, so what a code estimates is the inner product of the residual and the
// query less the centroid (the residual of the query). That is a much smaller vector than the
// query when the query is near the partition, and the error of the estimate is in proportion to
// its length; the estimate for the query itself would be no better than for a vector far from
// the partition. Rotating is linear, so the rotated residual of the query is the rotated query
// less the rotated centroid, which the partitions have: no rotation is made for a partition.
//
// The inner product of the rotated residual of the query and the signs of a code is the sum of
// its components for the bits that are set, twice, less the sum of all. The components are rounded
// to 4 bits so that the sum of those for the set bits is made of popcounts: with a plane for each
// bit of the rounded components, it is the sum over the planes of their weights by the number of
// bits that are set in both the code and the plane.
type quantQuery struct {
	words int
	// rotated is the rotated query.
	rotated []float32
	// A component is lo + step*a, where a is the rounded integer, and sum is the sum of the
	// components as they are rounded, which is what is estimated with, so that the errors of
	// rounding of the components with the bits set and unset are taken off each other. They are
	// those of the partition that the query was last pointed at.
	lo, step, sum float64
	// planes are the bits of the rounded integers, plane b having the bit b of each.
	planes [queryBits][]uint64
}

// prepareQuant rotates a query, which is prepared, for comparing with codes. It must be pointed
// at a partition before it is compared with its codes. The index must be quantized.
func (x *Index[K, V]) prepareQuant(q []float32) *quantQuery {
	qq := &quantQuery{words: x.rot.words(), rotated: make([]float32, x.rot.size)}
	x.rot.apply(qq.rotated, q)
	for b := range qq.planes {
		qq.planes[b] = make([]uint64, qq.words)
	}
	return qq
}

// point rounds the query less a centroid, given as the rotated centroid, for comparing with the
// codes of its partition. The buffers of the query are reused, so the codes of the partition that
// it was last pointed at are no longer compared.
func (qq *quantQuery) point(rotatedCentroid []float32) {
	rotatedCentroid = rotatedCentroid[:len(qq.rotated)]

	lo, hi := math.Inf(1), math.Inf(-1)
	for i, c := range qq.rotated {
		d := float64(c) - float64(rotatedCentroid[i])
		lo, hi = math.Min(lo, d), math.Max(hi, d)
	}
	const levels = 1<<queryBits - 1
	step := (hi - lo) / levels
	if step == 0 {
		step = 1
	}

	for b := range qq.planes {
		clear(qq.planes[b])
	}
	var total int
	for i, c := range qq.rotated {
		d := float64(c) - float64(rotatedCentroid[i])
		a := min(levels, max(0, int(math.Round((d-lo)/step))))
		total += a
		for b := range qq.planes {
			qq.planes[b][i/64] |= uint64(a>>b&1) << (i % 64)
		}
	}
	qq.lo, qq.step = lo, step
	qq.sum = lo*float64(len(qq.rotated)) + step*float64(total)
}

// innerProduct returns the estimate of the inner product of the residual of the vector that a
// code is of and the query less the centroid of the partition that the query is pointed at. The
// code must be as long as the codes of the index are.
func (qq *quantQuery) innerProduct(code []byte) float32 {
	factor := float64(component(code))
	signs := code[codeHeader : codeHeader+8*qq.words]
	p0, p1, p2, p3 := qq.planes[0][:qq.words], qq.planes[1][:qq.words], qq.planes[2][:qq.words], qq.planes[3][:qq.words]

	var set, n0, n1, n2, n3 int
	for w := range qq.words {
		word := binary.LittleEndian.Uint64(signs[8*w:])
		set += bits.OnesCount64(word)
		n0 += bits.OnesCount64(word & p0[w])
		n1 += bits.OnesCount64(word & p1[w])
		n2 += bits.OnesCount64(word & p2[w])
		n3 += bits.OnesCount64(word & p3[w])
	}

	// The sum of the components for the bits that are set.
	s := qq.lo*float64(set) + qq.step*float64(n0+2*n1+4*n2+8*n3)
	return float32(factor * (2*s - qq.sum))
}

// centroidTerm returns what the distance of a vector in a partition from the query has of the
// centroid of the partition: the inner product of the query and the centroid, or for the
// Euclidean distance the squared distance between them. It is computed once for a partition.
func centroidTerm(metric Metric, q []float32, centroid []byte) float32 {
	if metric == Euclidean {
		return squaredDistance(q, centroid)
	}
	return dotProduct(q, centroid)
}

// estimatedDistance returns the estimate of the distance of the vector that a code is of from the
// query, given the term of its partition and the estimate of the inner product of the residual
// and the query less the centroid. For the Euclidean distance it is the squared distance, which
// orders the vectors as the distance does, and is cheaper.
func estimatedDistance(metric Metric, term, inner float32, code []byte) float32 {
	switch metric {
	case Cosine:
		// <q, v> = <q, c> + <q, r> = <q, c> + <q - c, r> + <c, r>
		return max(0, 1-(term+inner+codeCR(code)))
	case Euclidean:
		// |q - c - r|^2 = |q - c|^2 - 2<q - c, r> + |r|^2
		return max(0, term+codeNorm2(code)-2*inner)
	default:
		return -(term + inner + codeCR(code))
	}
}

// appendRotated appends the rotated centroid, given as a stored vector, to some, which are
// rotated centroids one after the other.
func (x *Index[K, V]) appendRotated(rotated []float32, centroid []byte) []float32 {
	v := make([]float32, x.dims)
	_ = decode(v, centroid)
	at := len(rotated)
	rotated = append(rotated, make([]float32, x.rot.size)...)
	x.rot.apply(rotated[at:], v)
	return rotated
}

// checkCode returns ErrCorrupt if the bytes are not a code of the index.
func (x *Index[K, V]) checkCode(rawKey, code []byte) error {
	if want := x.rot.codeLen(); len(code) != want {
		return fmt.Errorf("%w: the code of the key %x has %d bytes and %d are needed", ErrCorrupt, rawKey, len(code), want)
	}
	return nil
}
