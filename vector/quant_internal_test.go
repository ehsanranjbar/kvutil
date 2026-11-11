package vector

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// gaussian returns a vector of independent normal components.
func gaussian(rng *rand.Rand, dims int) []float32 {
	v := make([]float32, dims)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

// dotF64 and norm are the inner product and the length of vectors, summed in float64.
func dotF64(a, b []float32) float64 {
	var s float64
	for i := range a {
		s += float64(a[i]) * float64(b[i])
	}
	return s
}

func norm(a []float32) float64 { return math.Sqrt(dotF64(a, a)) }

// quantized returns an index of some dimensions that is only as much as the codes need.
func quantized(dims int) *Index[int, int] {
	return &Index[int, int]{dims: dims, rot: newRotation(dims)}
}

func TestRotationIsOrthogonal(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, dims := range []int{1, 2, 3, 64, 96, 384} {
		t.Run(fmt.Sprint(dims), func(t *testing.T) {
			r := newRotation(dims)
			size := 1
			for size < dims {
				size <<= 1
			}
			require.Equal(t, size, r.size, "the least power of two that holds the dimensions")

			for range 20 {
				x, y := gaussian(rng, dims), gaussian(rng, dims)
				rx, ry := make([]float32, size), make([]float32, size)
				r.apply(rx, x)
				r.apply(ry, y)

				require.InDelta(t, norm(x), norm(rx), 1e-4*norm(x), "the length")
				require.InDelta(t, dotF64(x, y), dotF64(rx, ry), 1e-4*norm(x)*norm(y), "the inner product")
			}
		})
	}
}

func TestRotationIsDeterministic(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, dims := range []int{3, 96, 384} {
		x := gaussian(rng, dims)
		a, b := newRotation(dims), newRotation(dims)
		ra, rb := make([]float32, a.size), make([]float32, b.size)
		a.apply(ra, x)
		b.apply(rb, x)
		require.Equal(t, ra, rb)

		// The same rotation can be applied again and by many goroutines, and gives the same.
		again := make([]float32, a.size)
		a.apply(again, x)
		require.Equal(t, ra, again)
	}

	// The signs are those of the generators of the seeds, which every program must draw alike for
	// the codes that are stored to mean the same to all.
	for round, seed := range []uint64{0x7261626974, 0x71756e74} {
		rng := rand.New(rand.NewPCG(seed, 100))
		signs := newRotation(100).signs[round]
		require.Len(t, signs, 128)
		for i, s := range signs {
			want := 1.0
			if rng.IntN(2) == 1 {
				want = -1
			}
			require.Equal(t, want, s, "round %d, component %d", round, i)
		}
	}
	require.NotEqual(t, newRotation(8).signs[0], newRotation(9).signs[0][:8], "the dimensions are part of the seed")
}

// A vector that is all in one component is spread over all of them, which is what the rotation is
// for: the bits of a code carry something about every component of the vector then.
func TestRotationSpreads(t *testing.T) {
	for _, dims := range []int{64, 96, 384} {
		r := newRotation(dims)
		for _, hot := range []int{0, dims / 2, dims - 1} {
			x := make([]float32, dims)
			x[hot] = 1
			u := make([]float32, r.size)
			r.apply(u, x)

			var most float64
			for _, c := range u {
				most = math.Max(most, math.Abs(float64(c)))
			}
			require.Less(t, most, 6/math.Sqrt(float64(r.size)), "dims %d, component %d", dims, hot)
		}
	}
}

func TestRotationPanicsOnWrongLengths(t *testing.T) {
	r := newRotation(3)
	require.Panics(t, func() { r.apply(make([]float32, 4), make([]float32, 2)) })
	require.Panics(t, func() { r.apply(make([]float32, 3), make([]float32, 3)) })
	require.Panics(t, func() { newRotation(0) })
	require.NotPanics(t, func() { r.apply(make([]float32, 4), make([]float32, 3)) })
}

func TestCodeLayout(t *testing.T) {
	for dims, want := range map[int]int{1: 12 + 8, 3: 12 + 8, 64: 12 + 8, 65: 12 + 16, 96: 12 + 16, 384: 12 + 64} {
		require.Equal(t, want, newRotation(dims).codeLen(), dims)
	}

	// The header has the numbers that the estimates need, and they are right.
	rng := rand.New(rand.NewPCG(5, 6))
	x := quantized(96)
	v, c := gaussian(rng, 96), gaussian(rng, 96)
	code := x.rot.newCoder().encode(encode(v), encode(c))
	require.Len(t, code, x.rot.codeLen())

	r := make([]float32, 96)
	for i := range r {
		r[i] = v[i] - c[i]
	}
	require.InDelta(t, dotF64(r, r), codeNorm2(code), 1e-4)
	require.InDelta(t, dotF64(c, r), codeCR(code), 1e-4)
	require.Greater(t, component(code), float32(0))

	// The bits are the signs of the rotated residual, the first one first in the lowest bit.
	u := make([]float32, x.rot.size)
	x.rot.apply(u, r)
	var l1 float64
	for i, c := range u {
		l1 += math.Abs(float64(c))
		set := code[codeHeader+i/8]>>(i%8)&1 == 1
		require.Equal(t, c >= 0, set, "the bit %d", i)
	}
	require.InDelta(t, dotF64(r, r)/l1, component(code), 1e-5*dotF64(r, r)/l1, "the factor")
}

// estimate returns the estimate of the inner product of the residual of a code and the query less
// the centroid of its partition, the way a search makes it: with the rotated query and the rotated
// centroid.
func estimate(x *Index[int, int], q, centroid []float32, code []byte) float64 {
	qq := x.prepareQuant(q)
	qq.point(x.appendRotated(nil, encode(centroid)))
	return float64(qq.innerProduct(code))
}

// The estimate of the inner product of the query less the centroid and a residual is close to the
// true one, and the closer to it the more components there are: the error of one bit per component
// is about 0.8/sqrt(components) of the product of the lengths. The codes are the estimate that
// the search ranks the vectors by, so what matters is that it is unbiased and that its errors are
// small against the spread of the inner products of the vectors of an index.
func TestEstimateOfTheInnerProduct(t *testing.T) {
	const dims = 384
	rng := rand.New(rand.NewPCG(7, 8))
	x := quantized(dims)
	coder := x.rot.newCoder()

	var (
		errSum, errAbs, scale float64
		truths, estimates     []float64
	)
	const trials = 400
	for range trials {
		v, c, q := gaussian(rng, dims), gaussian(rng, dims), gaussian(rng, dims)
		code := coder.encode(encode(v), encode(c))

		// The residual and the query less the centroid, as the code and the estimate have them.
		r, d := make([]float32, dims), make([]float32, dims)
		for i := range r {
			r[i] = float32(float64(v[i]) - float64(c[i]))
			d[i] = float32(float64(q[i]) - float64(c[i]))
		}
		est := estimate(x, q, c, code)
		truth := dotF64(d, r)

		errSum += est - truth
		errAbs += math.Abs(est - truth)
		scale += norm(d) * norm(r)
		truths, estimates = append(truths, truth), append(estimates, est)
	}
	relative := errAbs / scale
	t.Logf("mean absolute error %.4f of the product of the lengths, mean error %.4f, correlation %.3f",
		relative, errSum/scale, correlation(truths, estimates))

	// The error of one bit per component is about 0.8/sqrt(512) = 0.035 of the product of the
	// lengths, which is 0.028 for the mean of its absolute value, as was seen; the rounding of the
	// query adds nothing that shows. 0.06 is twice that, and a third of the 0.2 that a code that was
	// wrong would be at.
	require.Less(t, relative, 0.06)
	require.Less(t, math.Abs(errSum)/scale, 0.005, "the estimate has no bias")

	// The inner product of two random vectors of 384 components is itself only 1/sqrt(384) = 0.05 of
	// the product of the lengths, so an error of 0.035 leaves a correlation of 1/sqrt(1+(0.035/0.05)^2),
	// about 0.82. The vectors of an index are nearer to the query than these are.
	require.Greater(t, correlation(truths, estimates), 0.75)
}

// The error is in proportion to the length of the query less the centroid, so a query that is
// near the centroid of a partition is estimated closely, against the inner product with the
// query itself. That is why the query is compared with the codes of a partition as a residual.
func TestEstimateIsCloserForAQueryNearTheCentroid(t *testing.T) {
	const dims = 384
	rng := rand.New(rand.NewPCG(21, 22))
	x := quantized(dims)
	coder := x.rot.newCoder()

	var near, far float64
	for range 200 {
		c := gaussian(rng, dims)
		v := make([]float32, dims)
		q := make([]float32, dims)
		for i := range v {
			v[i] = c[i] + 0.3*float32(rng.NormFloat64())
			q[i] = c[i] + 0.3*float32(rng.NormFloat64())
		}
		code := coder.encode(encode(v), encode(c))
		r := make([]float32, dims)
		for i := range r {
			r[i] = float32(float64(v[i]) - float64(c[i]))
		}

		// <q, r> is <q - c, r> + <c, r>, and the code has the second exactly.
		got := estimate(x, q, c, code) + float64(codeCR(code))
		near += math.Abs(got - dotF64(q, r))
		// What the estimate of <q, r> by the rotated query itself comes to, for comparison.
		qq := x.prepareQuant(q)
		qq.point(make([]float32, x.rot.size))
		far += math.Abs(float64(qq.innerProduct(code)) - dotF64(q, r))
	}
	t.Logf("mean absolute error %.4f as a residual, %.4f as the query", near/200, far/200)
	require.Less(t, near, far/2)
}

// correlation returns the Pearson correlation of two samples.
func correlation(a, b []float64) float64 {
	var ma, mb float64
	for i := range a {
		ma += a[i]
		mb += b[i]
	}
	ma, mb = ma/float64(len(a)), mb/float64(len(b))
	var cov, va, vb float64
	for i := range a {
		cov += (a[i] - ma) * (b[i] - mb)
		va += (a[i] - ma) * (a[i] - ma)
		vb += (b[i] - mb) * (b[i] - mb)
	}
	return cov / math.Sqrt(va*vb)
}

// A query that is the centroid and the residual has the residual with itself for the estimate,
// which is its squared length, and is what the factor of a code is chosen for, up to the
// rounding of the query to 4 bits.
func TestEstimateOfTheResidualWithItself(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	for _, dims := range []int{3, 96, 384} {
		x := quantized(dims)
		coder := x.rot.newCoder()
		for range 20 {
			v, c := gaussian(rng, dims), gaussian(rng, dims)
			r, q := make([]float32, dims), make([]float32, dims)
			for i := range r {
				r[i] = float32(float64(v[i]) - float64(c[i]))
				q[i] = float32(float64(c[i]) + float64(r[i]))
			}

			est := estimate(x, q, c, coder.encode(encode(v), encode(c)))
			want := dotF64(r, r)
			// Few components are rounded coarser, in relation to the sum of the magnitudes.
			require.InDelta(t, want, est, 0.15*want, "dims %d", dims)
		}
	}
}

func TestEstimateOfAZeroResidual(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	x := quantized(96)
	c := gaussian(rng, 96)

	// The vector is the centroid: there is nothing to estimate, whatever the query is.
	code := x.rot.newCoder().encode(encode(c), encode(c))
	require.Zero(t, component(code))
	require.Zero(t, codeNorm2(code))
	require.Zero(t, codeCR(code))
	for range 5 {
		require.Zero(t, estimate(x, gaussian(rng, 96), c, code))
	}
}

// A query less the centroid that has the same value in every component of the rotation has
// nothing to round with, and the query that is the centroid is such a one: all zeros.
func TestEstimateOfAQueryAtTheCentroid(t *testing.T) {
	rng := rand.New(rand.NewPCG(13, 14))
	x := quantized(96)
	c := gaussian(rng, 96)
	code := x.rot.newCoder().encode(encode(gaussian(rng, 96)), encode(c))
	require.Zero(t, estimate(x, c, c, code))
	require.Zero(t, estimate(x, make([]float32, 96), make([]float32, 96), code))
}

// A code with every bit set has all the signs positive, so it estimates the sum of the components
// of the rotated query, and one with none set estimates the opposite. That checks the sums of the
// popcounts against the sum of all the components, without the signs of a real residual in the way.
func TestEstimateSignsAddUp(t *testing.T) {
	rng := rand.New(rand.NewPCG(15, 16))
	x := quantized(384)
	q := gaussian(rng, 384)
	qq := x.prepareQuant(q)
	qq.point(make([]float32, x.rot.size))

	// A code whose bits are all set estimates the sum of the rotated components, times the factor.
	code := make([]byte, x.rot.codeLen())
	for i := codeHeader; i < len(code); i++ {
		code[i] = 0xff
	}
	copy(code, []byte{0, 0, 0x80, 0x3f}) // a factor of 1
	rotated := make([]float32, x.rot.size)
	x.rot.apply(rotated, q)
	var sum float64
	for _, c := range rotated {
		sum += float64(c)
	}
	require.InDelta(t, sum, qq.innerProduct(code), 0.05*norm(rotated)+1e-3)

	// And a code of no bits set estimates its opposite.
	clear(code[codeHeader:])
	require.InDelta(t, -sum, qq.innerProduct(code), 0.05*norm(rotated)+1e-3)
}

func TestEstimatorAllocatesNothing(t *testing.T) {
	rng := rand.New(rand.NewPCG(17, 18))
	x := quantized(384)
	code := x.rot.newCoder().encode(encode(gaussian(rng, 384)), encode(gaussian(rng, 384)))
	qq := x.prepareQuant(gaussian(rng, 384))
	rotated := x.appendRotated(nil, encode(gaussian(rng, 384)))

	// Pointing the query at a partition and comparing it with a code make no allocation.
	var sink float32
	require.Zero(t, testing.AllocsPerRun(100, func() {
		qq.point(rotated)
		sink += qq.innerProduct(code)
	}))
	_ = sink
}

func BenchmarkEstimate(b *testing.B) {
	rng := rand.New(rand.NewPCG(19, 20))
	for _, dims := range []int{96, 384, 1536} {
		x := quantized(dims)
		code := x.rot.newCoder().encode(encode(gaussian(rng, dims)), encode(gaussian(rng, dims)))
		qq := x.prepareQuant(gaussian(rng, dims))
		rotated := x.appendRotated(nil, encode(gaussian(rng, dims)))

		b.Run(fmt.Sprint(dims, "/code"), func(b *testing.B) {
			qq.point(rotated)
			var sink float32
			for b.Loop() {
				sink += qq.innerProduct(code)
			}
			_ = sink
		})
		b.Run(fmt.Sprint(dims, "/partition"), func(b *testing.B) {
			for b.Loop() {
				qq.point(rotated)
			}
		})
	}
}
