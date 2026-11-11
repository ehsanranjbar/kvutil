package vector

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// stored returns vectors in the form that they are stored in.
func stored(vectors ...[]float32) [][]byte {
	out := make([][]byte, len(vectors))
	for i, v := range vectors {
		out[i] = encode(v)
	}
	return out
}

func TestCentroidOf(t *testing.T) {
	vectors := stored([]float32{1, 0}, []float32{3, 4}, []float32{2, 2})

	require.InDeltaSlice(t, []float32{2, 2}, centroidOf(Euclidean, 2, vectors), 1e-6)
	require.InDeltaSlice(t, []float32{2, 2}, centroidOf(Dot, 2, vectors), 1e-6)

	// For the cosine it is a direction.
	c := centroidOf(Cosine, 2, vectors)
	require.InDelta(t, 1, math.Hypot(float64(c[0]), float64(c[1])), 1e-6)
	require.InDeltaSlice(t, []float32{math.Sqrt2 / 2, math.Sqrt2 / 2}, c, 1e-6)

	// Vectors that cancel out have no mean direction, and the first one is the centroid.
	opposite := stored([]float32{1, 0}, []float32{-1, 0})
	require.Equal(t, []float32{1, 0}, centroidOf(Cosine, 2, opposite))
	require.Equal(t, []float32{0, 0}, centroidOf(Euclidean, 2, opposite))
}

func TestCosineCentroidsHaveLengthOne(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 20 {
		var vectors [][]byte
		for range 1 + rng.IntN(30) {
			v := make([]float32, 6)
			var sum float64
			for i := range v {
				v[i] = float32(rng.NormFloat64())
				sum += float64(v[i]) * float64(v[i])
			}
			for i := range v {
				v[i] = float32(float64(v[i]) / math.Sqrt(sum))
			}
			vectors = append(vectors, encode(v))
		}

		c := centroidOf(Cosine, 6, vectors)
		var sum float64
		for _, x := range c {
			sum += float64(x) * float64(x)
		}
		require.InDelta(t, 1, sum, 1e-5)

		_, c0, c1 := bisect(Cosine, 6, vectors)
		for _, c := range [][]float32{c0, c1} {
			var sum float64
			for _, x := range c {
				sum += float64(x) * float64(x)
			}
			require.InDelta(t, 1, sum, 1e-5)
		}
	}
}

// blobs returns vectors around two centres that are far apart, the ones around the first first.
func blobs(n int) (vectors [][]byte, first int) {
	rng := rand.New(rand.NewPCG(3, 4))
	centres := [][]float32{{10, 0, 0}, {0, 10, 0}}
	for c, centre := range centres {
		for range n {
			v := make([]float32, 3)
			for i := range v {
				v[i] = centre[i] + float32(rng.NormFloat64())*0.1
			}
			vectors = append(vectors, encode(v))
		}
		if c == 0 {
			first = len(vectors)
		}
	}
	return vectors, first
}

func TestBisectSeparatesBlobs(t *testing.T) {
	for _, metric := range []Metric{Cosine, Euclidean, Dot} {
		vectors, first := blobs(50)
		if metric == Cosine {
			for i, v := range vectors {
				decoded := make([]float32, 3)
				require.NoError(t, decode(decoded, v))
				var sum float64
				for _, c := range decoded {
					sum += float64(c) * float64(c)
				}
				for j := range decoded {
					decoded[j] = float32(float64(decoded[j]) / math.Sqrt(sum))
				}
				vectors[i] = encode(decoded)
			}
		}

		side, c0, c1 := bisect(metric, 3, vectors)
		require.Len(t, side, len(vectors))
		for i := range side {
			if i < first {
				require.Equal(t, side[0], side[i], "%v: the first blob is on one side", metric)
			} else {
				require.NotEqual(t, side[0], side[i], "%v: the second blob is on the other", metric)
			}
		}

		// The centres are in the blobs they stand for: they point along the axis of the blob.
		near := func(c []float32, x, y float64) {
			length := math.Sqrt(float64(c[0]*c[0] + c[1]*c[1] + c[2]*c[2]))
			require.Greater(t, float64(c[0])*x+float64(c[1])*y, 0.95*length, "%v: %v", metric, c)
		}
		if side[0] == 0 {
			near(c0, 1, 0)
			near(c1, 0, 1)
		} else {
			near(c1, 1, 0)
			near(c0, 0, 1)
		}
	}
}

func TestBisectOfEqualVectorsIsHalves(t *testing.T) {
	for _, metric := range []Metric{Cosine, Euclidean, Dot} {
		for _, n := range []int{2, 7, 10} {
			v := encode([]float32{0.6, 0.8})
			vectors := make([][]byte, n)
			for i := range vectors {
				vectors[i] = v
			}

			side, c0, c1 := bisect(metric, 2, vectors)
			for i, s := range side {
				if i < n/2 {
					require.Zero(t, s, "%v %d: the first half", metric, n)
				} else {
					require.Equal(t, uint8(1), s, "%v %d: the rest", metric, n)
				}
			}
			require.InDeltaSlice(t, []float32{0.6, 0.8}, c0, 1e-6)
			require.InDeltaSlice(t, []float32{0.6, 0.8}, c1, 1e-6)
		}
	}
}

func TestBisectIsDeterministic(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	vectors := make([][]byte, 200)
	for i := range vectors {
		v := make([]float32, 8)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vectors[i] = encode(v)
	}

	for _, metric := range []Metric{Euclidean, Dot} {
		side, c0, c1 := bisect(metric, 8, vectors)
		for range 3 {
			side2, d0, d1 := bisect(metric, 8, vectors)
			require.Equal(t, side, side2)
			require.Equal(t, c0, d0)
			require.Equal(t, c1, d1)
		}

		// Both sides have vectors, and the centres are the centroids of their sides.
		var sides [2][][]byte
		for i, s := range side {
			sides[s] = append(sides[s], vectors[i])
		}
		require.NotEmpty(t, sides[0])
		require.NotEmpty(t, sides[1])
		require.InDeltaSlice(t, centroidOf(metric, 8, sides[0]), c0, 1e-5)
		require.InDeltaSlice(t, centroidOf(metric, 8, sides[1]), c1, 1e-5)
	}
}

func TestBisectOfFewVectors(t *testing.T) {
	side, c0, c1 := bisect(Euclidean, 2, stored([]float32{1, 2}))
	require.Equal(t, []uint8{0}, side)
	require.Equal(t, []float32{1, 2}, c0)
	require.Equal(t, []float32{1, 2}, c1)

	side, _, _ = bisect(Euclidean, 2, stored([]float32{0, 0}, []float32{9, 9}))
	require.Equal(t, []uint8{0, 1}, side)
}
