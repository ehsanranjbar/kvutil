package vector

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// The kernels that the package is built with sum the same as the scalar ones, give or take the
// rounding of a different order of the sums, for every length: the whole vectors of the simd
// package, four at a time and one at a time, and what is left after them.
func TestKernels(t *testing.T) {
	t.Logf("kernels: %s", kernels)

	rng := rand.New(rand.NewPCG(3, 4))
	for dims := 1; dims <= 130; dims++ {
		a := make([]float32, dims)
		b := make([]float32, dims)
		for i := range a {
			a[i] = float32(rng.NormFloat64())
			b[i] = float32(rng.NormFloat64())
		}
		stored := encode(b)

		var dot, sq float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			d := float64(a[i]) - float64(b[i])
			sq += d * d
		}
		tolerance := 1e-5 * math.Max(1, float64(dims))

		name := fmt.Sprint("dims ", dims)
		require.InDelta(t, dot, dotProduct(a, stored), tolerance, name)
		require.InDelta(t, sq, squaredDistance(a, stored), tolerance, name)
		require.InDelta(t, scalarDotProduct(a, stored), dotProduct(a, stored), tolerance, name)
		require.InDelta(t, scalarSquaredDistance(a, stored), squaredDistance(a, stored), tolerance, name)
	}
}

func BenchmarkKernels(b *testing.B) {
	rng := rand.New(rand.NewPCG(5, 6))
	for _, dims := range []int{96, 384, 1536} {
		a := make([]float32, dims)
		v := make([]float32, dims)
		for i := range a {
			a[i] = float32(rng.NormFloat64())
			v[i] = float32(rng.NormFloat64())
		}
		stored := encode(v)

		var sink float32
		for _, k := range []struct {
			name string
			f    func([]float32, []byte) float32
		}{
			{"scalar/dot", scalarDotProduct},
			{kernels + "/dot", dotProduct},
			{"scalar/l2", scalarSquaredDistance},
			{kernels + "/l2", squaredDistance},
		} {
			b.Run(fmt.Sprintf("%d/%s", dims, k.name), func(b *testing.B) {
				for b.Loop() {
					sink += k.f(a, stored)
				}
			})
		}
		_ = sink
	}
}
