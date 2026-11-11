package vector_test

import (
	"math"
	"testing"

	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

func TestMetricDistance(t *testing.T) {
	a := []float32{1, 2, 2}
	b := []float32{2, 0, 1}

	// a.b = 4, |a| = 3, |b| = sqrt(5), |a-b|^2 = 1 + 4 + 1 = 6.
	require.InDelta(t, 1-4/(3*math.Sqrt(5)), vector.Cosine.Distance(a, b), 1e-6)
	require.InDelta(t, math.Sqrt(6), vector.Euclidean.Distance(a, b), 1e-6)
	require.InDelta(t, -4, vector.Dot.Distance(a, b), 1e-6)

	// The cosine: 0 for the same direction whatever the length, 1 for orthogonal, 2 for opposite.
	require.InDelta(t, 0, vector.Cosine.Distance([]float32{1, 1}, []float32{5, 5}), 1e-6)
	require.InDelta(t, 1, vector.Cosine.Distance([]float32{1, 0}, []float32{0, 3}), 1e-6)
	require.InDelta(t, 2, vector.Cosine.Distance([]float32{1, 0}, []float32{-2, 0}), 1e-6)
	require.GreaterOrEqual(t, vector.Cosine.Distance(a, a), float32(0))

	// A zero vector has no direction.
	require.True(t, math.IsNaN(float64(vector.Cosine.Distance([]float32{0, 0}, []float32{1, 0}))))
	require.InDelta(t, 1, vector.Euclidean.Distance([]float32{0, 0}, []float32{1, 0}), 1e-6)
	require.InDelta(t, 0, vector.Dot.Distance([]float32{0, 0}, []float32{1, 0}), 1e-6)

	// The greatest inner product is the nearest.
	require.Less(t, vector.Dot.Distance([]float32{1, 1}, []float32{3, 3}), vector.Dot.Distance([]float32{1, 1}, []float32{1, 1}))

	// The empty vectors are at no distance from each other.
	require.Zero(t, vector.Euclidean.Distance(nil, nil))
}

func TestMetricString(t *testing.T) {
	require.Equal(t, "cosine", vector.Cosine.String())
	require.Equal(t, "euclidean", vector.Euclidean.String())
	require.Equal(t, "dot", vector.Dot.String())
	require.Equal(t, "Metric(0)", vector.Metric(0).String())
	require.Equal(t, "Metric(9)", vector.Metric(9).String())
}

func TestMetricDistancePanics(t *testing.T) {
	for _, m := range []vector.Metric{vector.Cosine, vector.Euclidean, vector.Dot} {
		require.Panics(t, func() { m.Distance([]float32{1, 2}, []float32{1}) }, m.String())
	}
	require.Panics(t, func() { vector.Metric(0).Distance([]float32{1}, []float32{1}) })
	require.Panics(t, func() { vector.Metric(7).Distance([]float32{1}, []float32{1}) })
}
