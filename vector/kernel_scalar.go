//go:build !(goexperiment.simd && (amd64 || arm64))

package vector

// kernels names the kernels that the distances are summed with.
const kernels = "scalar"

// dotProduct returns the inner product of a vector and a stored one of the same dimensions.
func dotProduct(a []float32, stored []byte) float32 {
	return scalarDotProduct(a, stored)
}

// squaredDistance returns the sum of the squares of the differences of a vector and a stored one
// of the same dimensions.
func squaredDistance(a []float32, stored []byte) float32 {
	return scalarSquaredDistance(a, stored)
}
