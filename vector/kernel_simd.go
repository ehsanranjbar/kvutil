//go:build goexperiment.simd && (amd64 || arm64)

package vector

import "simd"

// The kernels of the simd package, which a program gets when it is built with GOEXPERIMENT=simd.
// They are three times as fast as the scalar ones on a processor with 128-bit vectors (NEON).
//
// A stored vector is read as it is, its bytes reinterpreted as floats, which is right because it
// is little-endian, as these processors are. Where the simd package has to emulate the vectors in
// Go, which is slower than the scalar kernels, the scalar kernels are used.

// kernels names the kernels that the distances are summed with.
const kernels = "simd"

// emulated is set if the vectors of the simd package are emulated on this processor. It is set
// in init, as the simd package cannot be called from the initializer of a variable.
var emulated bool

func init() {
	emulated = simd.Emulated()
}

// storedFloats reads the floats of a stored vector that are in b, which is as long as they are.
func storedFloats(b []byte) simd.Float32s {
	return simd.LoadUint8s(b).ReshapeToUint32s().BitsToFloat32()
}

// sumLanes returns the sum of the lanes of a vector.
func sumLanes(x simd.Float32s) float32 {
	var buf [16]float32
	lanes := buf[:]
	if x.Len() > len(buf) {
		lanes = make([]float32, x.Len())
	}
	lanes = lanes[:x.Len()]
	x.Store(lanes)

	var s float32
	for _, l := range lanes {
		s += l
	}
	return s
}

// dotProduct returns the inner product of a vector and a stored one of the same dimensions. It
// sums in four vectors of accumulators, which the processor can work on at once.
func dotProduct(a []float32, stored []byte) float32 {
	if emulated {
		return scalarDotProduct(a, stored)
	}

	var s0, s1, s2, s3 simd.Float32s
	n := s0.Len()
	stored = stored[:4*len(a)]
	i := 0
	for ; i+4*n <= len(a); i += 4 * n {
		x, y := a[i:i+4*n], stored[4*i:4*i+16*n]
		s0 = simd.LoadFloat32s(x[:n]).MulAdd(storedFloats(y[:4*n]), s0)
		s1 = simd.LoadFloat32s(x[n:2*n]).MulAdd(storedFloats(y[4*n:8*n]), s1)
		s2 = simd.LoadFloat32s(x[2*n:3*n]).MulAdd(storedFloats(y[8*n:12*n]), s2)
		s3 = simd.LoadFloat32s(x[3*n:]).MulAdd(storedFloats(y[12*n:]), s3)
	}
	for ; i+n <= len(a); i += n {
		s0 = simd.LoadFloat32s(a[i:i+n]).MulAdd(storedFloats(stored[4*i:4*i+4*n]), s0)
	}

	s := sumLanes(s0.Add(s1).Add(s2.Add(s3)))
	for ; i < len(a); i++ {
		s += a[i] * component(stored[4*i:4*i+4])
	}
	return s
}

// squaredDistance returns the sum of the squares of the differences of a vector and a stored one
// of the same dimensions, summed like dotProduct does.
func squaredDistance(a []float32, stored []byte) float32 {
	if emulated {
		return scalarSquaredDistance(a, stored)
	}

	var s0, s1, s2, s3 simd.Float32s
	n := s0.Len()
	stored = stored[:4*len(a)]
	i := 0
	for ; i+4*n <= len(a); i += 4 * n {
		x, y := a[i:i+4*n], stored[4*i:4*i+16*n]
		d0 := simd.LoadFloat32s(x[:n]).Sub(storedFloats(y[:4*n]))
		d1 := simd.LoadFloat32s(x[n : 2*n]).Sub(storedFloats(y[4*n : 8*n]))
		d2 := simd.LoadFloat32s(x[2*n : 3*n]).Sub(storedFloats(y[8*n : 12*n]))
		d3 := simd.LoadFloat32s(x[3*n:]).Sub(storedFloats(y[12*n:]))
		s0 = d0.MulAdd(d0, s0)
		s1 = d1.MulAdd(d1, s1)
		s2 = d2.MulAdd(d2, s2)
		s3 = d3.MulAdd(d3, s3)
	}
	for ; i+n <= len(a); i += n {
		d := simd.LoadFloat32s(a[i : i+n]).Sub(storedFloats(stored[4*i : 4*i+4*n]))
		s0 = d.MulAdd(d, s0)
	}

	s := sumLanes(s0.Add(s1).Add(s2.Add(s3)))
	for ; i < len(a); i++ {
		d := a[i] - component(stored[4*i:4*i+4])
		s += d * d
	}
	return s
}
