package vector

// CodeOf returns the code that a quantized index makes of a vector in the partition of a
// centroid, both as the index stores them (normalized, for Cosine). It is deterministic, so the
// tests compare the codes that are stored with it byte by byte. It panics if the index is not
// quantized or the vectors are not of its dimensions.
func CodeOf[K, V any](x *Index[K, V], vector, centroid []float32) []byte {
	if len(vector) != x.dims || len(centroid) != x.dims {
		panic("vector: the vectors do not have the dimensions of the index")
	}
	return x.rot.newCoder().encode(encode(vector), encode(centroid))
}
