package vector

import (
	"cmp"
	"math"
	"slices"
)

// The functions here work on stored vectors, which are bytes, so that the clusters of a build
// and the partitions of an index, which are both read from storage, are not decoded.

// centroidOf returns the centroid of vectors: their mean (summed in float64), and for Cosine the
// mean normalized to length 1 (if the mean is the zero vector, the first vector).
func centroidOf(metric Metric, dims int, vectors [][]byte) []float32 {
	sum := make([]float64, dims)
	for _, v := range vectors {
		for i := range sum {
			sum[i] += float64(component(v[4*i:]))
		}
	}

	mean := make([]float32, dims)
	var squares float64
	for i, s := range sum {
		m := s / float64(max(len(vectors), 1))
		squares += m * m
		mean[i] = float32(m)
	}
	if metric != Cosine {
		return mean
	}

	norm := math.Sqrt(squares)
	if norm == 0 || math.IsNaN(norm) || math.IsInf(norm, 0) {
		if len(vectors) == 0 {
			return mean
		}
		// The first vector is what is left of a direction, and it is a unit vector already.
		first := make([]float32, dims)
		_ = decode(first, vectors[0])
		return first
	}
	for i := range mean {
		mean[i] = float32(float64(mean[i]) / norm)
	}
	return mean
}

// bisect splits vectors into two clusters with 2-means and returns which cluster (0 or 1) each
// vector is in and the two centroids. Deterministic: the first centre is the vector furthest (in
// assignment order) from the centroid of all, the second the vector furthest from the first; then
// at most 10 rounds of assigning every vector to its nearer centre and moving the centres to
// centroidOf their vectors, stopping when no vector changes sides. If a side ends up empty (for
// example all vectors are equal), the first half of the vectors is side 0 and the rest side 1.
func bisect(metric Metric, dims int, vectors [][]byte) (side []uint8, c0, c1 []float32) {
	const rounds = 10

	side = make([]uint8, len(vectors))
	if len(vectors) < 2 {
		c0 = centroidOf(metric, dims, vectors)
		return side, c0, c0
	}

	// furthest returns the vector that is furthest from a centre. The first one wins a tie.
	furthest := func(centre []float32) []float32 {
		at, worst := 0, float32(math.Inf(-1))
		for i, v := range vectors {
			if score := assignScore(metric, centre, v); score > worst {
				at, worst = i, score
			}
		}
		decoded := make([]float32, dims)
		_ = decode(decoded, vectors[at])
		return decoded
	}
	c0 = furthest(centroidOf(metric, dims, vectors))
	c1 = furthest(c0)

	// Which side a vector is on is not known yet, so that the first round is a change.
	for i := range side {
		side[i] = 2
	}
	for range rounds {
		changed := false
		var sides [2][][]byte
		for i, v := range vectors {
			s := uint8(0)
			if assignScore(metric, c1, v) < assignScore(metric, c0, v) {
				s = 1
			}
			if side[i] != s {
				changed = true
				side[i] = s
			}
			sides[s] = append(sides[s], v)
		}

		if len(sides[0]) == 0 || len(sides[1]) == 0 {
			return halves(metric, dims, vectors)
		}
		if !changed {
			break
		}
		c0 = centroidOf(metric, dims, sides[0])
		c1 = centroidOf(metric, dims, sides[1])
	}
	return side, c0, c1
}

// bisectBalanced is bisect, except that it splits at the median if the smaller of the two sides
// has fewer than an eighth of the vectors. Otherwise an outlier would be split off from its
// partition into a partition of its own, which is dissolved for having too few vectors, placed
// back and split off again, for as long as the maintainer runs. The vectors are put in order by
// how much nearer they are to the first centre than to the second, and the first half is the
// side 0.
func bisectBalanced(metric Metric, dims int, vectors [][]byte) (side []uint8, c0, c1 []float32) {
	side, c0, c1 = bisect(metric, dims, vectors)

	var sides [2]int
	for _, s := range side {
		sides[s]++
	}
	if min(sides[0], sides[1]) >= len(vectors)/8 {
		return side, c0, c1
	}

	margin := make([]float32, len(vectors))
	order := make([]int, len(vectors))
	for i, v := range vectors {
		margin[i] = assignScore(metric, c0, v) - assignScore(metric, c1, v)
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int {
		return cmp.Or(cmp.Compare(margin[a], margin[b]), cmp.Compare(a, b))
	})

	half := max(1, len(vectors)/2)
	var firsts, rests [][]byte
	for rank, i := range order {
		if rank < half {
			side[i] = 0
			firsts = append(firsts, vectors[i])
		} else {
			side[i] = 1
			rests = append(rests, vectors[i])
		}
	}
	return side, centroidOf(metric, dims, firsts), centroidOf(metric, dims, rests)
}

// halves splits vectors into the first half and the rest.
func halves(metric Metric, dims int, vectors [][]byte) (side []uint8, c0, c1 []float32) {
	half := max(1, len(vectors)/2)
	side = make([]uint8, len(vectors))
	for i := half; i < len(vectors); i++ {
		side[i] = 1
	}
	return side, centroidOf(metric, dims, vectors[:half]), centroidOf(metric, dims, vectors[half:])
}
