package vector

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
)

// stateLen is the length of the state of an index: a nonce, and the next partition id.
const stateLen = 8 + 4

// partitions are the centroids of the partitions of an index, as a transaction sees them.
type partitions struct {
	// nonce identifies the state that the centroids were read in. It is written, random, in every
	// transaction that changes a centroid, so a set that was read in a transaction that failed
	// afterwards cannot be taken for the one of another.
	nonce [8]byte
	// next is the id that the next partition is given. Every id that was given is less.
	next uint32
	// ids are the ids of the partitions, ascending, and centroids their centroids in the same
	// order, each as a stored vector.
	ids       []uint32
	centroids []byte
	// rotated are the centroids after the rotation of a quantized index, in the same order and
	// each as long as a rotated vector, or nil if the index is not quantized. A search compares the
	// codes of a partition with the query less the centroid, which is the rotated query less these.
	rotated []float32
}

// without returns the partitions as they are with some of them gone, in the state of another
// nonce. The ids stay ascending.
func (p *partitions) without(nonce [8]byte, gone uint32, dims int) *partitions {
	q := &partitions{nonce: nonce, next: p.next}
	size := 0
	rotating := len(p.rotated) > 0
	if rotating {
		size = len(p.rotated) / len(p.ids)
	}
	for j, id := range p.ids {
		if id != gone {
			q.ids = append(q.ids, id)
			q.centroids = append(q.centroids, p.centroid(j, dims)...)
			if rotating {
				q.rotated = append(q.rotated, p.rotatedCentroid(j, size)...)
			}
		}
	}
	return q
}

// rotatedCentroid returns the rotated centroid of the partition that is at an index in ids, where
// size is the length of a rotated vector. The slice must not be modified.
func (p *partitions) rotatedCentroid(j, size int) []float32 {
	return p.rotated[j*size : (j+1)*size]
}

// index returns the position of a partition in ids, or false if there is no such partition.
func (p *partitions) index(pid uint32) (int, bool) {
	if p == nil {
		return 0, false
	}
	return slices.BinarySearch(p.ids, pid)
}

// count returns the number of partitions. There are none in a set that is nil.
func (p *partitions) count() int {
	if p == nil {
		return 0
	}
	return len(p.ids)
}

// centroid returns the centroid of the partition that is at an index in ids, as a stored
// vector. The slice must not be modified.
func (p *partitions) centroid(j, dims int) []byte {
	return p.centroids[j*4*dims : (j+1)*4*dims]
}

// state returns the value of the state key.
func state(nonce [8]byte, next uint32) []byte {
	return binary.BigEndian.AppendUint32(slices.Clone(nonce[:]), next)
}

// loadPartitions returns the partitions that a transaction sees, or nil if there are none.
// They are read from the transaction only if they are not what was read last.
func (x *Index[K, V]) loadPartitions(data *prefix.Tx) (*partitions, error) {
	b, err := data.Get([]byte{stateKey})
	if errors.Is(err, kv.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) != stateLen {
		return nil, fmt.Errorf("%w: the state has %d bytes and %d are needed", ErrCorrupt, len(b), stateLen)
	}

	var nonce [8]byte
	copy(nonce[:], b)
	if cached := x.parts.Load(); cached != nil && cached.nonce == nonce {
		return cached, nil
	}

	it, err := data.Iterator([]byte{centroidsKey}, []byte{centroidsKey + 1})
	if err != nil {
		return nil, err
	}
	defer it.Close()

	p := &partitions{nonce: nonce, next: binary.BigEndian.Uint32(b[8:])}
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) != 5 {
			return nil, fmt.Errorf("%w: the centroid key %x is not of a partition", ErrCorrupt, key)
		}
		value, err := it.Value()
		if err != nil {
			return nil, err
		}
		if len(value) != 4*x.dims {
			return nil, fmt.Errorf("%w: the centroid has %d bytes and %d are needed", ErrCorrupt, len(value), 4*x.dims)
		}
		p.ids = append(p.ids, binary.BigEndian.Uint32(key[1:]))
		p.centroids = append(p.centroids, value...)
		if x.rot != nil {
			p.rotated = x.appendRotated(p.rotated, value)
		}
	}
	x.parts.Store(p)
	return p, nil
}

// assignScore tells how far a stored vector is from a centre when the partition that it belongs
// to is chosen: the less, the nearer. The partition of a vector is the one with the nearest
// centroid, which for the cosine is the one with the greatest inner product (the centroids have
// length 1), and otherwise the one at the least squared distance.
func assignScore(metric Metric, centre []float32, stored []byte) float32 {
	if metric == Cosine {
		return -dotProduct(centre, stored)
	}
	return squaredDistance(centre, stored)
}

// probeScore tells how far a partition is from the query when the partitions to read are
// chosen: the less, the nearer. Unlike the assignment it follows the distance that is searched
// by, so for the inner product the partition whose centroid has the greatest one is read first.
func probeScore(metric Metric, q []float32, centroid []byte) float32 {
	if metric == Euclidean {
		return squaredDistance(q, centroid)
	}
	return -dotProduct(q, centroid)
}

// assignment returns the index in a matrix of centroids of the centroid that a vector is
// assigned to. The first of the centroids that are as near wins, which is the lesser partition.
func assignment(metric Metric, dims int, centroids []byte, v []float32) int {
	best, bestScore := 0, float32(math.Inf(1))
	for j := 0; j*4*dims < len(centroids); j++ {
		if score := assignScore(metric, v, centroids[j*4*dims:(j+1)*4*dims]); score < bestScore {
			best, bestScore = j, score
		}
	}
	return best
}

// probeCount returns how many of k partitions a search reads besides the unplaced area: what the
// search says, or the index, or the square root of k rounded up. It is never more than k.
func (s Search[K, V]) probeCount(k int) int {
	n := s.nprobes
	if !s.hasProbes {
		n = s.ins.x.probes
	}
	if n == 0 {
		n = int(math.Ceil(math.Sqrt(float64(k))))
	}
	return min(n, k)
}

// probes returns the partitions that a search reads, ascending: the unplaced area, and the
// nearest n of the partitions to the query, where n is what the search says, or the index, or
// the square root of their number rounded up. It is never more than the number of partitions.
func (s Search[K, V]) probes(parts *partitions, q []float32) []uint32 {
	x := s.ins.x
	k := len(parts.ids)
	n := s.probeCount(k)

	type scored struct {
		score float32
		id    uint32
	}
	all := make([]scored, k)
	for j, id := range parts.ids {
		all[j] = scored{probeScore(x.metric, q, parts.centroid(j, x.dims)), id}
	}
	if n < k {
		slices.SortFunc(all, func(a, b scored) int {
			return cmp.Or(cmp.Compare(a.score, b.score), cmp.Compare(a.id, b.id))
		})
		all = all[:n]
	}

	ids := make([]uint32, 0, n+1)
	ids = append(ids, unplaced)
	for _, p := range all {
		ids = append(ids, p.id)
	}
	slices.Sort(ids)
	return ids
}
