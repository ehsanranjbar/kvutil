package vector

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"slices"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Instance is an Index in a transaction, which records are searched in.
type Instance[K, V any] struct {
	x      *Index[K, V]
	tx     kv.Tx
	data   *prefix.Tx
	getter extstore.Getter[K, V]
}

// partitions returns the partitions that the transaction sees, or nil if there are none.
func (i *Instance[K, V]) partitions() (*partitions, error) {
	if i.x.size == 0 {
		return nil, nil
	}
	return i.x.loadPartitions(i.data)
}

// find returns the stored vector of a record: the one in the partition that the record is in
// if the index has partitions, otherwise the one in the unplaced area. It returns
// kv.ErrNotFound if the record is in neither.
func (i *Instance[K, V]) find(rawKey []byte) ([]byte, error) {
	if i.x.size > 0 {
		pid, placed, err := readPlace(i.data, rawKey)
		if err != nil {
			return nil, err
		}
		if placed {
			b, err := i.data.Get(vectorKey(pid, rawKey))
			if errors.Is(err, kv.ErrNotFound) {
				return nil, fmt.Errorf("%w: the key %x is in the partition %d, which has no vector of it", ErrCorrupt, rawKey, pid)
			}
			return b, err
		}
	}
	return i.data.Get(vectorKey(unplaced, rawKey))
}

// Stats is what an index has in a transaction.
type Stats struct {
	// Partitions is the number of partitions. It is 0 if the vectors are in none.
	Partitions int
	// Unplaced is the number of vectors that are in no partition, which every search compares
	// the query with.
	Unplaced int
}

// Stats returns what the index has in the transaction. It counts the keys of the unplaced
// area, without reading the vectors.
func (i *Instance[K, V]) Stats() (Stats, error) {
	parts, err := i.partitions()
	if err != nil {
		return Stats{}, err
	}
	stats := Stats{Partitions: parts.count()}

	it, err := i.data.Iterator(vectorKey(unplaced, nil), vectorKey(unplaced+1, nil))
	if err != nil {
		return Stats{}, err
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		stats.Unplaced++
	}
	return stats, nil
}

// records returns what gets the records of the store, which is made when it is first needed.
func (i *Instance[K, V]) records() extstore.Getter[K, V] {
	if i.getter == nil {
		i.getter = i.x.store.Getter(i.tx)
	}
	return i.getter
}

// Nearest returns the k records whose vectors are nearest to the query, the nearest first.
// It is Search(q).Limit(k).Run().
func (i *Instance[K, V]) Nearest(q []float32, k int) (*Results[K, V], error) {
	return i.Search(q).Limit(k).Run()
}

// Search starts describing a search for the records whose vectors are nearest to the query.
func (i *Instance[K, V]) Search(q []float32) Search[K, V] {
	return Search[K, V]{ins: i, query: q}
}

// Vector returns the vector that the index has for a record (normalized, for Cosine), or
// kv.ErrNotFound if the record is not in the index.
func (i *Instance[K, V]) Vector(key K) ([]float32, error) {
	rawKey, err := i.x.store.KeyCodec().Encode(key)
	if err != nil {
		return nil, err
	}
	b, err := i.find(rawKey)
	if err != nil {
		return nil, err
	}

	v := make([]float32, i.x.dims)
	if err := decode(v, b); err != nil {
		return nil, fmt.Errorf("%w: the record %v", err, key)
	}
	return v, nil
}

// Search is a search that is being described. It is a value: every method returns another one
// and leaves its receiver as it was.
//
// A search with a source (Among) can only be run once, as running it consumes the source.
type Search[K, V any] struct {
	ins       *Instance[K, V]
	query     []float32
	limit     int
	limited   bool
	within    float32
	hasWithin bool
	conds     []cond.Cond[V]
	among     iterutil.Iterator[K, *V]
	nprobes   int
	hasProbes bool
	exact     bool
}

// Probes makes the search read the nearest n partitions, besides the vectors that are in none,
// when the index is partitioned. The more it reads, the nearer it is to finding all the nearest
// records and the longer it takes. Run fails if n is not positive. It makes no difference to a
// search with a source (Among), which is of the records of the source.
func (s Search[K, V]) Probes(n int) Search[K, V] {
	s.nprobes, s.hasProbes = n, true
	return s
}

// Exact makes the search compare the query with every vector, whatever the partitions are, so
// that it finds the nearest records and not the ones that are nearly the nearest.
func (s Search[K, V]) Exact() Search[K, V] {
	s.exact = true
	return s
}

// Limit makes the search return at most k records. Without it all the records that are
// candidates are returned. A negative k makes Run fail.
func (s Search[K, V]) Limit(k int) Search[K, V] {
	s.limit, s.limited = k, true
	return s
}

// Within makes the search return only the records whose distance is at most the given one.
func (s Search[K, V]) Within(distance float32) Search[K, V] {
	s.within, s.hasWithin = distance, true
	return s
}

// Where makes the search return only the records that satisfy the conditions, as well as the
// ones that were given before. The conditions are checked on the records, which are only read
// as far as the nearest ones that the search needs.
func (s Search[K, V]) Where(conds ...cond.Cond[V]) Search[K, V] {
	// A copy, so that two searches derived from one do not write to the same array.
	s.conds = slices.Concat(s.conds, conds)
	return s
}

// Among limits the candidates to the records of a source, like the iterator of a lookup in an
// index, which leaves the index to find the records and the vectors to order them. The source
// is closed by Run, whatever it returns. Another call closes the source that was given before
// and replaces it.
func (s Search[K, V]) Among(src iterutil.Iterator[K, *V]) Search[K, V] {
	if s.among != nil {
		s.among.Close()
	}
	s.among = src
	return s
}

// Run runs the search. The results must be closed.
func (s Search[K, V]) Run() (res *Results[K, V], err error) {
	src := s.among

	// The source is closed on every path, and the error of closing it is only returned if there
	// is no other.
	if src != nil {
		defer func() {
			if cerr := src.Close(); err == nil && cerr != nil {
				res, err = nil, cerr
			}
		}()
	}

	if s.limited && s.limit < 0 {
		return nil, fmt.Errorf("vector: the limit of a search must not be negative, not %d", s.limit)
	}
	if s.hasProbes && s.nprobes <= 0 {
		return nil, fmt.Errorf("vector: the number of partitions to read must be positive, not %d", s.nprobes)
	}
	q, err := s.ins.x.prepare(s.query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if s.limited && s.limit == 0 {
		return &Results[K, V]{ins: s.ins}, nil
	}

	var (
		hits    []hit[V]
		scanned int
	)
	if len(s.conds) == 0 {
		hits, scanned, err = s.nearest(q, src)
	} else {
		hits, scanned, err = s.filtered(q, src)
	}
	if err != nil {
		return nil, err
	}
	return &Results[K, V]{ins: s.ins, hits: hits, scanned: scanned}, nil
}

// hit is a record that a search found.
type hit[V any] struct {
	rawKey []byte
	dist   float32
	record *V
}

// before reports whether a candidate comes before another in the order of the results: by
// distance, and by key for the same distance, so that the results are the same every time.
func before(d1 float32, key1 []byte, d2 float32, key2 []byte) bool {
	if d1 != d2 {
		return d1 < d2
	}
	return bytes.Compare(key1, key2) < 0
}

// compareHits orders hits as before does.
func compareHits[V any](a, b hit[V]) int {
	switch {
	case before(a.dist, a.rawKey, b.dist, b.rawKey):
		return -1
	case before(b.dist, b.rawKey, a.dist, a.rawKey):
		return 1
	}
	return 0
}

// hitHeap is a heap of hits, whose root is the nearest of them or, if worst is set, the
// furthest.
type hitHeap[V any] struct {
	hits  []hit[V]
	worst bool
}

func (h *hitHeap[V]) Len() int { return len(h.hits) }

func (h *hitHeap[V]) Less(i, j int) bool {
	a, b := h.hits[i], h.hits[j]
	if h.worst {
		a, b = b, a
	}
	return before(a.dist, a.rawKey, b.dist, b.rawKey)
}

func (h *hitHeap[V]) Swap(i, j int) { h.hits[i], h.hits[j] = h.hits[j], h.hits[i] }

func (h *hitHeap[V]) Push(x any) { h.hits = append(h.hits, x.(hit[V])) }

func (h *hitHeap[V]) Pop() any {
	last := h.hits[len(h.hits)-1]
	h.hits = h.hits[:len(h.hits)-1]
	return last
}

// nearest finds the nearest candidates when there are no conditions to check on the records:
// the first k of them, or all if there is no limit.
func (s Search[K, V]) nearest(q []float32, src iterutil.Iterator[K, *V]) ([]hit[V], int, error) {
	if !s.limited {
		var hits []hit[V]
		scanned, err := s.scan(q, src, func(rawKey []byte, dist float32) {
			hits = append(hits, hit[V]{rawKey: bytes.Clone(rawKey), dist: dist})
		})
		if err != nil {
			return nil, 0, err
		}
		slices.SortFunc(hits, compareHits[V])
		return hits, scanned, nil
	}

	// The furthest of the nearest k is at the root, and is replaced by a candidate that is nearer.
	// The key of a candidate is only valid until the iterator moves, so it is copied only if the
	// candidate gets in.
	top := &hitHeap[V]{hits: make([]hit[V], 0, min(s.limit, 1024)), worst: true}
	scanned, err := s.scan(q, src, func(rawKey []byte, dist float32) {
		switch {
		case len(top.hits) < s.limit:
			heap.Push(top, hit[V]{rawKey: bytes.Clone(rawKey), dist: dist})
		case before(dist, rawKey, top.hits[0].dist, top.hits[0].rawKey):
			root := &top.hits[0]
			root.rawKey = append(root.rawKey[:0], rawKey...)
			root.dist = dist
			heap.Fix(top, 0)
		}
	})
	if err != nil {
		return nil, 0, err
	}

	slices.SortFunc(top.hits, compareHits[V])
	return top.hits, scanned, nil
}

// filtered finds the nearest candidates whose records satisfy the conditions. Which records do
// is only known by reading them, so the candidates are gone through from the nearest, and the
// records are read until there are enough.
func (s Search[K, V]) filtered(q []float32, src iterutil.Iterator[K, *V]) ([]hit[V], int, error) {
	var all []hit[V]
	scanned, err := s.scan(q, src, func(rawKey []byte, dist float32) {
		all = append(all, hit[V]{rawKey: bytes.Clone(rawKey), dist: dist})
	})
	if err != nil {
		return nil, 0, err
	}

	candidates := &hitHeap[V]{hits: all}
	heap.Init(candidates)

	var hits []hit[V]
	for candidates.Len() > 0 && (!s.limited || len(hits) < s.limit) {
		h := heap.Pop(candidates).(hit[V])

		key, err := s.ins.x.store.KeyCodec().Decode(h.rawKey)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode a key of the index: %w", err)
		}
		record, err := s.ins.records().Get(key)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to get the record %v: %w", key, err)
		}

		match := true
		for _, c := range s.conds {
			if match, err = c.Match(record); err != nil {
				return nil, 0, fmt.Errorf("failed to check the record %v: %w", key, err)
			}
			if !match {
				break
			}
		}
		if match {
			h.record = record
			hits = append(hits, h)
		}
	}
	return hits, scanned, nil
}

// scan calls found for every candidate whose distance from the query is not too much, in no
// particular order, and returns the number of vectors that it measured the distance to. The key
// is only valid until found returns.
//
// The candidates are the records of the source if there is one. Otherwise they are the unplaced
// vectors and those of the nearest partitions, or all the vectors if the search is exact or the
// index has no partitions.
func (s Search[K, V]) scan(
	q []float32,
	src iterutil.Iterator[K, *V],
	found func(rawKey []byte, dist float32),
) (scanned int, err error) {
	x := s.ins.x
	consider := func(rawKey, stored []byte) error {
		dist, err := distance(x.metric, q, stored)
		if err != nil {
			return fmt.Errorf("%w: the key %x", err, rawKey)
		}
		scanned++
		if !s.hasWithin || dist <= s.within {
			found(rawKey, dist)
		}
		return nil
	}

	if src != nil {
		return scanned, s.scanSource(src, consider)
	}
	parts, err := s.ins.partitions()
	if err != nil {
		return 0, err
	}

	// One iterator goes over the vectors, and is moved to the start of each partition to read.
	it, err := s.ins.data.Iterator([]byte{vectorsKey}, []byte{vectorsKey + 1})
	if err != nil {
		return 0, err
	}
	defer it.Close()

	readPartition := func(start []byte) error {
		for ; it.Valid() && (start == nil || bytes.HasPrefix(it.Key(), start)); it.Next() {
			rawKey, _, err := splitVectorKey(it.Key())
			if err != nil {
				return err
			}
			stored, err := it.Value()
			if err != nil {
				return err
			}
			if err := consider(rawKey, stored); err != nil {
				return err
			}
		}
		return nil
	}

	if parts == nil || s.exact {
		return scanned, readPartition(nil)
	}
	for _, pid := range s.probes(parts, q) {
		// What is left is past the partitions that were read, so there is nothing to seek to
		// once the iterator is done.
		if !it.Valid() {
			break
		}
		start := vectorKey(pid, nil)
		it.Seek(start)
		if err := readPartition(start); err != nil {
			return scanned, err
		}
	}
	return scanned, nil
}

// scanSource goes through the records of a source, and considers the ones that have a vector.
// The vector of a record is found where the record is, so which partitions are read is of no
// matter.
func (s Search[K, V]) scanSource(src iterutil.Iterator[K, *V], consider func(rawKey, stored []byte) error) error {
	type rawKeyer interface {
		RawKey() ([]byte, error)
	}
	rk, hasRawKey := src.(rawKeyer)

	for ; src.Valid(); src.Next() {
		var (
			rawKey []byte
			err    error
		)
		if hasRawKey {
			rawKey, err = rk.RawKey()
		} else {
			var key K
			if key, err = src.Key(); err == nil {
				rawKey, err = s.ins.x.store.KeyCodec().Encode(key)
			}
		}
		if err != nil {
			return err
		}

		stored, err := s.ins.find(rawKey)
		if errors.Is(err, kv.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := consider(rawKey, stored); err != nil {
			return err
		}
	}

	if e, ok := src.(interface{ Err() error }); ok {
		return e.Err()
	}
	return nil
}

// Results are the records that a search found, the nearest first. They are iterated over like
// any other records, and they also tell the distance of each.
type Results[K, V any] struct {
	ins     *Instance[K, V]
	hits    []hit[V]
	scanned int
	pos     int
	closed  bool
	// set is the keys of the hits, which is made when it is first needed.
	set map[string]struct{}
}

// Valid implements the iterutil.Iterator interface.
func (r *Results[K, V]) Valid() bool {
	return !r.closed && r.pos < len(r.hits)
}

// Next implements the iterutil.Iterator interface.
func (r *Results[K, V]) Next() {
	if r.Valid() {
		r.pos++
	}
}

// current returns the hit that the results are at.
func (r *Results[K, V]) current() (*hit[V], error) {
	if !r.Valid() {
		return nil, errors.New("vector: the results are not at a record")
	}
	return &r.hits[r.pos], nil
}

// Key implements the iterutil.Iterator interface.
func (r *Results[K, V]) Key() (key K, err error) {
	h, err := r.current()
	if err != nil {
		return key, err
	}

	key, err = r.ins.x.store.KeyCodec().Decode(h.rawKey)
	if err != nil {
		return key, fmt.Errorf("failed to decode primary key: %w", err)
	}
	return key, nil
}

// Value returns the current record. It is read from the store when it is first asked for, and
// is the same one after that.
func (r *Results[K, V]) Value() (*V, error) {
	h, err := r.current()
	if err != nil {
		return nil, err
	}
	if h.record != nil {
		return h.record, nil
	}

	key, err := r.Key()
	if err != nil {
		return nil, err
	}
	h.record, err = r.ins.records().Get(key)
	return h.record, err
}

// Distance returns the distance of the current record from the query, as the metric of the
// index measures it. It is 0 when the results are not at a record.
func (r *Results[K, V]) Distance() float32 {
	if !r.Valid() {
		return 0
	}
	return r.hits[r.pos].dist
}

// RawKey returns the primary key of the current record as the store encodes it.
// The slice must not be modified.
func (r *Results[K, V]) RawKey() ([]byte, error) {
	h, err := r.current()
	if err != nil {
		return nil, err
	}
	return h.rawKey, nil
}

// Contains reports whether a record is one of the results, wherever the iterator is and also
// after it is closed. The record itself is not looked at.
func (r *Results[K, V]) Contains(rawKey []byte, _ *V) (bool, error) {
	if r.set == nil {
		r.set = make(map[string]struct{}, len(r.hits))
		for _, h := range r.hits {
			r.set[string(h.rawKey)] = struct{}{}
		}
	}
	_, ok := r.set[string(rawKey)]
	return ok, nil
}

// Scanned returns the number of vectors that the search compared the query with. It is what
// the time of a search is spent on, and a search of a partitioned index is meant to keep it
// well below the number of vectors that the index has.
func (r *Results[K, V]) Scanned() int {
	return r.scanned
}

// Len returns the number of results in all, wherever the iterator is.
func (r *Results[K, V]) Len() int {
	return len(r.hits)
}

// Close implements the iterutil.Iterator interface. The results hold no resource, so it only
// stops the iteration; Contains and Len still work.
func (r *Results[K, V]) Close() error {
	r.closed = true
	return nil
}
