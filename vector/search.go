package vector

import (
	"bytes"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/internal/decoding"
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
	nrescore  int
	rescoring bool
}

// Probes makes the search read the nearest n partitions, besides the vectors that are in none,
// when the index is partitioned. The more it reads, the nearer it is to finding all the nearest
// records and the longer it takes. Run fails if n is not positive. It makes no difference to a
// search with a source (Among), which is of the records of the source.
//
// A search that is limited, has conditions (Where), has no source and is not Exact, and that
// finds fewer records than its limit, is run again with four times as many partitions, and four
// times as many candidates to re-rank if the index is Quantized, until it finds enough or has
// read every partition (and re-ranked every candidate): conditions that few records satisfy
// would otherwise leave it with fewer results than there are. So n is where the search starts.
func (s Search[K, V]) Probes(n int) Search[K, V] {
	s.nprobes, s.hasProbes = n, true
	return s
}

// Rescore sets how many of the candidates that the codes rank best a search re-ranks by their
// vectors, when the index is Quantized. The more it re-ranks, the nearer it is to finding the
// records that are nearest and the more vectors it reads. It is max(100, 10·limit) by default; a
// search without a limit and without Rescore needs the distance of every candidate, so it reads
// the vectors and no codes. Run fails if n is not positive. It makes no difference to an index
// that is not Quantized, to Exact and to Among.
func (s Search[K, V]) Rescore(n int) Search[K, V] {
	s.nrescore, s.rescoring = n, true
	return s
}

// rescoreCount returns how many candidates are re-ranked, or false if the search has no use for
// the codes because it re-ranks them all.
func (s Search[K, V]) rescoreCount() (n int, ok bool) {
	switch {
	case s.rescoring:
		return s.nrescore, true
	case s.limited:
		return max(100, 10*s.limit), true
	}
	return 0, false
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
//
// The partitions that a search reads are the nearest ones to the query, which need not have
// enough records that satisfy the conditions. So a search that is limited and has no source
// (Among) reads more partitions, and runs again, until it finds as many records as its limit or
// has read them all; see Probes. Scanned and Rescored count all the runs. A search that is
// Exact reads everything the first time.
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
	if s.rescoring && s.nrescore <= 0 {
		return nil, fmt.Errorf("vector: the number of candidates to re-rank must be positive, not %d", s.nrescore)
	}
	q, err := s.ins.x.prepare(s.query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if s.limited && s.limit == 0 {
		return &Results[K, V]{ins: s.ins}, nil
	}

	var (
		hits []hit[V]
		n    counts
	)
	if len(s.conds) == 0 {
		hits, n, err = s.nearest(q, src)
	} else {
		hits, n, err = s.widening(q, src)
	}
	if err != nil {
		return nil, err
	}
	return &Results[K, V]{ins: s.ins, hits: hits, scanned: n.scanned, rescored: n.rescored}, nil
}

// counts is how many entries a search read.
type counts struct {
	// scanned is the number of vectors and codes that the query was compared with.
	scanned int
	// rescored is the number of vectors that were read to re-rank the candidates of the codes.
	rescored int
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
func (s Search[K, V]) nearest(q []float32, src iterutil.Iterator[K, *V]) ([]hit[V], counts, error) {
	if !s.limited {
		var hits []hit[V]
		n, err := s.scan(q, src, func(rawKey []byte, dist float32) {
			hits = append(hits, hit[V]{rawKey: bytes.Clone(rawKey), dist: dist})
		})
		if err != nil {
			return nil, counts{}, err
		}
		slices.SortFunc(hits, compareHits[V])
		return hits, n, nil
	}

	// The furthest of the nearest k is at the root, and is replaced by a candidate that is nearer.
	// The key of a candidate is only valid until the iterator moves, so it is copied only if the
	// candidate gets in.
	top := &hitHeap[V]{hits: make([]hit[V], 0, min(s.limit, 1024)), worst: true}
	n, err := s.scan(q, src, func(rawKey []byte, dist float32) {
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
		return nil, counts{}, err
	}

	slices.SortFunc(top.hits, compareHits[V])
	return top.hits, n, nil
}

// widening is filtered, run again with more partitions while it finds fewer records than the
// limit and there are more to read. It returns the numbers of all the runs.
func (s Search[K, V]) widening(q []float32, src iterutil.Iterator[K, *V]) ([]hit[V], counts, error) {
	var total counts
	for {
		hits, n, err := s.filtered(q, src)
		if err != nil {
			return nil, counts{}, err
		}
		total.scanned += n.scanned
		total.rescored += n.rescored

		next, again, err := s.widened(len(hits), n)
		if err != nil {
			return nil, counts{}, err
		}
		if !again {
			return hits, total, nil
		}
		s = next
	}
}

// widened returns the search to run again after this one found a number of records with the
// counts n, or false if running it again would not find more: it found enough, or it is not one
// that is widened (see Where), or it has read every partition and re-ranked every candidate that
// the codes ranked.
func (s Search[K, V]) widened(found int, n counts) (Search[K, V], bool, error) {
	if !s.limited || found >= s.limit || s.among != nil || s.exact {
		return s, false, nil
	}
	parts, err := s.ins.partitions()
	if err != nil || parts.count() == 0 {
		return s, false, err
	}

	k := parts.count()
	probes := s.probeCount(k)
	rescore, coded := s.rescoreCount()
	coded = coded && s.ins.x.rot != nil
	// With every partition read, the candidates that the codes rank are what is left to widen. There
	// are no more of them if the ones that were re-ranked were fewer than were asked for.
	if probes >= k && (!coded || n.rescored < rescore || rescore > math.MaxInt/4) {
		return s, false, nil
	}

	s.nprobes, s.hasProbes = min(4*probes, k), true
	if coded {
		s.nrescore, s.rescoring = min(4*rescore, math.MaxInt/4), true
	}
	return s, true, nil
}

// filtered finds the nearest candidates whose records satisfy the conditions. Which records do
// is only known by reading them, so the candidates are gone through from the nearest, and the
// records are read until there are enough.
func (s Search[K, V]) filtered(q []float32, src iterutil.Iterator[K, *V]) ([]hit[V], counts, error) {
	var all []hit[V]
	n, err := s.scan(q, src, func(rawKey []byte, dist float32) {
		all = append(all, hit[V]{rawKey: bytes.Clone(rawKey), dist: dist})
	})
	if err != nil {
		return nil, counts{}, err
	}

	candidates := &hitHeap[V]{hits: all}
	heap.Init(candidates)

	// The fields that the conditions read are decoded for each candidate, and the record in full
	// for the ones that satisfy them, if the store and its codec can do so.
	partial := s.partialCheck()

	var hits []hit[V]
	for candidates.Len() > 0 && (!s.limited || len(hits) < s.limit) {
		h := heap.Pop(candidates).(hit[V])

		key, err := s.ins.x.store.KeyCodec().Decode(h.rawKey)
		if err != nil {
			return nil, counts{}, fmt.Errorf("failed to decode a key of the index: %w", err)
		}

		var (
			record *V
			stored extstore.Stored[K, V]
		)
		if partial != nil {
			if stored, err = partial.getter.GetStored(key); err == nil {
				if partial.choice.Full() {
					// Most candidates satisfy the conditions: they are decoded in full at once.
					start := partial.choice.StartWhole()
					record, err = stored.Record()
					partial.choice.EndWhole(start)
				} else {
					start := partial.choice.StartPartial()
					record, err = stored.Partial(partial.decode, partial.scratch)
					if record == partial.scratch {
						partial.choice.EndPartial(start)
					}
				}
			}
		} else {
			record, err = s.ins.records().Get(key)
		}
		if err != nil {
			return nil, counts{}, fmt.Errorf("failed to get the record %v: %w", key, err)
		}

		match := true
		for _, c := range s.conds {
			if match, err = c.Match(record); err != nil {
				return nil, counts{}, fmt.Errorf("failed to check the record %v: %w", key, err)
			}
			if !match {
				break
			}
		}
		if partial != nil {
			partial.choice.Checked(match)
		}
		if !match {
			continue
		}
		if partial != nil && record == partial.scratch {
			// The scratch is reused for the next candidate: the record is decoded in full.
			start := partial.choice.StartWhole()
			record, err = stored.Record()
			partial.choice.EndWhole(start)
			if err != nil {
				return nil, counts{}, fmt.Errorf("failed to get the record %v: %w", key, err)
			}
		}
		h.record = record
		hits = append(hits, h)
	}
	return hits, n, nil
}

// partialReader is how the records of a filtered search are read to check the conditions of the
// search on some of their fields only.
type partialReader[K, V any] struct {
	getter  extstore.StoredGetter[K, V]
	decode  func([]byte, *V) error
	scratch *V
	// choice decides whether the candidates are decoded in full at once instead, which is
	// cheaper when most of them satisfy the conditions.
	choice decoding.Choice
}

// partialCheck prepares the partial reading of the records for the conditions of the search, and
// returns nil if the records of the store cannot be read so: the getter of the store does not give
// records without decoding them, the store does not tell its codec, what the conditions read is not
// known, or the codec cannot decode in part.
func (s Search[K, V]) partialCheck() *partialReader[K, V] {
	if len(s.conds) == 0 {
		return nil
	}
	getter, ok := s.ins.records().(extstore.StoredGetter[K, V])
	if !ok {
		return nil
	}
	coded, ok := s.ins.x.store.(extstore.ValueCoded[V])
	if !ok {
		return nil
	}
	reads, ok := cond.Reads(s.conds...)
	if !ok {
		return nil
	}
	decode, ok := codec.PartialOf(coded.ValueCodec(), reads)
	if !ok {
		return nil
	}
	return &partialReader[K, V]{getter: getter, decode: decode, scratch: new(V)}
}

// scan calls found for every candidate whose distance from the query is not too much, in no
// particular order, and returns the number of vectors and codes that it compared the query with,
// and of vectors that it read to re-rank. The key is only valid until found returns, and the
// distance is the exact one.
//
// The candidates are the records of the source if there is one. Otherwise they are the unplaced
// vectors and those of the nearest partitions, or all the vectors if the search is exact or the
// index has no partitions. If the index is quantized the vectors of the partitions are compared
// by their codes, and the best of them are the candidates.
func (s Search[K, V]) scan(
	q []float32,
	src iterutil.Iterator[K, *V],
	found func(rawKey []byte, dist float32),
) (n counts, err error) {
	x := s.ins.x
	emit := func(rawKey []byte, dist float32) {
		if !s.hasWithin || dist <= s.within {
			found(rawKey, dist)
		}
	}
	consider := func(rawKey, stored []byte) error {
		dist, err := distance(x.metric, q, stored)
		if err != nil {
			return fmt.Errorf("%w: the key %x", err, rawKey)
		}
		n.scanned++
		emit(rawKey, dist)
		return nil
	}

	if src != nil {
		return n, s.scanSource(src, consider)
	}
	parts, err := s.ins.partitions()
	if err != nil {
		return counts{}, err
	}
	if rescore, ok := s.rescoreCount(); ok && x.rot != nil && parts != nil && !s.exact {
		return n, s.scanCodes(q, parts, rescore, &n, consider, emit)
	}

	// One iterator goes over the vectors, and is moved to the start of each partition to read.
	it, err := s.ins.data.Iterator([]byte{vectorsKey}, []byte{vectorsKey + 1})
	if err != nil {
		return counts{}, err
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
		return n, readPartition(nil)
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
			return n, err
		}
	}
	return n, nil
}

// candidate is a vector in a partition that is compared by its code.
type candidate struct {
	rawKey []byte
	pid    uint32
	// dist is the distance that the code estimates, or for the Euclidean distance the squared one.
	dist float32
}

// candidateHeap is a heap of candidates whose root is the furthest of them, by the estimate and
// by key for the same estimate.
type candidateHeap []candidate

func (h candidateHeap) Len() int { return len(h) }

func (h candidateHeap) Less(i, j int) bool {
	return before(h[j].dist, h[j].rawKey, h[i].dist, h[i].rawKey)
}

func (h candidateHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *candidateHeap) Push(x any) { *h = append(*h, x.(candidate)) }

func (h *candidateHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// scanCodes is the scan of an index that is quantized, which finds the candidates in three steps.
// The vectors that are unplaced are compared with the query as they are, as they have no codes.
// The codes of the nearest partitions are compared with it by the distance that they estimate,
// and the best rescore of them are kept. Last, the vectors of those are read, so that the
// distances that are given to found are exact, which is what puts the right ones in order.
//
// The numbers of what was read are added to n as it goes. consider and emit are the scan's: the
// first measures a vector and gives it to the second, which gives it to found if it is within
// the distance that is asked for. The estimates are not checked against that distance, as they
// are not exact.
func (s Search[K, V]) scanCodes(
	q []float32,
	parts *partitions,
	rescore int,
	n *counts,
	consider func(rawKey, stored []byte) error,
	emit func(rawKey []byte, dist float32),
) error {
	x := s.ins.x

	if err := s.scanUnplaced(consider); err != nil {
		return err
	}

	kept, err := s.rankCodes(q, parts, rescore, &n.scanned)
	if err != nil {
		return err
	}

	// The vectors are read in the order of their keys, which is the order they are stored in, so
	// that the reads of a partition follow each other. Each is a point read: an engine of LSM
	// trees, like Badger, skips the tables that do not have a key by their bloom filters, which
	// the seek of an iterator cannot, and a seek in a database of many tables cost 25 µs a vector.
	slices.SortFunc(kept, func(a, b candidate) int {
		return cmp.Or(cmp.Compare(a.pid, b.pid), bytes.Compare(a.rawKey, b.rawKey))
	})
	for _, c := range kept {
		stored, err := s.ins.data.Get(vectorKey(c.pid, c.rawKey))
		// A vector is removed together with its code, in the same transaction.
		if errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("%w: the key %x has a code in the partition %d, which has no vector of it", ErrCorrupt, c.rawKey, c.pid)
		}
		if err != nil {
			return err
		}
		dist, err := distance(x.metric, q, stored)
		if err != nil {
			return fmt.Errorf("%w: the key %x", err, c.rawKey)
		}
		n.rescored++
		emit(c.rawKey, dist)
	}
	return nil
}

// scanUnplaced goes through the vectors that are unplaced, and considers each.
func (s Search[K, V]) scanUnplaced(consider func(rawKey, stored []byte) error) error {
	it, err := s.ins.data.Iterator(vectorKey(unplaced, nil), vectorKey(unplaced+1, nil))
	if err != nil {
		return err
	}
	defer it.Close()

	for ; it.Valid(); it.Next() {
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

// rankCodes compares the query with the codes of the partitions that the search reads, and returns
// the rescore candidates that have the least estimated distances, in no order. It adds the number
// of codes to scanned.
func (s Search[K, V]) rankCodes(q []float32, parts *partitions, rescore int, scanned *int) ([]candidate, error) {
	x := s.ins.x
	qq := x.prepareQuant(q)

	// One iterator goes over the codes, and is moved to the start of each partition to read, as the
	// one that goes over the vectors does.
	it, err := s.ins.data.Iterator([]byte{codesKey}, []byte{codesKey + 1})
	if err != nil {
		return nil, err
	}
	defer it.Close()

	// The furthest of the best is at the root, and is replaced by a candidate that is nearer. The
	// key of a candidate is only valid until the iterator moves, so it is copied only if the
	// candidate gets in.
	best := make(candidateHeap, 0, min(rescore, 1024))
	for _, pid := range s.probes(parts, q) {
		if pid == unplaced {
			continue
		}
		if !it.Valid() {
			break
		}
		j, ok := parts.index(pid)
		if !ok {
			return nil, fmt.Errorf("%w: the partition %d has no centroid", ErrCorrupt, pid)
		}
		term := centroidTerm(x.metric, q, parts.centroid(j, x.dims))
		qq.point(parts.rotatedCentroid(j, x.rot.size))

		start := codeKey(pid, nil)
		for it.Seek(start); it.Valid() && bytes.HasPrefix(it.Key(), start); it.Next() {
			rawKey, _, err := splitVectorKey(it.Key())
			if err != nil {
				return nil, err
			}
			code, err := it.Value()
			if err != nil {
				return nil, err
			}
			if err := x.checkCode(rawKey, code); err != nil {
				return nil, err
			}
			*scanned++

			dist := estimatedDistance(x.metric, term, qq.innerProduct(code), code)
			switch {
			case len(best) < rescore:
				heap.Push(&best, candidate{rawKey: bytes.Clone(rawKey), pid: pid, dist: dist})
			case before(dist, rawKey, best[0].dist, best[0].rawKey):
				root := &best[0]
				root.rawKey = append(root.rawKey[:0], rawKey...)
				root.pid, root.dist = pid, dist
				heap.Fix(&best, 0)
			}
		}
	}
	return best, nil
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
	ins      *Instance[K, V]
	hits     []hit[V]
	scanned  int
	rescored int
	pos      int
	closed   bool
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

// Scanned returns the number of vectors that the search compared the query with, and of codes
// if the index is quantized. It is what the time of a search is spent on, and a search of a
// partitioned index is meant to keep it well below the number of vectors that the index has.
func (r *Results[K, V]) Scanned() int {
	return r.scanned
}

// Rescored returns the number of vectors that the search read to re-rank the candidates that the
// codes of a quantized index chose. They are not in Scanned. It is 0 for a search that did not
// use codes.
func (r *Results[K, V]) Rescored() int {
	return r.rescored
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
