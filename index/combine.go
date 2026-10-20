package index

import (
	"encoding/binary"
	"errors"
	"hash/fnv"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/ehsanranjbar/kvutil/iterutil"
)

// Keyed is a source of records that are known by their primary keys: the iterator of a lookup
// in an index, or a combination of such sources. The sources of one combination must be of the
// same store.
type Keyed[K, V any] interface {
	iterutil.Iterator[K, *V]
	// RawKey returns the primary key of the current record as the store encodes it.
	// The slice must not be modified and is valid until the iterator moves.
	RawKey() ([]byte, error)
	// Contains reports whether a record, with its primary key as the store encodes it, is one
	// of the records that the source returns. It does not depend on where the source is, and
	// it must work on a source that is closed.
	Contains(rawKey []byte, record *V) (bool, error)
}

// Combiner combines the results of lookups in several indexes.
// The zero Combiner keeps the primary keys that it has to remember as they are.
type Combiner struct {
	lossy bool
}

// Lossy is a Combiner that remembers primary keys of more than 8 bytes as hashes of 64 bits,
// which takes less memory. As two keys may have the same hash, a record whose hash is known
// is read and checked against the source that it would be of, so the results are the same.
var Lossy = Combiner{lossy: true}

// Intersect returns the records that all the sources have, in the order of the first one.
// The first source is read as it goes, and the primary keys of the others are remembered,
// so the source with the most records is best given first.
func Intersect[K, V any](sources ...Keyed[K, V]) (Keyed[K, V], error) {
	return Combiner{}.Intersect(sources...)
}

// Union returns the records that one of the sources at least has, each of them once:
// the records of the first source, then the ones of the second that the first does not have,
// and so on.
func Union[K, V any](sources ...Keyed[K, V]) (Keyed[K, V], error) {
	return Combiner{}.Union(sources...)
}

// Except returns the records of the first source that the second one does not have,
// in the order of the first one.
func Except[K, V any](a, b Keyed[K, V]) (Keyed[K, V], error) {
	return Combiner{}.Except(a, b)
}

// Intersect returns the records that all the sources have, in the order of the first one.
func (c Combiner) Intersect[K, V any](sources ...Keyed[K, V]) (Keyed[K, V], error) {
	if len(sources) == 0 {
		return nil, errors.New("index: nothing to intersect")
	}

	sets, err := c.collect(sources[1:])
	if err != nil {
		sources[0].Close()
		return nil, err
	}

	others := sources[1:]
	return newCombined(sources[:1], sources,
		func(_ int, src Keyed[K, V]) (bool, error) {
			raw, err := src.RawKey()
			if err != nil {
				return false, err
			}
			for j, set := range sets {
				if !set.has(raw) {
					return false, nil
				}
				if set.hashed() {
					// A hash that is known may be the hash of another key.
					record, err := src.Value()
					if err != nil {
						return false, err
					}
					if ok, err := others[j].Contains(raw, record); !ok || err != nil {
						return false, err
					}
				}
			}
			return true, nil
		},
		func(raw []byte, record *V) (bool, error) {
			for _, src := range sources {
				if ok, err := src.Contains(raw, record); !ok || err != nil {
					return false, err
				}
			}
			return true, nil
		}), nil
}

// Union returns the records that one of the sources at least has, each of them once.
func (c Combiner) Union[K, V any](sources ...Keyed[K, V]) (Keyed[K, V], error) {
	seen := newKeySet(c.lossy)
	return newCombined(sources, sources,
		func(i int, src Keyed[K, V]) (bool, error) {
			raw, err := src.RawKey()
			if err != nil {
				return false, err
			}
			if !seen.has(raw) {
				seen.add(raw)
				return true, nil
			}
			if !seen.hashed() {
				return false, nil
			}

			// The hash is known: the record was returned already if a source before this one has it.
			record, err := src.Value()
			if err != nil {
				return false, err
			}
			for _, earlier := range sources[:i] {
				if ok, err := earlier.Contains(raw, record); ok || err != nil {
					return false, err
				}
			}
			return true, nil
		},
		func(raw []byte, record *V) (bool, error) {
			for _, src := range sources {
				if ok, err := src.Contains(raw, record); ok || err != nil {
					return ok, err
				}
			}
			return false, nil
		}), nil
}

// Except returns the records of the first source that the second one does not have.
func (c Combiner) Except[K, V any](a, b Keyed[K, V]) (Keyed[K, V], error) {
	sets, err := c.collect([]Keyed[K, V]{b})
	if err != nil {
		a.Close()
		return nil, err
	}
	excluded := sets[0]

	return newCombined([]Keyed[K, V]{a}, []Keyed[K, V]{a, b},
		func(_ int, src Keyed[K, V]) (bool, error) {
			raw, err := src.RawKey()
			if err != nil {
				return false, err
			}
			if !excluded.has(raw) {
				return true, nil
			}
			if !excluded.hashed() {
				return false, nil
			}

			record, err := src.Value()
			if err != nil {
				return false, err
			}
			ok, err := b.Contains(raw, record)
			return !ok && err == nil, err
		},
		func(raw []byte, record *V) (bool, error) {
			if ok, err := a.Contains(raw, record); !ok || err != nil {
				return false, err
			}
			ok, err := b.Contains(raw, record)
			return !ok && err == nil, err
		}), nil
}

// collect reads the primary keys of each of the sources into a set, and closes the sources.
func (c Combiner) collect[K, V any](sources []Keyed[K, V]) ([]*keySet, error) {
	sets := make([]*keySet, 0, len(sources))
	var failure error
	for _, src := range sources {
		set := newKeySet(c.lossy)
		for ; failure == nil && src.Valid(); src.Next() {
			raw, err := src.RawKey()
			if err != nil {
				failure = err
				break
			}
			set.add(raw)
		}
		if failure == nil {
			failure = iterutil.Of(src).Err()
		}
		if err := src.Close(); failure == nil {
			failure = err
		}
		sets = append(sets, set)
	}
	return sets, failure
}

// combined returns the records of its sources, one source after the other, that it keeps.
type combined[K, V any] struct {
	sources  []Keyed[K, V] // the sources that are read
	all      []Keyed[K, V] // all the sources of the combination, to close
	keep     func(i int, src Keyed[K, V]) (bool, error)
	contains func(rawKey []byte, record *V) (bool, error)

	i       int
	settled bool
	err     error
}

func newCombined[K, V any](
	sources, all []Keyed[K, V],
	keep func(i int, src Keyed[K, V]) (bool, error),
	contains func(rawKey []byte, record *V) (bool, error),
) *combined[K, V] {
	return &combined[K, V]{sources: sources, all: all, keep: keep, contains: contains}
}

// settle moves to the next record that the combination keeps.
func (c *combined[K, V]) settle() {
	for !c.settled && c.err == nil && c.i < len(c.sources) {
		src := c.sources[c.i]
		if !src.Valid() {
			c.err = iterutil.Of(src).Err()
			c.i++
			continue
		}

		ok, err := c.keep(c.i, src)
		if err != nil {
			c.err = err
			return
		}
		if ok {
			c.settled = true
			return
		}
		src.Next()
	}
}

// Valid implements the iterutil.Iterator interface.
func (c *combined[K, V]) Valid() bool {
	c.settle()
	return c.settled && c.err == nil
}

// Next implements the iterutil.Iterator interface.
func (c *combined[K, V]) Next() {
	if !c.Valid() {
		return
	}
	c.sources[c.i].Next()
	c.settled = false
}

// Key implements the iterutil.Iterator interface.
func (c *combined[K, V]) Key() (K, error) {
	return c.sources[c.i].Key()
}

// Value implements the iterutil.Iterator interface.
func (c *combined[K, V]) Value() (*V, error) {
	return c.sources[c.i].Value()
}

// RawKey implements the Keyed interface.
func (c *combined[K, V]) RawKey() ([]byte, error) {
	return c.sources[c.i].RawKey()
}

// Contains implements the Keyed interface.
func (c *combined[K, V]) Contains(rawKey []byte, record *V) (bool, error) {
	return c.contains(rawKey, record)
}

// Err returns the error that stopped the iterator early, if there is one.
func (c *combined[K, V]) Err() error {
	return c.err
}

// Close implements the iterutil.Iterator interface.
func (c *combined[K, V]) Close() error {
	var first error
	for _, src := range c.all {
		if err := src.Close(); first == nil {
			first = err
		}
	}
	c.i = len(c.sources)
	return first
}

// keySet is a set of primary keys, as the store encodes them.
//
// The keys that all have the same size, of 8 bytes at most, are kept as numbers in a bitmap,
// which is what the keys that are numbers are. Other keys are kept as they are or, in a lossy
// set, as their hashes in a bitmap, in which case a key may seem to be in the set when it is not.
type keySet struct {
	lossy bool // whether keys that cannot be numbers are kept as hashes
	size  int  // the size of the keys that are in the bitmap as numbers: -1 before the first key, -2 if they are hashes
	bm    *roaring64.Bitmap
	keys  map[string]struct{}
}

func newKeySet(lossy bool) *keySet {
	return &keySet{lossy: lossy, size: -1, bm: roaring64.New()}
}

// keyValue returns a key of 8 bytes at most as a number, which sorts as the key does.
func keyValue(raw []byte) uint64 {
	var buf [8]byte
	copy(buf[8-len(raw):], raw)
	return binary.BigEndian.Uint64(buf[:])
}

// keyOf returns the key of a size that a number stands for.
func keyOf(value uint64, size int) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return buf[8-size:]
}

// keyHash returns the hash that a key is in a bitmap as. It is a variable so that the tests can
// make keys have the same hash.
var keyHash = func(raw []byte) uint64 {
	h := fnv.New64a()
	h.Write(raw)
	return h.Sum64()
}

// exact reports whether the keys of the set are in the bitmap as numbers.
func (s *keySet) exact() bool {
	return s.keys == nil && !s.hashed()
}

// hashed reports whether the keys of the set are in the bitmap as hashes.
func (s *keySet) hashed() bool {
	return s.size == -2
}

func (s *keySet) add(raw []byte) {
	switch {
	case s.keys != nil:
		s.keys[string(raw)] = struct{}{}
		return
	case s.hashed():
		s.bm.Add(keyHash(raw))
		return
	case s.size == -1 && len(raw) <= 8:
		s.size = len(raw)
	}
	if len(raw) == s.size {
		s.bm.Add(keyValue(raw))
		return
	}

	// A key of another size: the keys cannot be numbers any more.
	numbers, size := s.bm, s.size
	if s.lossy {
		s.bm, s.size = roaring64.New(), -2
	} else {
		s.keys = make(map[string]struct{})
	}
	if size >= 0 {
		for it := numbers.Iterator(); it.HasNext(); {
			s.add(keyOf(it.Next(), size))
		}
	}
	s.add(raw)
}

func (s *keySet) has(raw []byte) bool {
	switch {
	case s.keys != nil:
		_, ok := s.keys[string(raw)]
		return ok
	case s.hashed():
		return s.bm.Contains(keyHash(raw))
	}
	return len(raw) == s.size && s.bm.Contains(keyValue(raw))
}
