// Package iters provides ways to transform and consume the iterators of the stores.
package iters

import (
	"iter"

	"github.com/ehsanranjbar/kvutil"
)

// Seq is an iterator with methods to transform and consume it. It moves forward only.
type Seq[K, V any] struct {
	kvutil.Iterator[K, V]
}

// From wraps an iterator as a Seq.
func From[K, V any](it kvutil.Iterator[K, V]) Seq[K, V] {
	return Seq[K, V]{Iterator: it}
}

// errorer is implemented by the iterators that can stop because of an error.
type errorer interface {
	Err() error
}

// Err returns the error that stopped the iterator early, if there is one.
func (s Seq[K, V]) Err() error {
	if e, ok := s.Iterator.(errorer); ok {
		return e.Err()
	}
	return nil
}

// Filter returns the records that the function accepts.
func (s Seq[K, V]) Filter(f func(K, V) bool) Seq[K, V] {
	return From(&filterIterator[K, V]{base: s.Iterator, f: f})
}

// Limit returns the first n records.
func (s Seq[K, V]) Limit(n int) Seq[K, V] {
	return From(&limitIterator[K, V]{base: s.Iterator, n: n})
}

// Map returns the records with their values replaced by the results of the function.
func (s Seq[K, V]) Map[U any](f func(K, V) (U, error)) Seq[K, U] {
	return From(&mapIterator[K, V, U]{base: s.Iterator, f: f})
}

// Aggregate folds the records into a state with the function, and closes the iterator.
func (s Seq[K, V]) Aggregate[S any](state S, f func(S, K, V) S) (S, error) {
	defer s.Close()

	for ; s.Valid(); s.Next() {
		key, err := s.Key()
		if err != nil {
			return state, err
		}
		value, err := s.Value()
		if err != nil {
			return state, err
		}
		state = f(state, key, value)
	}
	return state, s.Err()
}

// Keys collects the keys of the records, and closes the iterator.
func (s Seq[K, V]) Keys() ([]K, error) {
	defer s.Close()

	var keys []K
	for ; s.Valid(); s.Next() {
		key, err := s.Key()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, s.Err()
}

// Values collects the values of the records, and closes the iterator.
func (s Seq[K, V]) Values() ([]V, error) {
	defer s.Close()

	var values []V
	for ; s.Valid(); s.Next() {
		value, err := s.Value()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, s.Err()
}

// All returns the records as a sequence to range over. The iterator is closed when the loop
// ends. An error stops the loop and is stored in err, which must be checked after it.
func (s Seq[K, V]) All(err *error) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		defer s.Close()

		for ; s.Valid(); s.Next() {
			key, e := s.Key()
			if e != nil {
				*err = e
				return
			}
			value, e := s.Value()
			if e != nil {
				*err = e
				return
			}
			if !yield(key, value) {
				return
			}
		}
		*err = s.Err()
	}
}

type filterIterator[K, V any] struct {
	base kvutil.Iterator[K, V]
	f    func(K, V) bool

	// The current record, once the iterator has settled on one that the function accepts.
	settled bool
	ok      bool
	key     K
	value   V
	err     error
}

// settle moves the base iterator to the next record that the function accepts.
func (it *filterIterator[K, V]) settle() {
	for !it.settled {
		it.ok = false
		if it.err != nil || !it.base.Valid() {
			it.settled = true
			return
		}

		key, err := it.base.Key()
		if err != nil {
			it.err = err
			continue
		}
		value, err := it.base.Value()
		if err != nil {
			it.err = err
			continue
		}

		if it.f(key, value) {
			it.key, it.value = key, value
			it.ok, it.settled = true, true
			return
		}
		it.base.Next()
	}
}

// Valid implements the kvutil.Iterator interface.
func (it *filterIterator[K, V]) Valid() bool {
	it.settle()
	return it.ok
}

// Next implements the kvutil.Iterator interface.
func (it *filterIterator[K, V]) Next() {
	it.settle()
	if it.ok {
		it.base.Next()
		it.settled = false
	}
}

// Key implements the kvutil.Iterator interface.
func (it *filterIterator[K, V]) Key() (K, error) {
	it.settle()
	return it.key, it.err
}

// Value implements the kvutil.Iterator interface.
func (it *filterIterator[K, V]) Value() (V, error) {
	it.settle()
	return it.value, it.err
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *filterIterator[K, V]) Err() error {
	if it.err != nil {
		return it.err
	}
	return From(it.base).Err()
}

// Close implements the kvutil.Iterator interface.
func (it *filterIterator[K, V]) Close() error {
	return it.base.Close()
}

type limitIterator[K, V any] struct {
	base kvutil.Iterator[K, V]
	n    int
}

// Valid implements the kvutil.Iterator interface.
func (it *limitIterator[K, V]) Valid() bool {
	return it.n > 0 && it.base.Valid()
}

// Next implements the kvutil.Iterator interface.
func (it *limitIterator[K, V]) Next() {
	if it.Valid() {
		it.n--
		it.base.Next()
	}
}

// Key implements the kvutil.Iterator interface.
func (it *limitIterator[K, V]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the kvutil.Iterator interface.
func (it *limitIterator[K, V]) Value() (V, error) {
	return it.base.Value()
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *limitIterator[K, V]) Err() error {
	return From(it.base).Err()
}

// Close implements the kvutil.Iterator interface.
func (it *limitIterator[K, V]) Close() error {
	return it.base.Close()
}

type mapIterator[K, V, U any] struct {
	base kvutil.Iterator[K, V]
	f    func(K, V) (U, error)
}

// Valid implements the kvutil.Iterator interface.
func (it *mapIterator[K, V, U]) Valid() bool {
	return it.base.Valid()
}

// Next implements the kvutil.Iterator interface.
func (it *mapIterator[K, V, U]) Next() {
	it.base.Next()
}

// Key implements the kvutil.Iterator interface.
func (it *mapIterator[K, V, U]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the kvutil.Iterator interface.
func (it *mapIterator[K, V, U]) Value() (u U, err error) {
	key, err := it.base.Key()
	if err != nil {
		return u, err
	}
	value, err := it.base.Value()
	if err != nil {
		return u, err
	}
	return it.f(key, value)
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *mapIterator[K, V, U]) Err() error {
	return From(it.base).Err()
}

// Close implements the kvutil.Iterator interface.
func (it *mapIterator[K, V, U]) Close() error {
	return it.base.Close()
}
