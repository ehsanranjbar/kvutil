// Package iterutil has the iterator of typed records that the stores return, and ways to
// transform and consume it.
package iterutil

import (
	"errors"
	"iter"

	"github.com/ehsanranjbar/kvutil/kv"
)

// Iterator iterates over typed records. It is what the iterators of the stores, of the indexes
// and of everything else that returns records have in common.
type Iterator[K, V any] interface {
	// Valid reports whether the iterator is positioned at a record.
	Valid() bool
	// Next moves to the next record.
	Next()
	// Key returns the key of the current record.
	Key() (K, error)
	// Value returns the value of the current record.
	Value() (V, error)
	// Close releases the iterator.
	Close() error
}

// AdvancedIterator is an iterator with methods to transform and consume it. It moves forward only.
type AdvancedIterator[K, V any] struct {
	Iterator[K, V]
}

// Of wraps an iterator as an AdvancedIterator.
func Of[K, V any](it Iterator[K, V]) AdvancedIterator[K, V] {
	return AdvancedIterator[K, V]{Iterator: it}
}

// errorer is implemented by the iterators that can stop because of an error.
type errorer interface {
	Err() error
}

// Err returns the error that stopped the iterator early, if there is one.
func (s AdvancedIterator[K, V]) Err() error {
	if e, ok := s.Iterator.(errorer); ok {
		return e.Err()
	}
	return nil
}

// Filter returns the records that the function accepts.
func (s AdvancedIterator[K, V]) Filter(f func(K, V) bool) AdvancedIterator[K, V] {
	return Of(&filterIterator[K, V]{base: s.Iterator, f: f})
}

// Limit returns the first n records.
func (s AdvancedIterator[K, V]) Limit(n int) AdvancedIterator[K, V] {
	return Of(&limitIterator[K, V]{base: s.Iterator, n: n})
}

// Map returns the records with their values replaced by the results of the function.
func (s AdvancedIterator[K, V]) Map[U any](f func(K, V) (U, error)) AdvancedIterator[K, U] {
	return Of(&mapIterator[K, V, U]{base: s.Iterator, f: f})
}

// Lookup returns the records with their values replaced by what the function gets for them,
// like the records of a store that the values are the keys of. A record that the function
// returns kv.ErrNotFound for is left out, which is what Map cannot do.
func (s AdvancedIterator[K, V]) Lookup[U any](get func(V) (U, error)) AdvancedIterator[K, U] {
	return Of(&lookupIterator[K, V, U]{base: s.Iterator, get: get})
}

// FlatMap returns the records of the iterators that the function returns for the records, one
// iterator after the other. The function may return nil for a record that has none.
// Every iterator that the function returns is closed when it is done with.
func (s AdvancedIterator[K, V]) FlatMap[L, U any](f func(K, V) (Iterator[L, U], error)) AdvancedIterator[L, U] {
	return Of(&flatMapIterator[K, V, L, U]{base: s.Iterator, f: f})
}

// Aggregate folds the records into a state with the function, and closes the iterator.
func (s AdvancedIterator[K, V]) Aggregate[S any](state S, f func(S, K, V) S) (S, error) {
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
func (s AdvancedIterator[K, V]) Keys() ([]K, error) {
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
func (s AdvancedIterator[K, V]) Values() ([]V, error) {
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
func (s AdvancedIterator[K, V]) All(err *error) iter.Seq2[K, V] {
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
	base Iterator[K, V]
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

// Valid implements the Iterator interface.
func (it *filterIterator[K, V]) Valid() bool {
	it.settle()
	return it.ok
}

// Next implements the Iterator interface.
func (it *filterIterator[K, V]) Next() {
	it.settle()
	if it.ok {
		it.base.Next()
		it.settled = false
	}
}

// Key implements the Iterator interface.
func (it *filterIterator[K, V]) Key() (K, error) {
	it.settle()
	return it.key, it.err
}

// Value implements the Iterator interface.
func (it *filterIterator[K, V]) Value() (V, error) {
	it.settle()
	return it.value, it.err
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *filterIterator[K, V]) Err() error {
	if it.err != nil {
		return it.err
	}
	return Of(it.base).Err()
}

// Close implements the Iterator interface.
func (it *filterIterator[K, V]) Close() error {
	return it.base.Close()
}

type limitIterator[K, V any] struct {
	base Iterator[K, V]
	n    int
}

// Valid implements the Iterator interface.
func (it *limitIterator[K, V]) Valid() bool {
	return it.n > 0 && it.base.Valid()
}

// Next implements the Iterator interface.
func (it *limitIterator[K, V]) Next() {
	if it.Valid() {
		it.n--
		it.base.Next()
	}
}

// Key implements the Iterator interface.
func (it *limitIterator[K, V]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
func (it *limitIterator[K, V]) Value() (V, error) {
	return it.base.Value()
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *limitIterator[K, V]) Err() error {
	return Of(it.base).Err()
}

// Close implements the Iterator interface.
func (it *limitIterator[K, V]) Close() error {
	return it.base.Close()
}

type mapIterator[K, V, U any] struct {
	base Iterator[K, V]
	f    func(K, V) (U, error)
}

// Valid implements the Iterator interface.
func (it *mapIterator[K, V, U]) Valid() bool {
	return it.base.Valid()
}

// Next implements the Iterator interface.
func (it *mapIterator[K, V, U]) Next() {
	it.base.Next()
}

// Key implements the Iterator interface.
func (it *mapIterator[K, V, U]) Key() (K, error) {
	return it.base.Key()
}

// Value implements the Iterator interface.
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
	return Of(it.base).Err()
}

// Close implements the Iterator interface.
func (it *mapIterator[K, V, U]) Close() error {
	return it.base.Close()
}

type lookupIterator[K, V, U any] struct {
	base Iterator[K, V]
	get  func(V) (U, error)

	// The current record, once the iterator has settled on one that the function gets a value for.
	settled bool
	ok      bool
	key     K
	value   U
	err     error
}

// settle moves the base iterator to the next record that the function gets a value for.
func (it *lookupIterator[K, V, U]) settle() {
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

		got, err := it.get(value)
		switch {
		case errors.Is(err, kv.ErrNotFound):
			it.base.Next()
		case err != nil:
			it.err = err
		default:
			it.key, it.value = key, got
			it.ok, it.settled = true, true
		}
	}
}

// Valid implements the Iterator interface.
func (it *lookupIterator[K, V, U]) Valid() bool {
	it.settle()
	return it.ok
}

// Next implements the Iterator interface.
func (it *lookupIterator[K, V, U]) Next() {
	it.settle()
	if it.ok {
		it.base.Next()
		it.settled = false
	}
}

// Key implements the Iterator interface.
func (it *lookupIterator[K, V, U]) Key() (K, error) {
	it.settle()
	return it.key, it.err
}

// Value implements the Iterator interface.
func (it *lookupIterator[K, V, U]) Value() (U, error) {
	it.settle()
	return it.value, it.err
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *lookupIterator[K, V, U]) Err() error {
	if it.err != nil {
		return it.err
	}
	return Of(it.base).Err()
}

// Close implements the Iterator interface.
func (it *lookupIterator[K, V, U]) Close() error {
	return it.base.Close()
}

type flatMapIterator[K, V, L, U any] struct {
	base Iterator[K, V]
	f    func(K, V) (Iterator[L, U], error)

	// The iterator of the current record of the base iterator, if it has one.
	inner   Iterator[L, U]
	settled bool
	err     error
}

// settle moves to the next record of the base iterator whose iterator has a record, unless
// the current one has.
func (it *flatMapIterator[K, V, L, U]) settle() {
	for !it.settled {
		if it.inner != nil {
			if it.inner.Valid() {
				it.settled = true
				return
			}
			// What stopped an iterator early stops everything.
			err := Of(it.inner).Err()
			if cerr := it.inner.Close(); err == nil {
				err = cerr
			}
			it.inner = nil
			if err != nil {
				it.err = err
			}
			it.base.Next()
		}

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

		inner, err := it.f(key, value)
		switch {
		case err != nil:
			it.err = err
		case inner == nil:
			it.base.Next()
		default:
			it.inner = inner
		}
	}
}

// Valid implements the Iterator interface.
func (it *flatMapIterator[K, V, L, U]) Valid() bool {
	it.settle()
	return it.inner != nil
}

// Next implements the Iterator interface.
func (it *flatMapIterator[K, V, L, U]) Next() {
	it.settle()
	if it.inner != nil {
		it.inner.Next()
		it.settled = false
	}
}

// Key implements the Iterator interface.
func (it *flatMapIterator[K, V, L, U]) Key() (key L, err error) {
	it.settle()
	if it.inner == nil {
		return key, it.err
	}
	return it.inner.Key()
}

// Value implements the Iterator interface.
func (it *flatMapIterator[K, V, L, U]) Value() (value U, err error) {
	it.settle()
	if it.inner == nil {
		return value, it.err
	}
	return it.inner.Value()
}

// Err returns the error that stopped the iterator early, if there is one.
func (it *flatMapIterator[K, V, L, U]) Err() error {
	if it.err != nil {
		return it.err
	}
	return Of(it.base).Err()
}

// Close implements the Iterator interface.
func (it *flatMapIterator[K, V, L, U]) Close() error {
	var err error
	if it.inner != nil {
		err = it.inner.Close()
		it.inner = nil
	}
	it.settled = true
	if cerr := it.base.Close(); err == nil {
		err = cerr
	}
	return err
}
