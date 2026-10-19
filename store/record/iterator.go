package record

import (
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/store/typed"
)

var _ iterutil.Iterator[int64, *struct{}] = (*Iterator[int64, struct{}])(nil)

// Iterator iterates over the records of a Store. The id is set on every record that it returns.
type Iterator[I comparable, T any] struct {
	base  *typed.Iterator[I, T]
	setID func(*T, I)
}

// Valid implements the iterutil.Iterator interface.
func (it *Iterator[I, T]) Valid() bool {
	return it.base.Valid()
}

// Next implements the iterutil.Iterator interface.
func (it *Iterator[I, T]) Next() {
	it.base.Next()
}

// Seek moves to the given id or, if it is not in the range of the iterator, to the first id
// after it in the direction of the iterator.
func (it *Iterator[I, T]) Seek(id I) error {
	return it.base.Seek(id)
}

// Key implements the iterutil.Iterator interface.
func (it *Iterator[I, T]) Key() (I, error) {
	return it.base.Key()
}

// Value implements the iterutil.Iterator interface. The id is set on the record.
func (it *Iterator[I, T]) Value() (*T, error) {
	id, err := it.base.Key()
	if err != nil {
		return nil, err
	}

	v, err := it.base.Value()
	if err != nil {
		return nil, err
	}
	it.setID(v, id)
	return v, nil
}

// RawKey returns the current id as the store encodes it.
// The slice must not be modified and is valid until the iterator moves.
func (it *Iterator[I, T]) RawKey() []byte {
	return it.base.RawKey()
}

// Close implements the iterutil.Iterator interface.
func (it *Iterator[I, T]) Close() error {
	return it.base.Close()
}
