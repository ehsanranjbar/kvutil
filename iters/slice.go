package iters

import (
	"encoding/binary"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec/lex"
)

func Slice[T any](s []T) kvutil.Iterator[[]byte, T] {
	return &sliceIterator[T]{s: s}
}

type sliceIterator[T any] struct {
	s []T
	i int
}

// Close implements the Iterator interface.
func (it *sliceIterator[T]) Close() {}

// Item implements the Iterator interface.
func (it *sliceIterator[T]) Item() *badger.Item {
	return nil
}

// Next implements the Iterator interface.
func (it *sliceIterator[T]) Next() {
	it.i++
}

// Rewind implements the Iterator interface.
func (it *sliceIterator[T]) Rewind() {
	it.i = 0
}

// Seek implements the Iterator interface.
func (it *sliceIterator[T]) Seek(key []byte) {
	it.i = int(binary.BigEndian.Uint64(key))
}

// Valid implements the Iterator interface.
func (it *sliceIterator[T]) Valid() bool {
	return it.i < len(it.s)
}

// Key implements the Iterator interface.
func (it *sliceIterator[T]) Key() ([]byte, error) {
	return lex.EncodeInt64(int64(it.i)), nil
}

// Value implements the Iterator interface.
func (it *sliceIterator[T]) Value() (value T, err error) {
	return it.s[it.i], nil
}
