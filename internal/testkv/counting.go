package testkv

import (
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/kv"
)

// Counts is what a counting DB has been asked for.
type Counts struct {
	// Gets is the number of calls of Get of a transaction.
	Gets atomic.Int64
	// Values is the number of calls of Value of an iterator.
	Values atomic.Int64
}

// Reset sets the counts to zero.
func (c *Counts) Reset() {
	c.Gets.Store(0)
	c.Values.Store(0)
}

// Count returns a DB that works as the given one and counts the reads that its transactions make.
func Count(db kv.DB) (kv.DB, *Counts) {
	c := &Counts{}
	return countingDB{base: db, counts: c}, c
}

type countingDB struct {
	base   kv.DB
	counts *Counts
}

func (db countingDB) View(fn func(kv.Tx) error) error {
	return db.base.View(func(tx kv.Tx) error { return fn(countingTx{Tx: tx, counts: db.counts}) })
}

func (db countingDB) Update(fn func(kv.Tx) error) error {
	return db.base.Update(func(tx kv.Tx) error { return fn(countingTx{Tx: tx, counts: db.counts}) })
}

type countingTx struct {
	kv.Tx
	counts *Counts
}

func (tx countingTx) Get(key []byte) ([]byte, error) {
	tx.counts.Gets.Add(1)
	return tx.Tx.Get(key)
}

func (tx countingTx) Iterator(start, end []byte) (kv.Iterator, error) {
	it, err := tx.Tx.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return countingIterator{Iterator: it, counts: tx.counts}, nil
}

func (tx countingTx) ReverseIterator(start, end []byte) (kv.Iterator, error) {
	it, err := tx.Tx.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return countingIterator{Iterator: it, counts: tx.counts}, nil
}

type countingIterator struct {
	kv.Iterator
	counts *Counts
}

func (it countingIterator) Value() ([]byte, error) {
	it.counts.Values.Add(1)
	return it.Iterator.Value()
}
