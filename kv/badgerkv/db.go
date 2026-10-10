// Package badgerkv adapts badger to the kv interfaces.
package badgerkv

import (
	"bytes"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
)

// DefaultMaxRetries is the default number of times that Update retries a conflicting transaction.
const DefaultMaxRetries = 10

type options struct {
	prefetch   int
	maxRetries int
}

func newOptions(opts []Option) options {
	o := options{maxRetries: DefaultMaxRetries}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Option is an option of a DB or of a wrapped transaction.
type Option func(*options)

// WithPrefetch makes iterators prefetch the values of the next n keys in the background,
// which speeds up the scans that read every value. Iterators do not prefetch by default.
func WithPrefetch(n int) Option {
	return func(o *options) {
		o.prefetch = n
	}
}

// WithMaxRetries sets the number of times that Update retries a conflicting transaction.
// It has no effect on a wrapped transaction.
func WithMaxRetries(n int) Option {
	return func(o *options) {
		o.maxRetries = n
	}
}

// DB is a badger database as a kv.DB.
type DB struct {
	db   *badger.DB
	opts options
}

// Open opens a badger database.
func Open(opt badger.Options, opts ...Option) (*DB, error) {
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	return Wrap(db, opts...), nil
}

// Wrap wraps a badger database that is already open.
func Wrap(db *badger.DB, opts ...Option) *DB {
	return &DB{
		db:   db,
		opts: newOptions(opts),
	}
}

// Badger returns the underlying badger database, for the operations that kv does not cover.
func (d *DB) Badger() *badger.DB {
	return d.db
}

// Close closes the database.
func (d *DB) Close() error {
	return d.db.Close()
}

// View implements the kv.DB interface.
func (d *DB) View(fn func(kv.Tx) error) error {
	err := d.db.View(func(txn *badger.Txn) error {
		return fn(&tx{txn: txn, prefetch: d.opts.prefetch})
	})
	return mapError(err)
}

// Update implements the kv.DB interface.
// A transaction that conflicts with another one is run again, up to the configured number of
// retries, so the function must not have side effects outside of the transaction.
func (d *DB) Update(fn func(kv.Tx) error) error {
	for attempt := 0; ; attempt++ {
		err := d.db.Update(func(txn *badger.Txn) error {
			return fn(&tx{txn: txn, writable: true, prefetch: d.opts.prefetch})
		})
		if !errors.Is(err, badger.ErrConflict) && !errors.Is(err, kv.ErrConflict) {
			return mapError(err)
		}
		if attempt >= d.opts.maxRetries {
			return kv.ErrConflict
		}
	}
}

// DropRange implements the kv.RangeDropper interface.
// A range that covers exactly the keys of one prefix is dropped with badger's DropPrefix,
// any other range is scanned and deleted.
func (d *DB) DropRange(start, end []byte) error {
	if len(start) == 0 && len(end) == 0 {
		return d.db.DropAll()
	}
	if len(start) > 0 && bytes.Equal(end, kv.PrefixEnd(start)) {
		return d.db.DropPrefix(start)
	}

	wb := d.db.NewWriteBatch()
	defer wb.Cancel()

	err := d.db.View(func(txn *badger.Txn) error {
		it, err := (&tx{txn: txn}).Iterator(start, end)
		if err != nil {
			return err
		}
		defer it.Close()

		for ; it.Valid(); it.Next() {
			if err := wb.Delete(bytes.Clone(it.Key())); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return mapError(err)
	}
	return mapError(wb.Flush())
}

// NewBulkWriter implements the kv.Bulker interface.
func (d *DB) NewBulkWriter() kv.BulkWriter {
	return &bulkWriter{wb: d.db.NewWriteBatch()}
}

type bulkWriter struct {
	wb *badger.WriteBatch
}

// Set implements the kv.BulkWriter interface.
func (w *bulkWriter) Set(key, value []byte) error {
	return mapError(w.wb.Set(bytes.Clone(key), bytes.Clone(value)))
}

// Delete implements the kv.BulkWriter interface.
func (w *bulkWriter) Delete(key []byte) error {
	return mapError(w.wb.Delete(bytes.Clone(key)))
}

// Flush implements the kv.BulkWriter interface.
func (w *bulkWriter) Flush() error {
	return mapError(w.wb.Flush())
}

// Cancel implements the kv.BulkWriter interface.
func (w *bulkWriter) Cancel() {
	w.wb.Cancel()
}

// mapError maps the errors of badger to the errors of kv.
func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return kv.ErrNotFound
	case errors.Is(err, badger.ErrEmptyKey):
		return kv.ErrEmptyKey
	case errors.Is(err, badger.ErrReadOnlyTxn):
		return kv.ErrReadOnly
	case errors.Is(err, badger.ErrConflict):
		return kv.ErrConflict
	case errors.Is(err, badger.ErrTxnTooBig):
		return kv.ErrTxnTooBig
	}
	return err
}
