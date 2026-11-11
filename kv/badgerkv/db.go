// Package badgerkv adapts badger to the kv interfaces.
package badgerkv

import (
	"bytes"
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
)

// DefaultMaxRetries is the default number of times that Update retries a conflicting transaction.
const DefaultMaxRetries = 10

type options struct {
	prefetch   int
	maxRetries int
	gc         bool
	gcInterval time.Duration
	gcRatio    float64

	groupCommit bool
}

func newOptions(opts []Option) options {
	o := options{
		maxRetries:  DefaultMaxRetries,
		gcInterval:  DefaultGCInterval,
		gcRatio:     DefaultGCDiscardRatio,
		groupCommit: true,
	}
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

// WithoutGroupCommit keeps Open from committing together the writes of a database that syncs
// its writes: badger then syncs after every transaction, as it does by itself. It has no
// effect on a wrapped database, or on one that does not sync its writes.
func WithoutGroupCommit() Option {
	return func(o *options) {
		o.groupCommit = false
	}
}

// DB is a badger database as a kv.DB. It also looks after the database: it collects the
// garbage of the value log in the background, and releases the sequences that it has given
// out when it is closed.
type DB struct {
	db   *badger.DB
	opts options
	m    maintenance

	// group is the syncer that the commits wait for, if the DB commits together the writes of a
	// database that syncs them. mem and vlog are the logs that its rounds look after.
	group *syncer
	mem   logFiles
	vlog  logFiles
}

// Open opens a badger database. The DB collects the garbage of its value log every
// DefaultGCInterval, unless an option says otherwise.
//
// If the options have SyncWrites on, the DB commits the writes together: badger is opened with
// SyncWrites off, and an Update that wrote returns when the data has been synced by a round
// that began after its commit, which covers all the commits that waited for it, so writers that
// are many are not slower for it. A commit is visible to the other transactions before it is
// durable, and Update returns once it is. An error of the sync is returned by Update although
// the transaction is committed and visible, as the errors of badger's own sync are.
// WithoutGroupCommit keeps badger's own sync, which is once for every transaction. A database
// that is kept in memory or read-only has nothing to sync.
func Open(opt badger.Options, opts ...Option) (*DB, error) {
	group := newOptions(opts).groupCommit && opt.SyncWrites && !opt.InMemory && !opt.ReadOnly
	if group {
		opt = opt.WithSyncWrites(false)
	}
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}
	// The garbage is collected unless the options say that it is not.
	d := Wrap(db, append([]Option{WithValueLogGC(DefaultGCInterval, DefaultGCDiscardRatio)}, opts...)...)
	if group {
		if err := d.startGroupCommit(); err != nil {
			return nil, errors.Join(err, d.Close())
		}
	}
	return d, nil
}

// Wrap wraps a badger database that is already open. The DB does not collect the garbage of
// the value log, unless WithValueLogGC is given. It does not commit the writes together
// either, as that needs the database to be opened with SyncWrites off, which cannot be changed
// once it is open: the DB leaves the syncs to badger, whichever way it was opened.
func Wrap(db *badger.DB, opts ...Option) *DB {
	d := &DB{
		db:   db,
		opts: newOptions(opts),
	}
	d.start()
	return d
}

// Badger returns the underlying badger database, for the operations that kv does not cover.
func (d *DB) Badger() *badger.DB {
	return d.db
}

// Close stops what the DB runs in the background, releases the sequences that it has given
// out, and closes the database. Closing it again does nothing but closing the database again.
func (d *DB) Close() error {
	err := d.shutdown()
	return errors.Join(err, d.db.Close())
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
//
// A transaction that wrote waits for the sync that Open sets up before it returns (see Open).
// A transaction that only read does not. Sequence does not wait either: a lease that a sequence
// wrote is covered by the round of any commit that uses its numbers, as a round covers
// everything that was written before it began.
func (d *DB) Update(fn func(kv.Tx) error) error {
	for attempt := 0; ; attempt++ {
		var wrote bool
		err := d.db.Update(func(txn *badger.Txn) error {
			t := &tx{txn: txn, writable: true, prefetch: d.opts.prefetch}
			err := fn(t)
			wrote = t.wrote
			return err
		})
		if err == nil && wrote {
			return awaitSync(d.group, false)
		}
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
// any other range is scanned and deleted. The drop is durable when it returns, with the sync
// that Open sets up. A drop can rotate the logs many times, so that sync lists the directories
// of the logs instead of looking for the files that were added.
func (d *DB) DropRange(start, end []byte) error {
	if err := d.dropRange(start, end); err != nil {
		return err
	}
	return awaitSync(d.group, true)
}

func (d *DB) dropRange(start, end []byte) error {
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

// NewBulkWriter implements the kv.BulkOpener interface.
func (d *DB) NewBulkWriter() kv.BulkWriter {
	return &bulkWriter{wb: d.db.NewWriteBatch(), group: d.group}
}

type bulkWriter struct {
	wb    *badger.WriteBatch
	group *syncer // nil if the DB does not commit together
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
	if err := w.wb.Flush(); err != nil {
		return mapError(err)
	}
	// A bulk write can rotate the logs many times, so the sync lists their directories.
	return awaitSync(w.group, true)
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
