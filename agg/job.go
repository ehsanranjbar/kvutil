package agg

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/upkeep"
)

// DefaultBatchSize is the default number of notes that an aggregation takes in one transaction,
// which is what an upkeep.Runner that was not given a batch size leaves to it.
const DefaultBatchSize = 1000

// job is the background work of an aggregation: turning what the transactions have noted for it
// into its stored states. It is the job that the extension of the aggregation gives to an
// upkeep.Runner. It holds only the pointer, so that it is comparable, as a job has to be.
type job[K, V, G, X, S any] struct {
	a *Aggregation[K, V, G, X, S]
}

// Name implements the upkeep.Job interface.
func (j job[K, V, G, X, S]) Name() string {
	return fmt.Sprintf("aggregation %q", j.a.name)
}

// Step implements the upkeep.Job interface. It turns a batch of what was noted after the cursor
// into stored states, and returns the cursor to go on from, or nil when there is nothing after
// it. A pass collects what was noted before it started and goes forward only, so it has no more to
// do when it is drained than when it is not.
func (j job[K, V, G, X, S]) Step(_ context.Context, db kv.DB, pass upkeep.Pass) ([]byte, error) {
	batch := pass.Batch
	if batch <= 0 {
		batch = DefaultBatchSize
	}
	return j.a.kind.collect(db, pass.Cursor, batch, pass.Report)
}

// RebuildOption is an option of Rebuild.
type RebuildOption func(*rebuildOptions)

type rebuildOptions struct {
	batch int
}

// WithBatchSize sets the number of records that Rebuild notes, and then the number of notes that
// it collects, in one transaction. It is DefaultBatchSize by default.
func WithBatchSize(n int) RebuildOption {
	return func(o *rebuildOptions) {
		o.batch = n
	}
}

// Rebuild builds an aggregation again from the records of its store, which is how an
// aggregation that is added to a store with records in it gets to know of them.
// It must not run while records are written to the store.
func Rebuild[K, V, G, X, S any](ctx context.Context, db kv.DB, a *Aggregation[K, V, G, X, S], opts ...RebuildOption) error {
	o := rebuildOptions{batch: DefaultBatchSize}
	for _, opt := range opts {
		opt(&o)
	}
	if o.batch <= 0 {
		o.batch = DefaultBatchSize
	}

	// Everything that the aggregation has stored goes first.
	if err := kv.DeleteRange(ctx, db, a.prefix, kv.PrefixEnd(a.prefix)); err != nil {
		return fmt.Errorf("failed to clear %s: %w", a.name, err)
	}

	// The records are then noted as if they were new, a batch at a time.
	var last *K
	for more := true; more; {
		if err := ctx.Err(); err != nil {
			return err
		}

		// What a batch finds is only kept once its transaction is committed, as the function
		// may run more than once.
		var (
			batchLast *K
			batchSize int
		)
		err := db.Update(func(tx kv.Tx) error {
			batchLast, batchSize = last, 0

			var r kvutil.Range[K]
			if last != nil {
				r = r.After(*last)
			}
			it, err := a.store.Records(tx, r)
			if err != nil {
				return err
			}
			defer it.Close()

			m := &maintenance[K, V, G, X, S]{a: a, tx: prefix.New(tx, a.prefix)}
			for ; it.Valid() && batchSize < o.batch; it.Next() {
				batchSize++
				key, err := it.Key()
				if err != nil {
					return err
				}
				record, err := it.Value()
				if err != nil {
					return err
				}
				rawKey, err := a.store.KeyCodec().Encode(key)
				if err != nil {
					return err
				}
				if err := m.change(rawKey, nil, record); err != nil {
					return err
				}
				batchLast = &key
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to rebuild %s: %w", a.name, err)
		}
		last, more = batchLast, batchSize == o.batch
	}

	// What was noted is collected, and the rebuild is over when it is all in the stored states.
	return upkeep.New(db, upkeep.WithBatchSize(o.batch)).AddJob(job[K, V, G, X, S]{a: a}).Drain(ctx)
}
