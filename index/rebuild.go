package index

import (
	"context"
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/kv"
	refstore "github.com/ehsanranjbar/kvutil/store/ref"
)

// rebuildBatch is the number of records that Rebuild indexes in a transaction.
const rebuildBatch = 500

// Rebuild builds an index again from the records of its store: it deletes the entries that
// the index has and writes the ones of every record. It is how an index that is defined on a
// store with records in it gets to know of them, and the Migrate of the schema calls it for
// an index that is new, that has changed, or whose store was migrated.
//
// It must not run while records are written to the store. A unique index of records that do
// not all have keys of their own fails with ErrUniqueViolation, and is left unfinished.
func Rebuild[K, V any](ctx context.Context, db kv.DB, x *Index[K, V]) error {
	r := x.refs.Namespace().Range()
	if err := kv.DeleteRange(ctx, db, r.Start, r.End); err != nil {
		return fmt.Errorf("failed to clear the index %q: %w", x.name, err)
	}

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
			it, err := x.store.Records(tx, r)
			if err != nil {
				return err
			}
			defer it.Close()

			refs := x.refs.Instantiate(tx)
			for ; it.Valid() && batchSize < rebuildBatch; it.Next() {
				batchSize++
				key, err := it.Key()
				if err != nil {
					return err
				}
				record, err := it.Value()
				if err != nil {
					return err
				}
				if err := x.add(refs, key, record); err != nil {
					return err
				}
				batchLast = &key
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to build the index %q: %w", x.name, err)
		}
		last, more = batchLast, batchSize == rebuildBatch
	}
	return nil
}

// add writes the entries of a record.
func (x *Index[K, V]) add(refs *refstore.Instance, key K, record *V) error {
	rawKey, err := x.store.KeyCodec().Encode(key)
	if err != nil {
		return err
	}
	entries, err := x.indexer.Index(record)
	if err != nil {
		return fmt.Errorf("failed to index the record %v: %w", key, err)
	}

	for _, e := range entries {
		err := refs.Set(e.Key, rawKey, e.Value)
		if errors.Is(err, refstore.ErrDuplicate) {
			return fmt.Errorf("%w: the record %v has a key that %s has", ErrUniqueViolation, key, x.holder(refs, e.Key))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// holder names the record that has a key of a unique index, for an error.
func (x *Index[K, V]) holder(refs *refstore.Instance, indexKey []byte) string {
	it, err := refs.Iterator(indexKey, kv.PrefixEnd(indexKey))
	if err != nil {
		return "another record"
	}
	defer it.Close()

	if !it.Valid() {
		return "another record"
	}
	rawKey, err := it.PrimaryKey()
	if err != nil {
		return "another record"
	}
	key, err := x.store.KeyCodec().Decode(rawKey)
	if err != nil {
		return "another record"
	}
	return fmt.Sprintf("the record %v", key)
}
