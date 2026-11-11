package vector

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
)

// Rebuild builds an index again from the records of its store: it deletes the entries that
// the index has and writes the ones of every record. It is how an index that is defined on a
// store with records in it gets to know of them, and migrate.Run calls it for an index that is
// new, that has changed, or whose store was migrated.
//
// An index that is partitioned is given its partitions: the vectors are grouped around
// centroids that are found from a sample of them, and every vector is written in the partition
// of the nearest one. An index with no more vectors than the size of a partition has none, and
// every vector is unplaced.
//
// It must not run while records are written to the store. A record with a vector that is not
// valid fails the build, which is left unfinished.
func Rebuild[K, V any](ctx context.Context, db kv.DB, x *Index[K, V]) error {
	start := x.prefix
	if err := kv.DeleteRange(ctx, db, start, kv.PrefixEnd(start)); err != nil {
		return fmt.Errorf("failed to clear the index %q: %w", x.name, err)
	}

	if x.size == 0 {
		return x.walk(ctx, db, true, func(tx kv.Tx, items []item) error {
			data := prefix.New(tx, x.prefix)
			for _, it := range items {
				if err := data.Set(vectorKey(unplaced, it.rawKey), it.stored); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// The records are gone through twice, so that every vector is written once, to its partition:
	// the first time to find the partitions, and the second to fill them.
	count, sample, err := x.sample(ctx, db)
	if err != nil {
		return err
	}
	var parts *partitions
	if count > x.size {
		parts, err = x.writePartitions(ctx, db, count, sample)
		if err != nil {
			return err
		}
	}

	return x.walk(ctx, db, true, func(tx kv.Tx, items []item) error {
		data := prefix.New(tx, x.prefix)
		var pids []uint32
		if parts != nil {
			pids = x.assignAll(parts, items)
		}
		for i, it := range items {
			if pids == nil {
				if err := data.Set(vectorKey(unplaced, it.rawKey), it.stored); err != nil {
					return err
				}
				continue
			}
			if err := data.Set(vectorKey(pids[i], it.rawKey), it.stored); err != nil {
				return err
			}
			if err := data.Set(placeKey(it.rawKey), binary.BigEndian.AppendUint32(nil, pids[i])); err != nil {
				return err
			}
		}
		return nil
	})
}

// item is the vector of a record, as it is stored.
type item struct {
	rawKey []byte
	stored []byte
}

// walk goes through the records of the store in batches, each in a transaction of its own, and
// gives visit the vectors of the records of a batch. A record without a vector is left out, and
// one with a vector that is not valid fails the walk.
//
// visit runs in the transaction if the walk writes, which may run more than once, and after it if
// not, when it is sure to run once.
func (x *Index[K, V]) walk(ctx context.Context, db kv.DB, write bool, visit func(tx kv.Tx, items []item) error) error {
	batch := x.batchSize()
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
			items     []item
		)
		read := func(tx kv.Tx) error {
			batchLast, batchSize, items = last, 0, nil

			var r kvutil.Range[K]
			if last != nil {
				r = r.After(*last)
			}
			records, err := x.store.Records(tx, r)
			if err != nil {
				return err
			}
			defer records.Close()

			for ; records.Valid() && batchSize < batch; records.Next() {
				batchSize++
				key, err := records.Key()
				if err != nil {
					return err
				}
				record, err := records.Value()
				if err != nil {
					return err
				}
				if it, ok, err := x.itemOf(key, record); err != nil {
					return err
				} else if ok {
					items = append(items, it)
				}
				batchLast = &key
			}
			if write {
				return visit(tx, items)
			}
			return nil
		}

		var err error
		if write {
			err = db.Update(read)
		} else {
			err = db.View(read)
			if err == nil {
				err = visit(nil, items)
			}
		}
		if err != nil {
			return fmt.Errorf("failed to build the index %q: %w", x.name, err)
		}
		last, more = batchLast, batchSize == batch
	}
	return nil
}

// itemOf returns the vector of a record as it is stored, if it has one.
func (x *Index[K, V]) itemOf(key K, record *V) (item, bool, error) {
	vec := x.get(record)
	if len(vec) == 0 {
		return item{}, false, nil
	}

	stored, err := x.prepare(vec)
	if err != nil {
		return item{}, false, fmt.Errorf("failed to index the record %v: %w", key, err)
	}
	rawKey, err := x.store.KeyCodec().Encode(key)
	if err != nil {
		return item{}, false, err
	}
	return item{rawKey: rawKey, stored: encode(stored)}, true, nil
}

// sample goes through the records, validates their vectors, counts them, and picks a sample of
// them to find the partitions from. The sample is picked the same way every time, so that a
// build of the same records gives the same partitions.
func (x *Index[K, V]) sample(ctx context.Context, db kv.DB) (count int, sample [][]byte, err error) {
	// A sample of a few times the size of a partition is enough to place the centroids, and it is
	// bounded by memory.
	limit := max(4*x.size, (128<<20)/(4*x.dims))
	rng := rand.New(rand.NewPCG(0x76656374, 0x6f72))

	err = x.walk(ctx, db, false, func(_ kv.Tx, items []item) error {
		for _, it := range items {
			count++
			// Reservoir sampling: every vector has the same chance to be in the sample.
			if len(sample) < limit {
				sample = append(sample, it.stored)
			} else if j := rng.IntN(count); j < limit {
				sample[j] = it.stored
			}
		}
		return nil
	})
	return count, sample, err
}

// writePartitions finds the partitions from a sample of count vectors and writes their
// centroids and the state, and returns them.
func (x *Index[K, V]) writePartitions(ctx context.Context, db kv.DB, count int, sample [][]byte) (*partitions, error) {
	// A cluster is split while the vectors that it is estimated to have are more than half of a
	// partition, so that the vectors that are written after the build have room to grow.
	var centroids [][]float32
	var split func(cluster [][]byte)
	split = func(cluster [][]byte) {
		estimate := len(cluster) * count / len(sample)
		if len(cluster) < 2 || estimate <= x.size/2 {
			centroids = append(centroids, centroidOf(x.metric, x.dims, cluster))
			return
		}
		side, _, _ := bisect(x.metric, x.dims, cluster)
		var halves [2][][]byte
		for i, v := range cluster {
			halves[side[i]] = append(halves[side[i]], v)
		}
		split(halves[0])
		split(halves[1])
	}
	split(sample)

	// The centroids are written in as many transactions as they need, and the state in the last,
	// so that there are partitions when it is there.
	var nonce [8]byte
	if _, err := crand.Read(nonce[:]); err != nil {
		return nil, err
	}
	per := max(1, (2<<20)/(4*x.dims))
	for from := 0; from < len(centroids); from += per {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		to := min(from+per, len(centroids))
		err := db.Update(func(tx kv.Tx) error {
			data := prefix.New(tx, x.prefix)
			for j := from; j < to; j++ {
				if err := data.Set(centroidKey(uint32(j+1)), encode(centroids[j])); err != nil {
					return err
				}
			}
			if to == len(centroids) {
				return data.Set([]byte{stateKey}, state(nonce, uint32(len(centroids)+1)))
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to write the partitions of the index %q: %w", x.name, err)
		}
	}

	parts := &partitions{nonce: nonce}
	for j, c := range centroids {
		parts.ids = append(parts.ids, uint32(j+1))
		parts.centroids = append(parts.centroids, encode(c)...)
	}
	return parts, nil
}

// assignAll returns the partition of each of the vectors. When there is enough to compute it is
// split between the processors; only this runs on other goroutines, never the transaction.
func (x *Index[K, V]) assignAll(parts *partitions, items []item) []uint32 {
	pids := make([]uint32, len(items))
	run := func(from, to int) {
		v := make([]float32, x.dims)
		for i := from; i < to; i++ {
			_ = decode(v, items[i].stored)
			pids[i] = parts.ids[assignment(x.metric, x.dims, parts.centroids, v)]
		}
	}

	workers := min(runtime.GOMAXPROCS(0), len(items))
	if workers < 2 || len(items)*len(parts.ids) < 4096 {
		run(0, len(items))
		return pids
	}

	var wg sync.WaitGroup
	chunk := (len(items) + workers - 1) / workers
	for from := 0; from < len(items); from += chunk {
		wg.Add(1)
		go func(from, to int) {
			defer wg.Done()
			run(from, to)
		}(from, min(from+chunk, len(items)))
	}
	wg.Wait()
	return pids
}
