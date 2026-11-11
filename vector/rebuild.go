package vector

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
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
		}, nil)
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

	// How many vectors each partition has is known once the batches are written, and is only
	// kept once a batch is committed, as the function of a transaction may run more than once.
	var (
		counts  = map[uint32]uint32{}
		pending = map[uint32]uint32{}
	)
	err = x.walk(ctx, db, true, func(tx kv.Tx, items []item) error {
		data := prefix.New(tx, x.prefix)
		var (
			pids  []uint32
			codes [][]byte
		)
		if parts != nil {
			pids, codes = x.assignAll(parts, items)
		}
		clear(pending)
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
			if codes != nil {
				if err := data.Set(codeKey(pids[i], it.rawKey), codes[i]); err != nil {
					return err
				}
			}
			pending[pids[i]]++
		}
		return nil
	}, func() {
		for pid, n := range pending {
			counts[pid] += n
		}
		clear(pending)
	})
	if err != nil || parts == nil {
		return err
	}
	return x.writeCounts(ctx, db, counts)
}

// writeCounts writes the numbers of vectors of the partitions, in as many transactions as it takes.
func (x *Index[K, V]) writeCounts(ctx context.Context, db kv.DB, counts map[uint32]uint32) error {
	pids := slices.Sorted(maps.Keys(counts))
	const perTransaction = 4096
	for from := 0; from < len(pids); from += perTransaction {
		if err := ctx.Err(); err != nil {
			return err
		}
		to := min(from+perTransaction, len(pids))
		err := db.Update(func(tx kv.Tx) error {
			data := prefix.New(tx, x.prefix)
			for _, pid := range pids[from:to] {
				if err := data.Set(countKey(pid), binary.BigEndian.AppendUint32(nil, counts[pid])); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to write the counts of the index %q: %w", x.name, err)
		}
	}
	return nil
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
// not, when it is sure to run once. committed, if it is not nil, is called after the transaction
// of a batch that was written is committed, which is when what visit found is to be kept.
func (x *Index[K, V]) walk(
	ctx context.Context,
	db kv.DB,
	write bool,
	visit func(tx kv.Tx, items []item) error,
	committed func(),
) error {
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
		if write && committed != nil {
			committed()
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
	s := x.newSampler()
	err = x.walk(ctx, db, false, func(_ kv.Tx, items []item) error {
		for _, it := range items {
			s.add(it.stored)
		}
		return nil
	}, nil)
	return s.count, s.sample, err
}

// sampler picks a sample of the vectors that it is given, each as likely as another to be in it.
type sampler struct {
	limit  int
	rng    *rand.Rand
	count  int
	sample [][]byte
}

// newSampler returns a sampler that picks the same vectors every time it is given the same ones.
func (x *Index[K, V]) newSampler() *sampler {
	return &sampler{
		// A sample of a few times the size of a partition is enough to place the centroids, and it
		// is bounded by memory.
		limit: max(4*x.size, (128<<20)/(4*x.dims)),
		rng:   rand.New(rand.NewPCG(0x76656374, 0x6f72)),
	}
}

// add counts a vector, and keeps it if it is in the sample.
func (s *sampler) add(stored []byte) {
	s.count++
	// Reservoir sampling: every vector has the same chance to be in the sample.
	if len(s.sample) < s.limit {
		s.sample = append(s.sample, stored)
	} else if j := s.rng.IntN(s.count); j < s.limit {
		s.sample[j] = stored
	}
}

// writePartitions finds the partitions from a sample of count vectors and writes their
// centroids and the state, and returns them. Each partition is written with a count of 0, which
// is right as it has no vector yet.
//
// It fails if the index has partitions already, and clears the centroids and counts that it
// finds without them, which are what a build that did not finish left.
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
			if from == 0 {
				if err := x.clearUnfinished(data); err != nil {
					return err
				}
			}
			for j := from; j < to; j++ {
				pid := uint32(j + 1)
				if err := data.Set(centroidKey(pid), encode(centroids[j])); err != nil {
					return err
				}
				if err := data.Set(countKey(pid), binary.BigEndian.AppendUint32(nil, 0)); err != nil {
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

	parts := &partitions{nonce: nonce, next: uint32(len(centroids) + 1)}
	for j, c := range centroids {
		parts.ids = append(parts.ids, uint32(j+1))
		parts.centroids = append(parts.centroids, encode(c)...)
		if x.rot != nil {
			parts.rotated = x.appendRotated(parts.rotated, encode(c))
		}
	}
	return parts, nil
}

// clearUnfinished fails if the index has partitions, and otherwise deletes the centroids, the
// counts and the codes that there are, which a build that did not get to write the state left
// behind.
func (x *Index[K, V]) clearUnfinished(data *prefix.Tx) error {
	if _, err := data.Get([]byte{stateKey}); err == nil {
		return fmt.Errorf("vector: the index %q has partitions already", x.name)
	} else if !errors.Is(err, kv.ErrNotFound) {
		return err
	}

	for _, key := range []byte{centroidsKey, countsKey, codesKey} {
		it, err := data.Iterator([]byte{key}, []byte{key + 1})
		if err != nil {
			return err
		}
		var keys [][]byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, bytes.Clone(it.Key()))
		}
		if err := it.Close(); err != nil {
			return err
		}
		for _, k := range keys {
			if err := data.Delete(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// assignAll returns the partition of each of the vectors, and their codes in those partitions if
// the index is quantized (nil if it is not). When there is enough to compute it is split between
// the processors; only this runs on other goroutines, never the transaction.
func (x *Index[K, V]) assignAll(parts *partitions, items []item) (pids []uint32, codes [][]byte) {
	pids = make([]uint32, len(items))
	if x.rot != nil {
		codes = make([][]byte, len(items))
	}
	parallel(len(items), len(items)*(len(parts.ids)+x.codeCost()), func(from, to int) {
		v := make([]float32, x.dims)
		var c *coder
		if x.rot != nil {
			c = x.rot.newCoder()
		}
		for i := from; i < to; i++ {
			_ = decode(v, items[i].stored)
			j := assignment(x.metric, x.dims, parts.centroids, v)
			pids[i] = parts.ids[j]
			if c != nil {
				codes[i] = c.encode(items[i].stored, parts.centroid(j, x.dims))
			}
		}
	})
	return pids, codes
}

// parallel calls run for the n items in as many ranges as there are processors to work on, or
// in one if the work, which is a rough measure of the comparisons it takes, is too little to be
// worth the goroutines. Each range is run by one goroutine, and parallel returns once all are done.
func parallel(n, work int, run func(from, to int)) {
	workers := min(runtime.GOMAXPROCS(0), n)
	if workers < 2 || work < 4096 {
		run(0, n)
		return
	}

	var wg sync.WaitGroup
	chunk := (n + workers - 1) / workers
	for from := 0; from < n; from += chunk {
		wg.Add(1)
		go func(from, to int) {
			defer wg.Done()
			run(from, to)
		}(from, min(from+chunk, n))
	}
	wg.Wait()
}
