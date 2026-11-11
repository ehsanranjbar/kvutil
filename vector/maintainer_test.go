package vector_test

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// newMaintainer returns a maintainer of the index of a store of docs.
func newMaintainer(db kv.DB, docs *extstore.Store[int64, doc], opts ...vector.MaintainerOption) *vector.Maintainer {
	return vector.NewMaintainer(db, []vector.Source{docs}, opts...)
}

// maintain runs a maintainer until there is nothing left to do.
func maintain(t *testing.T, m *vector.Maintainer) {
	t.Helper()
	require.NoError(t, m.Maintain(context.Background()))
}

// settled checks what holds once a maintainer is done: the keys of the index are right, there
// is no vector that is unplaced, and no partition has more vectors than its size.
func settled(t *testing.T, db kv.DB, f *fixture, size int) layout {
	t.Helper()

	got := f.check(t, db)
	require.Zero(t, got.Unplaced, "unplaced vectors")
	for pid, n := range got.Sizes {
		require.LessOrEqual(t, n, size, "the size of the partition %d", pid)
	}
	require.Equal(t, got.Partitions, stats(t, db, f.x).Partitions)
	return got
}

// counts returns the counts that the index has of its partitions.
func counts(t *testing.T, db kv.DB, x *vector.Index[int64, doc]) map[uint32]uint32 {
	t.Helper()

	out := map[uint32]uint32{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		p := append(slices.Clone(x.Prefix()), 'n')
		it, err := tx.Iterator(p, kv.PrefixEnd(p))
		require.NoError(t, err)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			require.NoError(t, err)
			out[binary.BigEndian.Uint32(it.Key()[len(p):])] = binary.BigEndian.Uint32(value)
		}
		return nil
	}))
	return out
}

// dump returns every key under the prefix of an index and its value.
func dump(t *testing.T, db kv.DB, prefix []byte) map[string]string {
	t.Helper()

	out := map[string]string{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(prefix, kv.PrefixEnd(prefix))
		require.NoError(t, err)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			require.NoError(t, err)
			out[string(it.Key())] = string(value)
		}
		return nil
	}))
	return out
}

// requireRecall checks that searches of the index find what exact ones do, as far as is
// asked for, and that they find it all when they read every partition.
func requireRecall(t *testing.T, db kv.DB, f *fixture, queries [][]float32, atLeast float64) {
	t.Helper()

	partitions := stats(t, db, f.x).Partitions
	require.Positive(t, partitions)
	var recall float64
	require.NoError(t, db.View(func(tx kv.Tx) error {
		ins := f.x.Instantiate(tx)
		for _, q := range queries {
			exact := run(t, ins.Search(q).Limit(10).Exact())
			recall += overlap(exact.keys, run(t, ins.Search(q).Limit(10)).keys)
			require.Equal(t, exact.keys, run(t, ins.Search(q).Limit(10).Probes(partitions)).keys)
		}
		return nil
	}))
	t.Logf("%d partitions, recall@10 %.3f", partitions, recall/float64(len(queries)))
	require.GreaterOrEqual(t, recall/float64(len(queries)), atLeast, "recall@10")
}

// queries draws vectors around the centres.
func (c *clusters) queries(rng *rand.Rand, n int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		out[i] = c.draw(rng)
	}
	return out
}

// add writes docs around the centres, with the keys from a number on.
func (f *fixture) add(t *testing.T, db kv.DB, cl *clusters, rng *rand.Rand, from, n int) {
	t.Helper()

	added := map[int64]doc{}
	for id := int64(from); id < int64(from+n); id++ {
		added[id] = doc{Lang: "en", Embedding: cl.draw(rng)}
		f.byID[id] = added[id]
	}
	put(t, db, f.docs, added)
}

func TestMaintainFromNothing(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 3000, vector.Partitioned(100))
		vectors := f.withVector()
		require.Equal(t, vector.Stats{Unplaced: vectors}, stats(t, db, f.x))

		maintain(t, newMaintainer(db, f.docs))

		got := settled(t, db, f, 100)
		require.Equal(t, vectors, got.Placed)
		require.Greater(t, got.Partitions, 10)
		requireRecall(t, db, f, cl.queries(rng, 40), 0.9)

		// Every count is right, as far as an upper bound can be, right after the pass.
		for pid, n := range counts(t, db, f.x) {
			require.Equal(t, got.Sizes[pid], int(n), "the count of the partition %d", pid)
		}
	})
}

func TestMaintainLeavesFewVectorsUnplaced(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, _ := newClusteredFixture(t, db, vector.Cosine, 90, vector.Partitioned(100))
		maintain(t, newMaintainer(db, f.docs))

		vectors := f.withVector()
		require.Equal(t, vector.Stats{Unplaced: vectors}, stats(t, db, f.x))
		f.check(t, db)

		// And makes them once there are more.
		f.add(t, db, newClusters(rand.New(rand.NewPCG(1, 1)), clusterDims, 5, 0.3), rand.New(rand.NewPCG(2, 2)), 10_000, 60)
		maintain(t, newMaintainer(db, f.docs))
		require.Positive(t, stats(t, db, f.x).Partitions)
		settled(t, db, f, 100)
	})
}

func TestMaintainAfterARebuild(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 2000, vector.Partitioned(100))
		f.rebuild(t, db)
		built := f.check(t, db)
		for pid, n := range counts(t, db, f.x) {
			require.Equal(t, built.Sizes[pid], int(n), "the count of the partition %d after the build", pid)
		}

		// As many records again are written, and are unplaced until the maintainer places them.
		f.add(t, db, cl, rng, 10_000, 2000)
		require.Equal(t, 2000, stats(t, db, f.x).Unplaced)
		maintain(t, newMaintainer(db, f.docs))

		got := settled(t, db, f, 100)
		require.Greater(t, got.Placed, built.Placed)
		queries := cl.queries(rng, 40)
		for id := int64(10_000); id < 10_020; id++ {
			queries = append(queries, f.byID[id].Embedding)
		}
		requireRecall(t, db, f, queries, 0.9)
	})
}

func TestMaintainUpdatesAndDeletes(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 2000, vector.Partitioned(100))
		m := newMaintainer(db, f.docs)
		maintain(t, m)
		before := settled(t, db, f, 100).Partitions

		// Placed records get other vectors, or none, or go.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := f.docs.Instantiate(tx)
			for id := int64(1); id <= 900; id++ {
				d := f.byID[id]
				switch {
				case id%3 == 0:
					if err := store.Delete(id); err != nil {
						return err
					}
					delete(f.byID, id)
					continue
				case id%3 == 1:
					d.Embedding = cl.draw(rng)
				default:
					d.Embedding = nil
				}
				f.byID[id] = d
				if err := store.Set(id, &d); err != nil {
					return err
				}
			}
			return nil
		}))
		maintain(t, m)
		settled(t, db, f, 100)
		requireRecall(t, db, f, cl.queries(rng, 40), 0.9)

		// Nine of ten records go, which leaves partitions with few vectors, and they are dissolved.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := f.docs.Instantiate(tx)
			for id := range f.byID {
				if id%10 != 0 {
					if err := store.Delete(id); err != nil {
						return err
					}
					delete(f.byID, id)
				}
			}
			return nil
		}))
		maintain(t, m)
		got := settled(t, db, f, 100)
		require.Less(t, got.Partitions, before)
		if got.Partitions > 1 {
			for pid, n := range got.Sizes {
				require.GreaterOrEqual(t, n, 100/8, "the partition %d is small", pid)
			}
		}
		requireRecall(t, db, f, cl.queries(rng, 40), 0.8)
	})
}

func TestMaintainEqualVectors(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, _, _ := newClusteredFixture(t, db, metric, 0, vector.Partitioned(100))
		same := make([]float32, clusterDims)
		for i := range same {
			same[i] = float32(i + 1)
		}
		for id := int64(1); id <= 500; id++ {
			f.byID[id] = doc{Embedding: slices.Clone(same)}
		}
		put(t, db, f.docs, f.byID)

		maintain(t, newMaintainer(db, f.docs))
		got := settled(t, db, f, 100)
		require.GreaterOrEqual(t, got.Partitions, 5)

		// Every search finds them all, whichever partitions it reads.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res := run(t, f.x.Instantiate(tx).Search(same).Probes(got.Partitions))
			require.Len(t, res.keys, 500)
			return nil
		}))
	})
}

func TestMaintainSettlesWithOutliers(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 3000, vector.Partitioned(100))

		// A few vectors that are far from all the others, in directions of their own.
		for id := int64(20_000); id < 20_008; id++ {
			v := make([]float32, clusterDims)
			for i := range v {
				v[i] = float32(rng.NormFloat64() * 1000)
			}
			f.byID[id] = doc{Embedding: v}
		}
		put(t, db, f.docs, f.byID)

		m := newMaintainer(db, f.docs)
		maintain(t, m)
		settled(t, db, f, 100)

		// A second time there is nothing to do, so nothing is written.
		before := dump(t, db, f.x.Prefix())
		maintain(t, m)
		require.Equal(t, before, dump(t, db, f.x.Prefix()))

		// The outliers are found by the searches that are made for them. For the inner product they
		// are the nearest to every query, and the partitions that have them are not the ones that a
		// search of few partitions reads, so only a search of all of them is asked to find them.
		recall := 0.9
		if metric == vector.Dot {
			recall = 0
		}
		requireRecall(t, db, f, append(cl.queries(rng, 20), f.byID[20_000].Embedding, f.byID[20_003].Embedding), recall)
	})
}

func TestMaintainFixesACountThatIsTooLow(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, rng := newClusteredFixture(t, db, vector.Cosine, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		built := f.check(t, db)

		// The count of a partition is wrong, and far below what it has. A count is an upper bound,
		// so this is a mistake that nothing but counting finds.
		var (
			id   int64
			pid  uint32
			have int
		)
		for id = 1; ; id++ {
			rawKey, err := f.docs.KeyCodec().Encode(id)
			require.NoError(t, err)
			err = db.View(func(tx kv.Tx) error {
				b, err := tx.Get(append(append(slices.Clone(f.x.Prefix()), 'k'), rawKey...))
				if err == nil {
					pid = binary.BigEndian.Uint32(b)
				}
				return err
			})
			if err == nil && built.Sizes[pid] >= 20 {
				have = built.Sizes[pid]
				break
			}
		}
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			key := binary.BigEndian.AppendUint32(append(slices.Clone(f.x.Prefix()), 'n'), pid)
			return tx.Set(key, binary.BigEndian.AppendUint32(nil, 0))
		}))

		// Vectors like the one of a record of the partition are written, so many that the partition
		// has more than its size with them, and fewer than its size by its count.
		near := f.byID[id].Embedding
		added := map[int64]doc{}
		for n := int64(30_000); n < 30_000+int64(100-have+5); n++ {
			v := slices.Clone(near)
			for i := range v {
				v[i] += float32(rng.NormFloat64() * 0.001)
			}
			added[n] = doc{Embedding: v}
			f.byID[n] = added[n]
		}
		require.Less(t, len(added), 100)
		require.Greater(t, have+len(added), 100)
		put(t, db, f.docs, added)

		maintain(t, newMaintainer(db, f.docs))
		got := settled(t, db, f, 100)
		require.NotContains(t, got.Sizes, pid, "the partition was split")
	})
}

func TestMaintainLeavesPartitionlessIndexesAlone(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		docs := extstore.New[int64, doc]("docs")
		partitioned := vector.On(docs, "by_partitioned", clusterDims, vector.Cosine, embedding, vector.Partitioned(100))
		plain := vector.On(docs, "by_plain", clusterDims, vector.Cosine, embedding)
		require.NoError(t, docs.Check(db))

		rng := rand.New(rand.NewPCG(3, 4))
		cl := newClusters(rng, clusterDims, 20, 0.3)
		byID := map[int64]doc{}
		for id := int64(1); id <= 1000; id++ {
			byID[id] = doc{Embedding: cl.draw(rng)}
		}
		put(t, db, docs, byID)

		before := dump(t, db, plain.Prefix())
		maintain(t, newMaintainer(db, docs))
		require.Equal(t, before, dump(t, db, plain.Prefix()))
		checkLayout(t, db, plain, docs)

		// The other index of the same store is maintained.
		require.Positive(t, stats(t, db, partitioned).Partitions)
		require.Zero(t, stats(t, db, partitioned).Unplaced)
		checkLayout(t, db, partitioned, docs)

		// A store with an index that is not partitioned only.
		only := extstore.New[int64, doc]("only")
		x := vector.On(only, "by_embedding", clusterDims, vector.Cosine, embedding)
		require.NoError(t, only.Check(db))
		put(t, db, only, byID)
		before = dump(t, db, x.Prefix())
		maintain(t, newMaintainer(db, only))
		require.Equal(t, before, dump(t, db, x.Prefix()))

		// Nor does it matter if there is no source.
		require.NoError(t, vector.NewMaintainer(db, nil).Maintain(context.Background()))
		require.NoError(t, vector.NewMaintainer(db, []vector.Source{nil}).Maintain(context.Background()))
	})
}

// unplacedNow returns the number of unplaced vectors, for a function that polls.
func unplacedNow(db kv.DB, x *vector.Index[int64, doc]) (stats vector.Stats, err error) {
	err = db.View(func(tx kv.Tx) (err error) {
		stats, err = x.Instantiate(tx).Stats()
		return err
	})
	return stats, err
}

func TestMaintainerSpawn(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, cl, rng := newClusteredFixture(t, db, vector.Cosine, 500, vector.Partitioned(100))

		var (
			mu   sync.Mutex
			errs []error
		)
		m := newMaintainer(db, f.docs,
			vector.WithInterval(5*time.Millisecond),
			vector.WithErrorHandler(func(err error) {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}))
		ctx := context.Background()
		unplaced := func(want int) func() bool {
			return func() bool {
				s, err := unplacedNow(db, f.x)
				return err == nil && s.Unplaced == want
			}
		}

		// Shutting down a maintainer that was never spawned does nothing.
		require.NoError(t, m.Shutdown(ctx))

		// A maintainer that is spawned places what is there, and spawning it again leaves it as it is.
		m.Spawn(ctx)
		m.Spawn(ctx)
		require.Eventually(t, unplaced(0), 5*time.Second, 5*time.Millisecond)
		require.Positive(t, stats(t, db, f.x).Partitions)

		// And what is written while it runs.
		f.add(t, db, cl, rng, 10_000, 200)
		require.Eventually(t, unplaced(0), 5*time.Second, 5*time.Millisecond)

		// Once it is shut down it does nothing more, however often it is shut down.
		require.NoError(t, m.Shutdown(ctx))
		require.NoError(t, m.Shutdown(ctx))
		f.add(t, db, cl, rng, 20_000, 5)
		time.Sleep(60 * time.Millisecond)
		require.Equal(t, 5, stats(t, db, f.x).Unplaced)

		// It can be spawned again, and it stops with its context too.
		parent, cancel := context.WithCancel(ctx)
		m.Spawn(parent)
		require.Eventually(t, unplaced(0), 5*time.Second, 5*time.Millisecond)
		cancel()
		time.Sleep(20 * time.Millisecond)
		f.add(t, db, cl, rng, 30_000, 5)
		time.Sleep(60 * time.Millisecond)
		require.Equal(t, 5, stats(t, db, f.x).Unplaced)
		require.NoError(t, m.Shutdown(ctx))

		m.Spawn(ctx)
		require.Eventually(t, unplaced(0), 5*time.Second, 5*time.Millisecond)
		require.NoError(t, m.Shutdown(ctx))

		settled(t, db, f, 100)
		mu.Lock()
		defer mu.Unlock()
		require.Empty(t, errs)
	})
}

func TestMaintainerReportsErrors(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, cl, rng := newClusteredFixture(t, db, vector.Cosine, 500, vector.Partitioned(100))
		maintain(t, newMaintainer(db, f.docs))

		// An entry of the unplaced area that is not a vector, which no partition can be chosen for.
		rawKey, err := f.docs.KeyCodec().Encode(999_999)
		require.NoError(t, err)
		bad := vectorEntryKey(f.x.Prefix(), 0, rawKey)
		require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Set(bad, []byte{1, 2, 3}) }))
		f.add(t, db, cl, rng, 10_000, 10)

		var (
			mu   sync.Mutex
			errs []error
		)
		m := newMaintainer(db, f.docs,
			vector.WithInterval(2*time.Millisecond),
			vector.WithErrorHandler(func(err error) {
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, err)
			}))
		m.Spawn(context.Background())
		defer m.Shutdown(context.Background())

		// It is told of the error again and again, as the maintainer tries every round.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(errs) >= 3
		}, 5*time.Second, 2*time.Millisecond)
		mu.Lock()
		require.ErrorIs(t, errs[0], vector.ErrCorrupt)
		mu.Unlock()

		// And goes on when the entry is gone.
		require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Delete(bad) }))
		require.Eventually(t, func() bool {
			s, err := unplacedNow(db, f.x)
			return err == nil && s.Unplaced == 0
		}, 5*time.Second, 2*time.Millisecond)
		require.NoError(t, m.Shutdown(context.Background()))
		settled(t, db, f, 100)

		// Maintain returns the error too.
		require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Set(bad, []byte{1, 2, 3}) }))
		require.ErrorIs(t, newMaintainer(db, f.docs).Maintain(context.Background()), vector.ErrCorrupt)
	})
}

func TestMaintainStopsWithItsContext(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, _ := newClusteredFixture(t, db, vector.Cosine, 500, vector.Partitioned(100))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, newMaintainer(db, f.docs).Maintain(ctx), context.Canceled)
		require.ErrorIs(t, newMaintainer(db, f.docs).Run(ctx), context.Canceled)
	})
}

func TestMaintainerConcurrently(t *testing.T) {
	t.Run("vectors", func(t *testing.T) { maintainerConcurrently(t) })
	// The codes are written and deleted with the vectors, by the writers and the maintainer.
	t.Run("codes", func(t *testing.T) { maintainerConcurrently(t, vector.Quantized()) })
}

// maintainerConcurrently has a maintainer keep an index, with the options that are given besides
// the partitions, while writers and searches go on.
func maintainerConcurrently(t *testing.T, extra ...vector.Option) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		const (
			ids     = 600
			workers = 4
			ops     = 300
			size    = 50
		)
		docs := extstore.New[int64, doc]("docs")
		x := vector.On(docs, "by_embedding", 8, metric, embedding,
			append([]vector.Option{vector.Partitioned(size)}, extra...)...)
		require.NoError(t, docs.Check(db))

		seed := rand.New(rand.NewPCG(9, 9))
		cl := newClusters(seed, 8, 12, 0.3)

		var (
			problems atomic.Pointer[error]
			mu       sync.Mutex
		)
		problem := func(err error) {
			problems.CompareAndSwap(nil, &err)
		}

		m := newMaintainer(db, docs,
			vector.WithInterval(time.Millisecond),
			vector.WithErrorHandler(func(err error) {
				// Two transactions of a busy database can fail to get along, which the engine says by
				// a conflict. Anything else is a mistake.
				if !errors.Is(err, kv.ErrConflict) {
					problem(err)
				}
			}))
		m.Spawn(context.Background())

		// The records as the writers leave them. A writer takes the lock of an id while it writes
		// it, so that the order they are written in is the order they are put in the model.
		var (
			locks [ids + 1]sync.Mutex
			model = map[int64]doc{}
			wg    sync.WaitGroup
			done  atomic.Bool
		)
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(w)+100, 7))
				for range ops {
					id := int64(1 + rng.IntN(ids))
					var (
						d      doc
						remove bool
					)
					switch r := rng.IntN(100); {
					case r < 25:
						remove = true
					case r < 40:
						d = doc{Title: "empty"}
					default:
						d = doc{Title: "vector", Embedding: cl.draw(rng)}
					}

					locks[id].Lock()
					var err error
					for {
						err = db.Update(func(tx kv.Tx) error {
							if remove {
								return docs.Instantiate(tx).Delete(id)
							}
							return docs.Instantiate(tx).Set(id, &d)
						})
						if !errors.Is(err, kv.ErrConflict) {
							break
						}
					}
					if err == nil {
						mu.Lock()
						if remove {
							delete(model, id)
						} else {
							model[id] = d
						}
						mu.Unlock()
					} else {
						problem(err)
					}
					locks[id].Unlock()
				}
			}()
		}

		// Searches, in the middle of it, never fail.
		var searched sync.WaitGroup
		searched.Add(1)
		go func() {
			defer searched.Done()
			rng := rand.New(rand.NewPCG(5, 5))
			for !done.Load() {
				err := db.View(func(tx kv.Tx) error {
					res, err := x.Instantiate(tx).Nearest(cl.draw(rng), 5)
					if err != nil {
						return err
					}
					return res.Close()
				})
				if err != nil {
					problem(err)
				}
			}
		}()

		wg.Wait()
		done.Store(true)
		searched.Wait()
		require.NoError(t, m.Shutdown(context.Background()))
		if p := problems.Load(); p != nil {
			require.NoError(t, *p)
		}

		// Whatever the maintainer left undone is done now.
		maintain(t, m)

		// The store has the records that the writers put in it.
		byID := map[int64]doc{}
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it, err := docs.Instantiate(tx).All()
			require.NoError(t, err)
			defer it.Close()
			for ; it.Valid(); it.Next() {
				key, err := it.Key()
				require.NoError(t, err)
				d, err := it.Value()
				require.NoError(t, err)
				byID[key] = *d
			}
			return nil
		}))
		require.Equal(t, len(model), len(byID))
		for id, d := range model {
			require.Equal(t, d.Embedding, byID[id].Embedding, "the record %d", id)
		}

		// And the index has them all in its partitions, which hold what they should.
		f := &fixture{metric: metric, docs: docs, x: x, byID: byID, query: cl.draw(seed)}
		got := settled(t, db, f, size)
		t.Logf("%d records, %d with a vector, %d partitions", len(byID), f.withVector(), got.Partitions)
		require.Positive(t, got.Partitions)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := x.Instantiate(tx)
			for range 5 {
				f.query = cl.draw(seed)
				f.verify(t, "exact", ins.Search(f.query).Exact(), keepAll, 10, math.NaN())
				f.verify(t, "exact, no limit", ins.Search(f.query).Exact(), keepAll, -1, math.NaN())

				// Reading every partition and re-ranking every candidate is exact too, with codes or not.
				every := ins.Search(f.query).Probes(got.Partitions).Rescore(1 << 30)
				f.verify(t, "every partition", every, keepAll, 10, math.NaN())
			}
			return nil
		}))
	})
}
