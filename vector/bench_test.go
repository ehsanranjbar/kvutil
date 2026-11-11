package vector_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// benchDims is the number of dimensions of the vectors of the benchmarks, which is what a small
// embedding model makes.
const benchDims = 384

// benchLoad is the number of docs that are written in a transaction when the benchmarks load.
const benchLoad = 500

// benchQueries is the number of queries that the benchmarks take turns with.
const benchQueries = 50

// benchSizes are the numbers of vectors that the benchmarks search. The search of 100000 vectors
// is only run if VECTOR_BENCH_LARGE is 1, as loading them takes a while.
func benchSizes() []int {
	sizes := []int{10_000}
	if os.Getenv("VECTOR_BENCH_LARGE") == "1" {
		sizes = append(sizes, 100_000)
	}
	return sizes
}

// benchEngines are the engines that the benchmarks run on.
var benchEngines = []struct {
	name string
	open func(testing.TB) kv.DB
}{
	{"memkv", testkv.NewMem},
	{"badger", testkv.NewBadger},
}

// BenchmarkNearest measures finding the 10 nearest vectors of a query, which compares the
// query with every vector that is stored. The vectors are around centres, like the embeddings of
// texts about some subjects are, and so are the queries.
func BenchmarkNearest(b *testing.B) {
	for _, engine := range benchEngines {
		for _, n := range benchSizes() {
			b.Run(fmt.Sprintf("%s/%d", engine.name, n), func(b *testing.B) {
				db := engine.open(b)
				x, queries := benchLoaded(b, db, n)
				benchSearch(b, db, x, queries)
			})
		}
	}
}

// BenchmarkNearestPartitioned measures the same search on an index that is partitioned, which
// reads the nearest partitions only. Before the time is measured it finds how many of the 10
// nearest vectors the search finds, against an exact search of the same index.
func BenchmarkNearestPartitioned(b *testing.B) {
	for _, engine := range benchEngines {
		for _, n := range benchSizes() {
			b.Run(fmt.Sprintf("%s/%d", engine.name, n), func(b *testing.B) {
				db := engine.open(b)
				x, queries := benchLoaded(b, db, n, vector.Partitioned(0))
				require.NoError(b, vector.Rebuild(context.Background(), db, x))

				var recall float64
				require.NoError(b, db.View(func(tx kv.Tx) error {
					ins := x.Instantiate(tx)
					for _, q := range queries {
						exact, err := ins.Search(q).Limit(10).Exact().Run()
						if err != nil {
							return err
						}
						approx, err := ins.Search(q).Limit(10).Run()
						if err != nil {
							return err
						}
						found := 0
						for ; exact.Valid(); exact.Next() {
							rawKey, err := exact.RawKey()
							if err != nil {
								return err
							}
							if ok, err := approx.Contains(rawKey, nil); err != nil {
								return err
							} else if ok {
								found++
							}
						}
						recall += float64(found) / float64(exact.Len())
					}
					return nil
				}))

				benchSearch(b, db, x, queries)
				// After the search, as resetting the timer clears the metrics.
				b.ReportMetric(recall/float64(len(queries)), "recall@10")
			})
		}
	}
}

// benchLoaded writes n docs with a vector of benchDims components around sqrt(n)/2 centres, and
// returns the index and queries that are around the same centres.
func benchLoaded(b *testing.B, db kv.DB, n int, opts ...vector.Option) (*vector.Index[int64, doc], [][]float32) {
	b.Helper()

	docs := extstore.New[int64, doc]("docs")
	x := vector.On(docs, "by_embedding", benchDims, vector.Cosine, embedding, opts...)
	require.NoError(b, docs.Check(db))

	rng := rand.New(rand.NewPCG(1, 2))
	cl := newClusters(rng, benchDims, max(2, int(math.Sqrt(float64(n))/2)), 0.3)
	for start := 1; start <= n; start += benchLoad {
		require.NoError(b, db.Update(func(tx kv.Tx) error {
			store := docs.Instantiate(tx)
			for id := start; id < start+benchLoad && id <= n; id++ {
				if err := store.Set(int64(id), &doc{Embedding: cl.draw(rng)}); err != nil {
					return err
				}
			}
			return nil
		}))
	}

	queries := make([][]float32, benchQueries)
	for i := range queries {
		queries[i] = cl.draw(rng)
	}
	return x, queries
}

// benchSearch measures searches for the 10 nearest vectors, and reports the number of vectors
// that a search compares the query with.
func benchSearch(b *testing.B, db kv.DB, x *vector.Index[int64, doc], queries [][]float32) {
	b.Helper()

	var scanned int
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		err := db.View(func(tx kv.Tx) error {
			res, err := x.Instantiate(tx).Nearest(queries[i%len(queries)], 10)
			if err != nil {
				return err
			}
			defer res.Close()
			if res.Len() != 10 {
				return fmt.Errorf("found %d vectors", res.Len())
			}
			scanned += res.Scanned()
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(scanned)/float64(b.N), "scanned/op")
}
