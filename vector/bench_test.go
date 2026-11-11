package vector_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"testing"
	"time"

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
				_, x, queries := benchLoaded(b, db, n, nil)
				benchSearch(b, db, x, queries, 0)
			})
		}
	}
}

// benchData is a kind of vectors that the benchmarks search.
type benchData struct {
	name string
	// scale, if it is not nil, is what component j of every centre and every noise draw is
	// multiplied by.
	scale func(j int) float64
}

// benchDatasets are the vectors that the benchmarks of the partitioned indexes search: vectors
// around centres, and the same with a few dimensions that carry most of the variance, as the
// embeddings of a model do.
var benchDatasets = []benchData{
	{"clustered", nil},
	{"uneven", func(j int) float64 { return 3*math.Exp(-float64(j)/60) + 0.05 }},
}

// BenchmarkNearestPartitioned measures the same search on an index that is partitioned, which
// reads the nearest partitions only. Before the time is measured it finds how many of the 10
// nearest vectors the search finds, against an exact search of the same index.
func BenchmarkNearestPartitioned(b *testing.B) {
	benchPartitioned(b, []benchLevel{{}}, vector.Partitioned(0))
}

// BenchmarkNearestQuantized measures the same search on an index that is partitioned and
// quantized, which compares the query with the codes of the vectors of the nearest partitions and
// reads the vectors of the best of them only. It reports the number of vectors that it reads to
// re-rank besides. The search re-ranks the default number of candidates (100 for the 10 nearest),
// and in another level 50, to show what fewer cost and lose.
func BenchmarkNearestQuantized(b *testing.B) {
	levels := []benchLevel{{"default", 0}, {"rescore50", 50}}
	benchPartitioned(b, levels, vector.Partitioned(0), vector.Quantized())
}

// benchLevel is how many candidates a search of a quantized index re-ranks, in a benchmark of its
// own named after it. 0 is the default, and the zero value is a benchmark with no level.
type benchLevel struct {
	name    string
	rescore int
}

// benchSearchOf describes the search for the 10 nearest vectors that the benchmarks make.
func benchSearchOf(ins *vector.Instance[int64, doc], q []float32, rescore int) vector.Search[int64, doc] {
	s := ins.Search(q).Limit(10)
	if rescore > 0 {
		s = s.Rescore(rescore)
	}
	return s
}

// benchPartitioned runs the benchmark of searching an index with the options on every engine,
// kind of vectors and number of them, and for every level.
func benchPartitioned(b *testing.B, levels []benchLevel, opts ...vector.Option) {
	for _, engine := range benchEngines {
		for _, data := range benchDatasets {
			for _, n := range benchSizes() {
				b.Run(fmt.Sprintf("%s/%s/%d", engine.name, data.name, n), func(b *testing.B) {
					db := engine.open(b)
					_, x, queries := benchLoaded(b, db, n, data.scale, opts...)
					require.NoError(b, vector.Rebuild(context.Background(), db, x))

					measure := func(b *testing.B, rescore int) {
						recall := benchRecall(b, db, x, queries, rescore)
						benchSearch(b, db, x, queries, rescore)
						// After the search, as resetting the timer clears the metrics.
						b.ReportMetric(recall, "recall@10")
					}
					for _, level := range levels {
						if level.name == "" {
							measure(b, level.rescore)
							continue
						}
						b.Run(level.name, func(b *testing.B) { measure(b, level.rescore) })
					}
				})
			}
		}
	}
}

// BenchmarkMaintain measures a maintainer making the partitions of an index and placing its
// vectors, which were written without any. Setting up is not measured. The recall of the last
// index is what BenchmarkNearestPartitioned reports for an index that was built.
func BenchmarkMaintain(b *testing.B) {
	for _, n := range benchSizes() {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			var (
				db      kv.DB
				x       *vector.Index[int64, doc]
				queries [][]float32
			)
			var elapsed time.Duration
			for range b.N {
				b.StopTimer()
				db = testkv.NewMem(b)
				var docs *extstore.Store[int64, doc]
				docs, x, queries = benchLoaded(b, db, n, nil, vector.Partitioned(0))
				m := vector.NewMaintainer(db, []vector.Source{docs})

				b.StartTimer()
				start := time.Now()
				require.NoError(b, m.Maintain(context.Background()))
				elapsed += time.Since(start)
			}
			b.StopTimer()

			b.ReportMetric(float64(n)*float64(b.N)/elapsed.Seconds(), "vectors/s")
			b.ReportMetric(benchRecall(b, db, x, queries, 0), "recall@10")
		})
	}
}

// benchRecall returns the fraction of the 10 nearest vectors of the queries that a search finds,
// with the partitions that it reads by default, against an exact search of the same index.
func benchRecall(
	b *testing.B, db kv.DB, x *vector.Index[int64, doc], queries [][]float32, rescore int,
) float64 {
	b.Helper()

	var recall float64
	require.NoError(b, db.View(func(tx kv.Tx) error {
		ins := x.Instantiate(tx)
		for _, q := range queries {
			exact, err := ins.Search(q).Limit(10).Exact().Run()
			if err != nil {
				return err
			}
			approx, err := benchSearchOf(ins, q, rescore).Run()
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
	return recall / float64(len(queries))
}

// benchLoaded writes n docs with a vector of benchDims components around sqrt(n)/2 centres, and
// returns the index and queries that are around the same centres. If scale is not nil, component
// j of the centres and of the noise is multiplied by scale(j).
func benchLoaded(
	b *testing.B, db kv.DB, n int, scale func(j int) float64, opts ...vector.Option,
) (*extstore.Store[int64, doc], *vector.Index[int64, doc], [][]float32) {
	b.Helper()

	docs := extstore.New[int64, doc]("docs")
	x := vector.On(docs, "by_embedding", benchDims, vector.Cosine, embedding, opts...)
	require.NoError(b, docs.Check(db))

	rng := rand.New(rand.NewPCG(1, 2))
	cl := newClusters(rng, benchDims, max(2, int(math.Sqrt(float64(n))/2)), 0.3)
	if scale != nil {
		cl.scaled(scale)
	}
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
	return docs, x, queries
}

// benchSearch measures searches for the 10 nearest vectors, and reports the number of vectors
// (and codes) that a search compares the query with, and for an index that is quantized the
// number of vectors that it reads to re-rank.
func benchSearch(b *testing.B, db kv.DB, x *vector.Index[int64, doc], queries [][]float32, rescore int) {
	b.Helper()

	var scanned, rescored int
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		err := db.View(func(tx kv.Tx) error {
			res, err := benchSearchOf(x.Instantiate(tx), queries[i%len(queries)], rescore).Run()
			if err != nil {
				return err
			}
			defer res.Close()
			if res.Len() != 10 {
				return fmt.Errorf("found %d vectors", res.Len())
			}
			scanned += res.Scanned()
			rescored += res.Rescored()
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(scanned)/float64(b.N), "scanned/op")
	if x.Quantized() {
		b.ReportMetric(float64(rescored)/float64(b.N), "rescored/op")
	}
}
