package vector_test

import (
	"context"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// everything is a number of candidates to re-rank that is more than any index in a test has.
const everything = 1 << 30

// codeFound is what a search of an index that is quantized finds: the keys, in order, and how many
// codes and vectors it read.
type codeFound struct {
	found
	rescored int
}

// runCodes runs a search and returns what it found.
func runCodes(t *testing.T, s vector.Search[int64, doc]) codeFound {
	t.Helper()

	res, err := s.Run()
	require.NoError(t, err)
	defer res.Close()
	keys := make([]int64, 0, res.Len())
	for ; res.Valid(); res.Next() {
		key, err := res.Key()
		require.NoError(t, err)
		keys = append(keys, key)
	}
	return codeFound{found{keys, res.Scanned()}, res.Rescored()}
}

// newQuantizedFixture writes docs around centres and builds a partitioned index of them that is
// quantized.
func newQuantizedFixture(t *testing.T, db kv.DB, metric vector.Metric, n int) (*fixture, *clusters, *rand.Rand) {
	t.Helper()

	f, cl, rng := newClusteredFixture(t, db, metric, n, vector.Partitioned(100), vector.Quantized())
	require.True(t, f.x.Quantized())
	f.rebuild(t, db)
	return f, cl, rng
}

// requireQuantizedRecall checks that searches of the index find what exact ones do, as far as is
// asked for, and that they find it all when they read every partition and re-rank every candidate.
func requireQuantizedRecall(t *testing.T, db kv.DB, f *fixture, queries [][]float32, atLeast float64) {
	t.Helper()

	partitions := stats(t, db, f.x).Partitions
	require.Positive(t, partitions)
	var recall float64
	require.NoError(t, db.View(func(tx kv.Tx) error {
		ins := f.x.Instantiate(tx)
		for _, q := range queries {
			exact := run(t, ins.Search(q).Limit(10).Exact())
			recall += overlap(exact.keys, run(t, ins.Search(q).Limit(10)).keys)
			all := ins.Search(q).Limit(10).Probes(partitions).Rescore(everything)
			require.Equal(t, exact.keys, run(t, all).keys)
		}
		return nil
	}))
	t.Logf("%d partitions, recall@10 %.3f", partitions, recall/float64(len(queries)))
	require.GreaterOrEqual(t, recall/float64(len(queries)), atLeast, "recall@10")
}

func TestQuantizedSearch(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		const n = 3000
		f, cl, rng := newClusteredFixture(t, db, metric, n, vector.Partitioned(100), vector.Quantized())
		vectors := f.withVector()

		// Until the index is built the records are unplaced, and have no codes: a search reads
		// them all, as vectors.
		require.Equal(t, layout{Unplaced: vectors, Sizes: map[uint32]int{0: vectors}}, f.check(t, db))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res := runCodes(t, f.x.Instantiate(tx).Search(f.query).Limit(10))
			require.Equal(t, vectors, res.scanned)
			require.Zero(t, res.rescored)
			return nil
		}))

		f.rebuild(t, db)
		s := stats(t, db, f.x)
		require.Zero(t, s.Unplaced)
		require.Greater(t, s.Partitions, 10)
		got := f.check(t, db)
		require.Equal(t, vectors, got.Placed)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)

			var recall float64
			const queries = 40
			for range queries {
				f.query = cl.draw(rng)
				exact := run(t, ins.Search(f.query).Limit(10).Exact())
				approx := runCodes(t, ins.Search(f.query).Limit(10))

				recall += overlap(exact.keys, approx.keys)
				require.Equal(t, vectors, exact.scanned)
				require.Less(t, approx.scanned, vectors/2)
				require.Positive(t, approx.rescored)
				require.LessOrEqual(t, approx.rescored, 100, "the default is 100 for a limit of 10")
				require.LessOrEqual(t, approx.rescored, approx.scanned)

				// An exact search finds what the oracle says, whatever the codes are.
				f.verify(t, "exact", ins.Search(f.query).Exact().Rescore(3), keepAll, 10, math.NaN())

				// Reading every partition and re-ranking every candidate is the exact search.
				all := runCodes(t, ins.Search(f.query).Limit(10).Probes(s.Partitions).Rescore(everything))
				require.Equal(t, exact.keys, all.keys)
				require.Equal(t, vectors, all.scanned, "a code for every vector")
				require.Equal(t, vectors, all.rescored)
				every := run(t, ins.Search(f.query).Rescore(everything).Probes(s.Partitions))
				require.Equal(t, run(t, ins.Search(f.query).Exact()).keys, every.keys)
			}
			t.Logf("%d partitions, recall@10 %.3f", s.Partitions, recall/queries)
			require.GreaterOrEqual(t, recall/queries, 0.95, "recall@10")
			return nil
		}))
	})
}

// The distances that a search of codes returns are the exact ones, whichever candidates the
// codes chose, and the number of candidates that are re-ranked is the one that is asked for.
func TestQuantizedSearchRescoresWhatItIsAskedTo(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newQuantizedFixture(t, db, metric, 3000)
		partitions := stats(t, db, f.x).Partitions

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			var few, many float64
			for range 10 {
				f.query = cl.draw(rng)

				// Every partition is read, so that there are more candidates than are re-ranked.
				for _, c := range []struct {
					name           string
					search         vector.Search[int64, doc]
					rescored, want int
				}{
					{"default", ins.Search(f.query).Limit(10), 100, 10},
					{"a limit of 30", ins.Search(f.query).Limit(30), 300, 30},
					{"rescore 7", ins.Search(f.query).Limit(3).Rescore(7), 7, 3},
					{"rescore 1", ins.Search(f.query).Limit(1).Rescore(1), 1, 1},
					{"rescore without a limit", ins.Search(f.query).Rescore(25), 25, 25},
				} {
					c.search = c.search.Probes(partitions)
					res, err := c.search.Run()
					require.NoError(t, err, c.name)
					require.Equal(t, c.rescored, res.Rescored(), c.name)
					require.Equal(t, c.want, res.Len(), c.name)
					for ; res.Valid(); res.Next() {
						key, err := res.Key()
						require.NoError(t, err)
						require.InDelta(t, trueDistance(metric, f.query, f.byID[key].Embedding),
							res.Distance(), 1e-4, "%s: the distance of %d", c.name, key)
					}
					require.NoError(t, res.Close())
				}

				exact := run(t, ins.Search(f.query).Limit(10).Exact())
				few += overlap(exact.keys, runCodes(t, ins.Search(f.query).Limit(10).Rescore(10)).keys)
				many += overlap(exact.keys, runCodes(t, ins.Search(f.query).Limit(10).Rescore(500)).keys)
			}
			// Re-ranking more candidates finds more of the nearest ones.
			t.Logf("recall@10 %.3f re-ranking 10, %.3f re-ranking 500", few/10, many/10)
			require.LessOrEqual(t, few, many)

			// A search without a limit that does not say how many to re-rank has to measure every
			// candidate, so it reads the vectors of the partitions and not their codes.
			all := runCodes(t, ins.Search(f.query).Probes(partitions))
			require.Zero(t, all.rescored)
			require.Equal(t, run(t, ins.Search(f.query).Exact()).keys, all.keys)
			return nil
		}))
	})
}

// A search that is exact, or of the records of a source, or of an index that is not quantized,
// is the same whatever it is told to re-rank.
func TestRescoreMakesNoDifferenceWhereThereAreNoCodes(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newQuantizedFixture(t, db, metric, 1500)

		q := cl.draw(rng)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			exact := runCodes(t, ins.Search(q).Limit(10).Exact())
			require.Zero(t, exact.rescored)
			require.Equal(t, exact, runCodes(t, ins.Search(q).Limit(10).Exact().Rescore(1)))
			require.Equal(t, f.withVector(), exact.scanned)

			source := func() vector.Search[int64, doc] {
				it, err := f.docs.Instantiate(tx).Iterator(kvutil.From[int64](100).Until(200))
				require.NoError(t, err)
				return ins.Search(q).Among(it)
			}
			among := runCodes(t, source().Limit(5))
			require.Zero(t, among.rescored)
			require.Equal(t, among, runCodes(t, source().Limit(5).Rescore(1)))
			f.query = q
			f.verify(t, "among", source().Rescore(1), func(id int64, _ *doc) bool { return id >= 100 && id < 200 }, 5, math.NaN())
			return nil
		}))
	})
}

// An index that has no codes is searched the same way whatever the search says to re-rank.
func TestRescoreOfAnIndexWithoutCodes(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f := newFixtureWith(t, db, metric, vector.Partitioned(20))
		require.False(t, f.x.Quantized())

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			plain := runCodes(t, ins.Search(f.query).Limit(5))
			require.Zero(t, plain.rescored)
			require.Equal(t, plain, runCodes(t, ins.Search(f.query).Limit(5).Rescore(2)))
			require.Equal(t, plain, runCodes(t, ins.Search(f.query).Limit(5).Rescore(everything)))
			return nil
		}))
	})
}

func TestRescoreMustBePositive(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, _ := newQuantizedFixture(t, db, vector.Cosine, 1000)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			for _, n := range []int{0, -1} {
				_, err := f.x.Instantiate(tx).Search(f.query).Rescore(n).Run()
				require.Error(t, err, n)
				_, err = f.x.Instantiate(tx).Search(f.query).Limit(5).Rescore(n).Run()
				require.Error(t, err, n)
			}
			_, err := f.x.Instantiate(tx).Search(f.query).Rescore(1).Run()
			require.NoError(t, err)
			return nil
		}))
	})

	// It is a mistake of the search of an index that has no codes too.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db, vector.Cosine)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			_, err := f.x.Instantiate(tx).Search(f.query).Rescore(0).Run()
			require.Error(t, err)
			return nil
		}))
	})
}

func TestQuantizedNeedsPartitions(t *testing.T) {
	docs := extstore.New[int64, doc]("docs")
	require.Panics(t, func() { vector.On(docs, "a", 4, vector.Cosine, embedding, vector.Quantized()) })
	require.Panics(t, func() {
		vector.On(extstore.New[int64, doc]("docs"), "a", 4, vector.Cosine, embedding, vector.Probes(3), vector.Quantized())
	})

	// Whatever the order of the options is.
	for _, opts := range [][]vector.Option{
		{vector.Partitioned(50), vector.Quantized()},
		{vector.Quantized(), vector.Partitioned(50)},
	} {
		l := newLibrary(4, vector.Cosine, opts...)
		require.True(t, l.x.Quantized())
		require.Equal(t, 50, l.x.PartitionSize())
	}
	require.False(t, newLibrary(4, vector.Cosine, vector.Partitioned(50)).x.Quantized())
	require.False(t, newLibrary(4, vector.Cosine).x.Quantized())
}

func TestSearchOracleQuantized(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f := newFixtureWith(t, db, metric, vector.Partitioned(20), vector.Quantized())
		partitions := stats(t, db, f.x).Partitions
		require.Greater(t, partitions, 3)

		// Every partition is read and every candidate is re-ranked, so the results are exact.
		oracleCases(t, db, f, func(s vector.Search[int64, doc]) vector.Search[int64, doc] {
			return s.Probes(partitions).Rescore(everything)
		})
	})
}

func TestQuantizedWhereWithinAndTies(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newQuantizedFixture(t, db, metric, 1500)
		partitions := stats(t, db, f.x).Partitions

		// Documents with one vector are ties, and the ones that are written after the build are in
		// the unplaced area, without codes, while the others are not.
		tied := cl.draw(rng)
		if metric == vector.Dot {
			// The greatest inner product is nearest, so the vector has to be a long one.
			tied = scaled(tied, 10)
		}
		for id := int64(5000); id < 5006; id++ {
			d := doc{Lang: "en", Embedding: slices.Clone(tied)}
			put(t, db, f.docs, map[int64]doc{id: d})
			f.byID[id] = d
		}
		// And some placed ones: other records get the vector too, which unplaces them.
		for id := int64(11); id < 14; id++ {
			d := doc{Lang: "en", Embedding: slices.Clone(tied)}
			put(t, db, f.docs, map[int64]doc{id: d})
			f.byID[id] = d
		}
		f.query = tied
		f.check(t, db)

		english := func(_ int64, d *doc) bool { return d.Lang == "en" }
		isEnglishCond := func() cond.Cond[doc] {
			return cond.Func("english", func(d *doc) bool { return d.Lang == "en" })
		}
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			search := func() vector.Search[int64, doc] {
				return ins.Search(f.query).Probes(partitions).Rescore(everything)
			}
			within := f.threshold(0.05)

			f.verify(t, "within", search(), keepAll, -1, within)
			f.verify(t, "limit", search(), keepAll, 8, math.NaN())
			f.verify(t, "limit of the ties", search(), keepAll, 4, math.NaN())
			f.verify(t, "where", search().Where(isEnglishCond()), english, 8, math.NaN())
			f.verify(t, "where and within", search().Where(isEnglishCond()), english, 5, within)

			// The ties come by their keys: the placed ones and the unplaced ones alike are found.
			res := runCodes(t, ins.Search(f.query).Limit(10))
			require.Subset(t, res.keys, []int64{5000, 5001, 5002, 5003, 5004, 5005})
			return nil
		}))
	})
}

// The estimates are not checked against the distance that a search is limited to, as they are not
// exact: a vector within the distance is found if it is among the candidates, whatever its
// estimate is.
func TestQuantizedWithinIsOfTheExactDistance(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, cl, rng := newQuantizedFixture(t, db, vector.Euclidean, 2000)
		partitions := stats(t, db, f.x).Partitions

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			for range 10 {
				f.query = cl.draw(rng)
				within := float32(f.threshold(0.02))
				res := runCodes(t, ins.Search(f.query).Probes(partitions).Rescore(everything).Within(within))
				want := 0
				for _, c := range f.oracle(keepAll) {
					if c.dist <= float64(within) {
						want++
					}
				}
				require.Equal(t, want, len(res.keys), "everything within is found")

				// Fewer candidates than there are within the distance find fewer of them.
				few := runCodes(t, ins.Search(f.query).Probes(partitions).Rescore(5).Within(within))
				require.LessOrEqual(t, len(few.keys), 5)
			}
			return nil
		}))
	})
}

func TestQuantizedMaintenance(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		const n = 2000
		f, cl, rng := newQuantizedFixture(t, db, metric, n)
		built := f.check(t, db)
		partitions := stats(t, db, f.x).Partitions

		set := func(id int64, d doc) {
			put(t, db, f.docs, map[int64]doc{id: d})
			f.byID[id] = d
			f.check(t, db)
		}

		// A record that is new is unplaced, and has no code.
		set(100_001, doc{Title: "new", Embedding: cl.draw(rng)})
		require.Equal(t, vector.Stats{Partitions: partitions, Unplaced: 1}, stats(t, db, f.x))

		// Another vector for a record that is placed takes its code away with the vector.
		placed := int64(7)
		set(placed, doc{Title: "moved", Embedding: cl.draw(rng)})
		require.Equal(t, built.Placed-1, f.check(t, db).Placed)

		// The same vector leaves the record where it is, with its code.
		stays := int64(8)
		set(stays, doc{Title: "renamed", Lang: "fr", Embedding: slices.Clone(f.byID[stays].Embedding)})
		require.Equal(t, built.Placed-1, f.check(t, db).Placed)

		// No vector, and a record that is gone.
		set(9, doc{Title: "empty"})
		set(100_001, doc{Title: "new, empty"})
		for _, id := range []int64{10, placed} {
			del(t, db, f.docs, id)
			delete(f.byID, id)
			f.check(t, db)
		}
		require.Equal(t, built.Placed-3, f.check(t, db).Placed)

		// The upkeep runner places what is unplaced, and its codes are against the centroids.
		f.add(t, db, cl, rng, 200_000, 50)
		f.check(t, db)
		m := newUpkeeps(db, f.docs)
		maintain(t, m)
		settled(t, db, f, 100)
		requireQuantizedRecall(t, db, f, cl.queries(rng, 40), 0.95)

		// As many records again: the partitions that fill up are split, and the codes of their
		// vectors are of the centroids of the partitions they are split into.
		f.add(t, db, cl, rng, 10_000, 4000)
		f.check(t, db)
		maintain(t, m)
		after := settled(t, db, f, 100)
		// A partition that is split is replaced by two with new ids, which are past the ones of the build.
		require.Greater(t, slices.Max(slices.Collect(maps.Keys(after.Sizes))),
			slices.Max(slices.Collect(maps.Keys(built.Sizes))), "partitions were split")
		requireQuantizedRecall(t, db, f, cl.queries(rng, 40), 0.95)

		// Placed records get other vectors, or none, or go.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := f.docs.Instantiate(tx)
			for id := int64(1); id <= 900; id++ {
				d, ok := f.byID[id]
				if !ok {
					continue
				}
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
		f.check(t, db)
		maintain(t, m)
		settled(t, db, f, 100)

		// Nine of ten records go, which leaves partitions with few vectors, and they are dissolved:
		// their codes go and their vectors are placed in other partitions, with codes of theirs.
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
		f.check(t, db)
		maintain(t, m)
		last := settled(t, db, f, 100)
		require.Less(t, last.Partitions, after.Partitions)
		requireQuantizedRecall(t, db, f, cl.queries(rng, 40), 0.8)

		// The partitions that the index remembers after the splits and dissolves, with their rotated
		// centroids, are what another program reads from the database: it finds the same.
		other := extstore.New[int64, doc]("docs")
		fresh := vector.On(other, "by_embedding", clusterDims, metric, embedding,
			vector.Partitioned(100), vector.Quantized())
		require.NoError(t, other.Check(db))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			for _, q := range cl.queries(rng, 20) {
				want := runCodes(t, f.x.Instantiate(tx).Search(q).Limit(10))
				require.Equal(t, want, runCodes(t, fresh.Instantiate(tx).Search(q).Limit(10)))
			}
			return nil
		}))

		// And a build again leaves the codes right.
		f.rebuild(t, db)
		f.check(t, db)
	})
}

func TestQuantizedMaintenanceFromNothing(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 3000, vector.Partitioned(100), vector.Quantized())
		vectors := f.withVector()

		maintain(t, newUpkeeps(db, f.docs))
		got := settled(t, db, f, 100)
		require.Equal(t, vectors, got.Placed)
		require.Greater(t, got.Partitions, 10)
		requireQuantizedRecall(t, db, f, cl.queries(rng, 40), 0.95)
	})
}

func TestQuantizedMigration(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ctx := context.Background()

		v1 := newLibrary(clusterDims, vector.Cosine, vector.Partitioned(100))
		v1.ready(t, db)
		rng := rand.New(rand.NewPCG(7, 8))
		cl := newClusters(rng, clusterDims, 20, 0.3)
		docs := map[int64]doc{}
		for id := int64(1); id <= 1000; id++ {
			docs[id] = doc{Embedding: cl.draw(rng)}
		}
		put(t, db, v1.docs, docs)
		require.NoError(t, vector.Rebuild(ctx, db, v1.x))
		require.Equal(t, 1000, v1.check(t, db).Placed)

		// Turning the codes on builds the index again, with codes.
		v2 := newLibrary(clusterDims, vector.Cosine, vector.Partitioned(100), vector.Quantized())
		require.ErrorIs(t, v2.docs.Check(db), migrate.ErrNeedsMigration)
		report, err := v2.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.NotEmpty(t, report.String())
		require.NoError(t, v2.docs.Check(db))
		require.Equal(t, 1000, v2.check(t, db).Placed)
		require.Zero(t, stats(t, db, v2.x).Unplaced)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res, err := v2.x.Instantiate(tx).Search(docs[1].Embedding).Limit(1).Run()
			require.NoError(t, err)
			require.Positive(t, res.Rescored())
			return nil
		}))

		// Nothing changed since, so nothing is built.
		again, err := v2.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.Empty(t, again.String())

		// Turning them off does too, and there is no code left.
		v3 := newLibrary(clusterDims, vector.Cosine, vector.Partitioned(100))
		require.ErrorIs(t, v3.docs.Check(db), migrate.ErrNeedsMigration)
		_, err = v3.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.Equal(t, 1000, v3.check(t, db).Placed)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res, err := v3.x.Instantiate(tx).Search(docs[1].Embedding).Limit(1).Run()
			require.NoError(t, err)
			require.Zero(t, res.Rescored())
			return nil
		}))
	})
}

// A vector that has a code of another length than the index makes is a corruption, and so is a
// code without its vector.
func TestQuantizedCorruption(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, _ := newQuantizedFixture(t, db, vector.Cosine, 1000)

		// A code that is not as long as it should be.
		var codeKey []byte
		require.NoError(t, db.View(func(tx kv.Tx) error {
			p := append(slices.Clone(f.x.Prefix()), 'q')
			it, err := tx.Iterator(p, kv.PrefixEnd(p))
			require.NoError(t, err)
			defer it.Close()
			require.True(t, it.Valid())
			codeKey = slices.Clone(it.Key())
			return nil
		}))
		original := dump(t, db, f.x.Prefix())[string(codeKey)]
		require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Set(codeKey, []byte{1, 2, 3}) }))
		partitions := stats(t, db, f.x).Partitions
		require.NoError(t, db.View(func(tx kv.Tx) error {
			_, err := f.x.Instantiate(tx).Search(f.query).Limit(5).Probes(partitions).Run()
			require.ErrorIs(t, err, vector.ErrCorrupt)
			return nil
		}))

		// A code of a vector that is not there.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			if err := tx.Set(codeKey, []byte(original)); err != nil {
				return err
			}
			vectorKey := slices.Clone(codeKey)
			vectorKey[len(f.x.Prefix())] = 'p'
			return tx.Delete(vectorKey)
		}))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			_, err := f.x.Instantiate(tx).Search(f.query).Limit(5).Probes(partitions).Rescore(everything).Run()
			require.ErrorIs(t, err, vector.ErrCorrupt)
			return nil
		}))
	})
}
