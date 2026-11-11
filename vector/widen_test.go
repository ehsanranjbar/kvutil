package vector_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/vector"
)

// rareDocs is a condition that 1 in 100 of the docs of the clustered fixture satisfy, wherever
// they are in the clusters.
func rareDocs() cond.Cond[doc] {
	return cond.Func("rare", func(d *doc) bool { return d.Title == "rare" })
}

// countRare returns how many of the keys are of rare docs.
func (f *fixture) countRare(keys []int64) int {
	n := 0
	for _, k := range keys {
		if f.byID[k].Title == "rare" {
			n++
		}
	}
	return n
}

// rareWithVector returns how many rare docs have a vector.
func (f *fixture) rareWithVector() int {
	n := 0
	for _, d := range f.byID {
		if d.Title == "rare" && len(d.Embedding) > 0 {
			n++
		}
	}
	return n
}

func TestWhereWidensTheProbes(t *testing.T) {
	eachMetric(t, []vector.Metric{vector.Cosine, vector.Euclidean}, func(t *testing.T, db kv.DB, metric vector.Metric) {
		const limit = 10
		f, cl, rng := newClusteredFixture(t, db, metric, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		vectors := f.withVector()
		rare := f.rareWithVector()
		require.Greater(t, rare, 2*limit)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)

			short := 0
			recall := 0.0
			const queries = 20
			for range queries {
				f.query = cl.draw(rng)

				// The partitions that are read first have fewer of them than the limit, mostly.
				first := run(t, ins.Search(f.query).Limit(vectors))
				if f.countRare(first.keys) < limit {
					short++
				}

				widened := run(t, ins.Search(f.query).Limit(limit).Where(rareDocs()))
				require.Len(t, widened.keys, limit)
				require.Equal(t, limit, f.countRare(widened.keys))
				f.verify(t, "exact", ins.Search(f.query).Exact().Where(rareDocs()), func(_ int64, d *doc) bool {
					return d.Title == "rare"
				}, limit, math.NaN())
				exact := run(t, ins.Search(f.query).Limit(limit).Exact().Where(rareDocs()))
				// The nearest of them may be in a partition that was not read once there are enough in
				// the ones that were, so what is found is nearly what is exact.
				recall += overlap(exact.keys, widened.keys)
				if f.countRare(first.keys) < limit {
					require.Greater(t, widened.scanned, first.scanned, "it read more partitions")
				}

				// The totals are of all the runs, and an exact search is one.
				require.Equal(t, vectors, exact.scanned)
				require.LessOrEqual(t, widened.scanned, 2*vectors)

				// There are fewer of them than the limit: every partition is read, and the result is
				// what is exact.
				everyRare := run(t, ins.Search(f.query).Limit(1000).Where(rareDocs()))
				require.Len(t, everyRare.keys, rare)
				require.Equal(t, run(t, ins.Search(f.query).Limit(1000).Exact().Where(rareDocs())).keys, everyRare.keys)
			}
			require.Greater(t, short, queries/2, "the first partitions rarely have enough")
			t.Logf("recall@%d %.3f, %d of %d needed more partitions", limit, recall/queries, short, queries)
			require.GreaterOrEqual(t, recall/queries, 0.8, "recall")
			return nil
		}))
	})
}

func TestWhereWidensFromTheProbesOfTheSearch(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, cl, rng := newClusteredFixture(t, db, vector.Euclidean, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		vectors := f.withVector()

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			f.query = cl.draw(rng)

			// One probe is where it starts, and it does not read more than there are.
			one := run(t, ins.Search(f.query).Limit(20).Probes(1).Where(rareDocs()))
			require.Len(t, one.keys, 20)
			require.Equal(t, 20, f.countRare(one.keys))
			require.Greater(t, one.scanned, run(t, ins.Search(f.query).Limit(20).Probes(1)).scanned)

			// Nothing satisfies the condition, so every partition is read before it gives up, once
			// the last run has them all.
			never := cond.Func("never", func(*doc) bool { return false })
			none := run(t, ins.Search(f.query).Limit(5).Probes(1).Where(never))
			require.Empty(t, none.keys)
			require.GreaterOrEqual(t, none.scanned, vectors)
			require.Less(t, none.scanned, 2*vectors)

			// A search with enough in the first run is not run again.
			all := cond.Func("all", func(*doc) bool { return true })
			require.Equal(t, run(t, ins.Search(f.query).Limit(5)), run(t, ins.Search(f.query).Limit(5).Where(all)))
			return nil
		}))
	})
}

func TestWhereDoesNotWidenWhereItIsNotAsked(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, cl, rng := newClusteredFixture(t, db, vector.Cosine, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		vectors := f.withVector()

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			f.query = cl.draw(rng)

			// An exact search reads every vector once.
			require.Equal(t, vectors, run(t, ins.Search(f.query).Limit(20).Exact().Where(rareDocs())).scanned)

			// So does a search of a source, of the records that it has.
			english := 0
			for _, d := range f.byID {
				if d.Lang == "en" && len(d.Embedding) > 0 {
					english++
				}
			}
			source, err := f.byLang.Instantiate(tx).Match("en")
			require.NoError(t, err)
			among := run(t, ins.Search(f.query).Limit(1000).Among(source).Where(rareDocs()))
			require.Equal(t, english, among.scanned)
			require.LessOrEqual(t, f.countRare(among.keys), len(among.keys))

			// A search without a limit finds all it can in the first run.
			unlimited := run(t, ins.Search(f.query).Where(rareDocs()))
			require.Equal(t, run(t, ins.Search(f.query)).scanned, unlimited.scanned)
			return nil
		}))
	})
}

func TestWhereWidensTheRescoreOfCodes(t *testing.T) {
	eachMetric(t, []vector.Metric{vector.Cosine, vector.Euclidean}, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newQuantizedFixture(t, db, metric, 3000)
		rare := f.rareWithVector()
		partitions := stats(t, db, f.x).Partitions

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			for range 10 {
				f.query = cl.draw(rng)

				// The default is the best 100 candidates, of which about one satisfies the condition.
				found := runCodes(t, ins.Search(f.query).Limit(10).Where(rareDocs()))
				require.Len(t, found.keys, 10)
				require.Equal(t, 10, f.countRare(found.keys))
				require.Greater(t, found.rescored, 100)

				// Fewer satisfy it than the limit: every partition is read, and every candidate of the
				// codes is re-ranked, which is what a search that is exact finds.
				every := runCodes(t, ins.Search(f.query).Limit(1000).Where(rareDocs()))
				require.Len(t, every.keys, rare)
				exact := runCodes(t, ins.Search(f.query).Limit(1000).Exact().Where(rareDocs()))
				require.Equal(t, exact.keys, every.keys)

				// With every partition read and the candidates not limited, there is nothing to widen.
				once := runCodes(t, ins.Search(f.query).Limit(1000).Probes(partitions).Rescore(everything).Where(rareDocs()))
				require.Equal(t, exact.keys, once.keys)
				require.Less(t, once.scanned+once.rescored, every.scanned+every.rescored, "no run was repeated")
			}
			return nil
		}))
	})
}
