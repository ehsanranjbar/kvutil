package vector_test

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// clusterDims is the number of dimensions of the clustered vectors of the tests.
const clusterDims = 16

// clusters are random centres that vectors are drawn around, which is what the embeddings of
// texts about some subjects look like, and what partitions are for.
type clusters struct {
	centres [][]float32
	noise   float64
	// scale, if it is not nil, is what each component of a noise draw is multiplied by. The
	// centres are scaled by the same when it is set, with scaled.
	scale []float64
}

// scaled makes component j of every centre and of every noise draw from now on j's scale times
// what it was, which is how embeddings have a few dimensions that carry most of the variance.
func (c *clusters) scaled(scale func(j int) float64) *clusters {
	c.scale = make([]float64, len(c.centres[0]))
	for j := range c.scale {
		c.scale[j] = scale(j)
	}
	for _, centre := range c.centres {
		for j := range centre {
			centre[j] *= float32(c.scale[j])
		}
	}
	return c
}

// newClusters draws k centres of dims components, whose spread is 1.
func newClusters(rng *rand.Rand, dims, k int, noise float64) *clusters {
	c := &clusters{noise: noise}
	for range k {
		centre := make([]float32, dims)
		for i := range centre {
			centre[i] = float32(rng.NormFloat64())
		}
		c.centres = append(c.centres, centre)
	}
	return c
}

// draw returns a vector around one of the centres.
func (c *clusters) draw(rng *rand.Rand) []float32 {
	centre := c.centres[rng.IntN(len(c.centres))]
	v := make([]float32, len(centre))
	for i := range v {
		noise := rng.NormFloat64() * c.noise
		if c.scale != nil {
			noise *= c.scale[i]
		}
		v[i] = centre[i] + float32(noise)
	}
	return v
}

// newClusteredFixture writes n docs around 40 centres, some of which have the vector of the one
// before them, and some none. The index is not built.
func newClusteredFixture(
	t *testing.T, db kv.DB, metric vector.Metric, n int, opts ...vector.Option,
) (*fixture, *clusters, *rand.Rand) {
	t.Helper()

	f := &fixture{metric: metric, docs: extstore.New[int64, doc]("docs"), byID: map[int64]doc{}}
	f.x = vector.On(f.docs, "by_embedding", clusterDims, metric, embedding, opts...)
	f.byLang = index.On(f.docs, "by_lang", field.Path[doc, string]("Lang"))
	require.NoError(t, f.docs.Check(db))

	rng := rand.New(rand.NewPCG(31, 41))
	cl := newClusters(rng, clusterDims, 40, 0.3)
	langs := []string{"en", "fr", "de"}
	for id := int64(1); id <= int64(n); id++ {
		d := doc{Title: fmt.Sprint("doc-", id), Lang: langs[id%3]}
		if id%100 == 0 {
			d.Title = "rare"
		}
		switch {
		case id%97 == 0:
		case id%50 == 0 && len(f.byID[id-1].Embedding) > 0:
			d.Embedding = slices.Clone(f.byID[id-1].Embedding)
		default:
			d.Embedding = cl.draw(rng)
		}
		f.byID[id] = d
	}
	put(t, db, f.docs, f.byID)
	f.query = cl.draw(rng)
	return f, cl, rng
}

// withVector returns the number of docs that have a vector.
func (f *fixture) withVector() int {
	n := 0
	for _, d := range f.byID {
		if len(d.Embedding) > 0 {
			n++
		}
	}
	return n
}

// rebuild builds the index.
func (f *fixture) rebuild(t *testing.T, db kv.DB) {
	t.Helper()
	require.NoError(t, vector.Rebuild(context.Background(), db, f.x))
}

// check checks the keys of the index against the docs that the fixture has written.
func (f *fixture) check(t *testing.T, db kv.DB) layout {
	t.Helper()
	return checkLayout(t, db, f.x, f.docs)
}

// stats returns what the index has.
func stats[K, V any](t *testing.T, db kv.DB, x *vector.Index[K, V]) vector.Stats {
	t.Helper()

	var s vector.Stats
	require.NoError(t, db.View(func(tx kv.Tx) (err error) {
		s, err = x.Instantiate(tx).Stats()
		return err
	}))
	return s
}

// found is what a search finds: the keys, in order, and the number of vectors it compared.
type found struct {
	keys    []int64
	scanned int
}

// run runs a search and returns what it found.
func run(t *testing.T, s vector.Search[int64, doc]) found {
	t.Helper()

	res, err := s.Run()
	require.NoError(t, err)
	defer res.Close()
	keys, err := iterutil.Of(res).Keys()
	require.NoError(t, err)
	return found{keys, res.Scanned()}
}

// overlap returns the fraction of the wanted keys that are found.
func overlap(want, got []int64) float64 {
	n := 0
	for _, k := range want {
		if slices.Contains(got, k) {
			n++
		}
	}
	return float64(n) / float64(len(want))
}

func TestUnpartitionedSearchReadsEverything(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f := newFixture(t, db, metric)
		require.Zero(t, f.x.PartitionSize())
		n := f.withVector()

		require.Equal(t, vector.Stats{Partitions: 0, Unplaced: n}, stats(t, db, f.x))
		require.Equal(t, layout{Unplaced: n, Sizes: map[uint32]int{0: n}}, f.check(t, db))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			plain := run(t, ins.Search(f.query).Limit(10))
			require.Equal(t, n, plain.scanned)

			// The partitions are of no matter where there are none.
			require.Equal(t, plain, run(t, ins.Search(f.query).Limit(10).Probes(1)))
			require.Equal(t, plain, run(t, ins.Search(f.query).Limit(10).Exact()))
			require.Equal(t, n, run(t, ins.Search(f.query).Exact().Probes(3)).scanned)
			return nil
		}))
	})
}

func TestPartitionedSearch(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		const n = 3000
		f, cl, rng := newClusteredFixture(t, db, metric, n, vector.Partitioned(100))
		require.Equal(t, 100, f.x.PartitionSize())
		vectors := f.withVector()

		// The records are all unplaced until the index is built, and a search reads them all.
		require.Equal(t, vector.Stats{Partitions: 0, Unplaced: vectors}, stats(t, db, f.x))
		require.Equal(t, layout{Unplaced: vectors, Sizes: map[uint32]int{0: vectors}}, f.check(t, db))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			require.Equal(t, vectors, run(t, f.x.Instantiate(tx).Search(f.query).Limit(10)).scanned)
			return nil
		}))

		f.rebuild(t, db)
		s := stats(t, db, f.x)
		require.Zero(t, s.Unplaced)
		require.Greater(t, s.Partitions, 10)
		got := f.check(t, db)
		require.Equal(t, vectors, got.Placed)
		require.Equal(t, s.Partitions, got.Partitions)
		for pid, size := range got.Sizes {
			require.LessOrEqual(t, size, 100, "the partition %d", pid)
		}

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)

			var recall float64
			const queries = 40
			for range queries {
				f.query = cl.draw(rng)
				exact := run(t, ins.Search(f.query).Limit(10).Exact())
				approx := run(t, ins.Search(f.query).Limit(10))

				recall += overlap(exact.keys, approx.keys)
				require.Equal(t, vectors, exact.scanned)
				require.Less(t, approx.scanned, vectors/2)

				// An exact search finds what the oracle says.
				f.verify(t, "exact", ins.Search(f.query).Exact(), keepAll, 10, math.NaN())

				// So does one that reads every partition, in the same order.
				all := run(t, ins.Search(f.query).Limit(10).Probes(s.Partitions))
				require.Equal(t, exact.keys, all.keys)
				require.Equal(t, vectors, all.scanned)
				every := run(t, ins.Search(f.query).Probes(s.Partitions))
				require.Equal(t, run(t, ins.Search(f.query).Exact()).keys, every.keys)
			}
			t.Logf("%d partitions, recall@10 %.3f", s.Partitions, recall/queries)
			require.GreaterOrEqual(t, recall/queries, 0.9, "recall@10")
			return nil
		}))
	})
}

func TestProbes(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, cl, rng := newClusteredFixture(t, db, vector.Cosine, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		partitions := stats(t, db, f.x).Partitions

		// The same docs, with a number of probes in the definition. It is no attribute, so the
		// index is not built again.
		defined := extstore.New[int64, doc]("docs")
		withProbes := vector.On(defined, "by_embedding", clusterDims, vector.Cosine, embedding,
			vector.Partitioned(100), vector.Probes(2))
		require.NoError(t, defined.Check(db))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			other := withProbes.Instantiate(tx)
			scanned := func(ins *vector.Instance[int64, doc], q []float32, probes int) int {
				s := ins.Search(q)
				if probes > 0 {
					s = s.Probes(probes)
				}
				return run(t, s.Limit(10)).scanned
			}

			var one, two, five, def int
			for range 20 {
				q := cl.draw(rng)
				counts := []int{
					scanned(ins, q, 1), scanned(ins, q, 2), scanned(ins, q, 5), scanned(ins, q, 20),
					scanned(ins, q, partitions), scanned(ins, q, partitions+10),
				}
				require.True(t, slices.IsSorted(counts), "more probes never scan less: %v", counts)
				require.Equal(t, counts[4], counts[5], "there are no more partitions to read")
				one += counts[0]
				two += counts[1]
				five += counts[2]
				def += scanned(ins, q, 0)

				// The definition says how many, unless the search does.
				require.Equal(t, counts[1], scanned(other, q, 0))
				require.Equal(t, counts[2], scanned(other, q, 5))
			}
			require.Less(t, one, def, "one probe reads less than the default")
			require.Less(t, two, five)

			// The default is the square root of the number of partitions, rounded up.
			root := int(math.Ceil(math.Sqrt(float64(partitions))))
			q := cl.draw(rng)
			require.Equal(t, scanned(ins, q, root), scanned(ins, q, 0))

			for _, n := range []int{0, -1} {
				_, err := ins.Search(f.query).Probes(n).Run()
				require.Error(t, err, n)
			}
			return nil
		}))
		f.check(t, db)
		require.NoError(t, defined.Check(db))
	})
}

func TestPartitionedMaintenance(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		const n = 2000
		f, cl, rng := newClusteredFixture(t, db, metric, n, vector.Partitioned(100))
		f.rebuild(t, db)
		vectors := f.withVector()
		partitions := stats(t, db, f.x).Partitions

		// set writes a doc in a transaction, and the fixture remembers it.
		set := func(id int64, d doc) {
			put(t, db, f.docs, map[int64]doc{id: d})
			f.byID[id] = d
		}
		nearest := func(q []float32, probes int) found {
			var out found
			require.NoError(t, db.View(func(tx kv.Tx) error {
				out = run(t, f.x.Instantiate(tx).Search(q).Limit(5).Probes(probes))
				return nil
			}))
			return out
		}
		vectorOf := func(id int64) []float32 {
			v, err := (&library{docs: f.docs, x: f.x}).vectorOf(t, db, id)
			require.NoError(t, err)
			return v
		}

		// A record that is new is unplaced, and every search finds it at once.
		q := cl.draw(rng)
		mine := q
		if metric == vector.Dot {
			// The greatest inner product is nearest, so the vector has to be a long one.
			mine = scaled(q, 10)
		}
		set(100_001, doc{Title: "new", Lang: "en", Embedding: mine})
		require.Equal(t, int64(100_001), nearest(q, 1).keys[0])
		require.Equal(t, int64(100_001), nearest(q, partitions).keys[0])
		require.Equal(t, vectors, f.check(t, db).Placed)
		require.Equal(t, vector.Stats{Partitions: partitions, Unplaced: 1}, stats(t, db, f.x))
		require.InDeltaSlice(t, prepared(metric, mine), vectorOf(100_001), 1e-6)

		// Another vector for a record that is placed moves it to the unplaced area.
		placed := int64(7)
		before := vectorOf(placed)
		set(placed, doc{Title: "moved", Lang: "en", Embedding: cl.draw(rng)})
		require.Equal(t, vector.Stats{Partitions: partitions, Unplaced: 2}, stats(t, db, f.x))
		require.Equal(t, vectors-1, f.check(t, db).Placed)
		require.NotEqual(t, before, vectorOf(placed))
		require.InDeltaSlice(t, prepared(metric, f.byID[placed].Embedding), vectorOf(placed), 1e-6)

		// Other fields change and the vector does not: the record stays where it is.
		stays := int64(8)
		set(stays, doc{Title: "renamed", Lang: "fr", Embedding: slices.Clone(f.byID[stays].Embedding)})
		require.Equal(t, vector.Stats{Partitions: partitions, Unplaced: 2}, stats(t, db, f.x))
		require.Equal(t, vectors-1, f.check(t, db).Placed)

		// A record that has no vector any more has nothing in the index, wherever it was.
		set(9, doc{Title: "empty"})
		set(100_001, doc{Title: "new, empty"})
		require.Equal(t, vector.Stats{Partitions: partitions, Unplaced: 1}, stats(t, db, f.x))
		require.Equal(t, vectors-2, f.check(t, db).Placed)
		_, err := (&library{docs: f.docs, x: f.x}).vectorOf(t, db, 9)
		require.ErrorIs(t, err, kv.ErrNotFound)

		// And so has a record that is deleted.
		for _, id := range []int64{10, placed} {
			del(t, db, f.docs, id)
			delete(f.byID, id)
		}
		require.Equal(t, vector.Stats{Partitions: partitions, Unplaced: 0}, stats(t, db, f.x))
		require.Equal(t, vectors-3, f.check(t, db).Placed)

		// A deleted record that is gone from the index is not found by a search.
		for range 5 {
			for _, k := range nearest(cl.draw(rng), partitions).keys {
				require.NotContains(t, []int64{9, 10, placed}, k)
			}
		}

		// Records come back as unplaced ones, and the index is built again to place them.
		for id := int64(200_000); id < 200_050; id++ {
			set(id, doc{Embedding: cl.draw(rng)})
		}
		require.Equal(t, 50, stats(t, db, f.x).Unplaced)
		f.check(t, db)
		f.rebuild(t, db)
		require.Zero(t, stats(t, db, f.x).Unplaced)
		f.check(t, db)
	})
}

// scaled returns a vector that is longer or shorter than another one in the same direction.
func scaled(v []float32, by float32) []float32 {
	out := make([]float32, len(v))
	for i, c := range v {
		out[i] = c * by
	}
	return out
}

func TestPartitionedAmong(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 1500, vector.Partitioned(100))
		f.rebuild(t, db)

		// Some of the records are placed and some are not.
		for id := int64(110); id < 130; id++ {
			d := doc{Title: "again", Lang: "en", Embedding: cl.draw(rng)}
			put(t, db, f.docs, map[int64]doc{id: d})
			f.byID[id] = d
		}
		require.Equal(t, 20, stats(t, db, f.x).Unplaced)
		f.check(t, db)

		inRange := func(id int64, _ *doc) bool { return id >= 100 && id < 200 }
		isEnglish := func(_ int64, d *doc) bool { return d.Lang == "en" }
		inRangeWithVector := 0
		for id, d := range f.byID {
			if inRange(id, &d) && len(d.Embedding) > 0 {
				inRangeWithVector++
			}
		}
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			for range 5 {
				f.query = cl.draw(rng)
				source := func() vector.Search[int64, doc] {
					it, err := f.docs.Instantiate(tx).Iterator(kvutil.From[int64](100).Until(200))
					require.NoError(t, err)
					return ins.Search(f.query).Among(it)
				}
				lang := func() vector.Search[int64, doc] {
					it, err := f.byLang.Instantiate(tx).Match("en")
					require.NoError(t, err)
					return ins.Search(f.query).Among(it)
				}

				f.verify(t, "range", source(), inRange, -1, math.NaN())
				f.verify(t, "range limit", source(), inRange, 7, math.NaN())
				f.verify(t, "lang", lang(), isEnglish, -1, math.NaN())

				// The records of the source are what is read, whatever the partitions to read are.
				plain := run(t, source())
				require.Equal(t, plain, run(t, source().Probes(1)))
				require.Equal(t, plain, run(t, source().Exact()))
				require.Equal(t, inRangeWithVector, plain.scanned)
			}
			return nil
		}))
	})
}

func TestPartitionedWhereWithinAndTies(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 1500, vector.Partitioned(100))
		f.rebuild(t, db)

		// Documents with one vector are ties, and the ones that are written after the build are in
		// the unplaced area while the others are not.
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
		f.query = tied
		f.check(t, db)

		english := func(_ int64, d *doc) bool { return d.Lang == "en" }
		isEnglishCond := func() cond.Cond[doc] {
			return cond.Func("english", func(d *doc) bool { return d.Lang == "en" })
		}
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			search := func() vector.Search[int64, doc] { return ins.Search(f.query).Exact() }
			within := f.threshold(0.05)

			f.verify(t, "within", search(), keepAll, -1, within)
			f.verify(t, "limit", search(), keepAll, 8, math.NaN())
			f.verify(t, "where", search().Where(isEnglishCond()), english, 8, math.NaN())
			f.verify(t, "where and within", search().Where(isEnglishCond()), english, 5, within)

			// The ties come by their keys, and the unplaced ones are found by every search.
			res := run(t, ins.Search(f.query).Limit(10))
			require.Subset(t, res.keys, []int64{5000, 5001, 5002, 5003, 5004, 5005})
			return nil
		}))
	})
}

func TestSmallPartitionedIndexHasNoPartitions(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, _ := newClusteredFixture(t, db, vector.Euclidean, 80, vector.Partitioned(100))
		f.rebuild(t, db)

		vectors := f.withVector()
		require.Equal(t, vector.Stats{Partitions: 0, Unplaced: vectors}, stats(t, db, f.x))
		require.Equal(t, layout{Unplaced: vectors, Sizes: map[uint32]int{0: vectors}}, f.check(t, db))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			require.Equal(t, vectors, run(t, f.x.Instantiate(tx).Search(f.query)).scanned)
			return nil
		}))
	})
}

// contents returns every key under the prefix of an index and its value. The nonce of the state
// is random, so it is left out.
func contents(t *testing.T, db kv.DB, prefix []byte) map[string]string {
	t.Helper()

	out := map[string]string{}
	require.NoError(t, db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(prefix, kv.PrefixEnd(prefix))
		require.NoError(t, err)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			value, err := it.Value()
			require.NoError(t, err)
			if string(it.Key()[len(prefix):]) == "s" {
				value = value[8:]
			}
			out[string(it.Key())] = string(value)
		}
		return nil
	}))
	return out
}

func TestRebuildIsDeterministic(t *testing.T) {
	for name, open := range map[string]func(testing.TB) kv.DB{"mem": testkv.NewMem, "badger": testkv.NewBadger} {
		t.Run(name, func(t *testing.T) {
			var all []map[string]string
			for range 2 {
				db := open(t)
				f, _, _ := newClusteredFixture(t, db, vector.Cosine, 2500, vector.Partitioned(100))
				f.rebuild(t, db)
				require.Greater(t, stats(t, db, f.x).Partitions, 10)
				all = append(all, contents(t, db, f.x.Prefix()))
			}
			require.Equal(t, all[0], all[1])
		})
	}
}

func TestPartitionedMigration(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ctx := context.Background()

		// A program with an index that is not partitioned writes the docs.
		v1 := newLibrary(clusterDims, vector.Cosine)
		v1.ready(t, db)
		rng := rand.New(rand.NewPCG(7, 8))
		cl := newClusters(rng, clusterDims, 20, 0.3)
		docs := map[int64]doc{}
		for id := int64(1); id <= 1000; id++ {
			docs[id] = doc{Embedding: cl.draw(rng)}
		}
		put(t, db, v1.docs, docs)
		require.Equal(t, vector.Stats{Unplaced: 1000}, stats(t, db, v1.x))

		// The next one partitions it, which builds it again.
		v2 := newLibrary(clusterDims, vector.Cosine, vector.Partitioned(100))
		require.ErrorIs(t, v2.docs.Check(db), migrate.ErrNeedsMigration)
		report, err := v2.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.NotEmpty(t, report.String())
		require.NoError(t, v2.docs.Check(db))
		s2 := stats(t, db, v2.x)
		require.Greater(t, s2.Partitions, 1)
		require.Zero(t, s2.Unplaced)
		require.Equal(t, 1000, v2.check(t, db).Placed)

		// Another size does too.
		v3 := newLibrary(clusterDims, vector.Cosine, vector.Partitioned(300))
		require.ErrorIs(t, v3.docs.Check(db), migrate.ErrNeedsMigration)
		_, err = v3.docs.Migrate(ctx, db)
		require.NoError(t, err)
		s3 := stats(t, db, v3.x)
		require.Greater(t, s3.Partitions, 1)
		require.Less(t, s3.Partitions, s2.Partitions)
		require.Equal(t, 1000, v3.check(t, db).Placed)

		// The number of probes is no data, so changing it changes nothing in the database.
		v4 := newLibrary(clusterDims, vector.Cosine, vector.Partitioned(300), vector.Probes(3))
		require.NoError(t, v4.docs.Check(db))
		again, err := v4.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.Empty(t, again.String())
		require.Equal(t, s3, stats(t, db, v4.x))

		// And going back to an index that is not partitioned puts every vector in the unplaced area.
		v5 := newLibrary(clusterDims, vector.Cosine)
		require.ErrorIs(t, v5.docs.Check(db), migrate.ErrNeedsMigration)
		_, err = v5.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.Equal(t, vector.Stats{Unplaced: 1000}, stats(t, db, v5.x))
		v5.check(t, db)
	})
}

func TestPartitionOptions(t *testing.T) {
	require.Panics(t, func() { vector.Partitioned(-1) })
	require.Panics(t, func() { vector.Probes(-1) })
	require.NotPanics(t, func() { vector.Probes(0) })

	// The size is a default that depends on the dimensions, if it is not given.
	for dims, want := range map[int]int{2: 1024, 384: 682, 768: 341, 4096: 64} {
		l := newLibrary(dims, vector.Cosine, vector.Partitioned(0))
		require.Equal(t, want, l.x.PartitionSize(), dims)
	}
	require.Equal(t, 50, newLibrary(4, vector.Cosine, vector.Partitioned(50)).x.PartitionSize())
	require.Zero(t, newLibrary(4, vector.Cosine).x.PartitionSize())
}

func TestEuclideanProbesTheNearestCentroid(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		docs := extstore.New[int64, doc]("docs")
		x := vector.On(docs, "by_embedding", 2, vector.Euclidean, embedding, vector.Partitioned(100))
		require.NoError(t, docs.Check(db))

		// Groups of vectors along one direction, each ten times further than the one before. The
		// furthest has the greatest inner product with any query, and is not the nearest.
		rng := rand.New(rand.NewPCG(5, 6))
		byID := map[int64]doc{}
		id := int64(0)
		for _, radius := range []float64{1, 10, 100, 1000} {
			// Few enough for a group to be one partition.
			for range 40 {
				id++
				byID[id] = doc{Embedding: []float32{
					float32(radius * (1 + 0.01*rng.NormFloat64())),
					float32(radius * 0.01 * rng.NormFloat64()),
				}}
			}
		}
		put(t, db, docs, byID)
		require.NoError(t, vector.Rebuild(context.Background(), db, x))
		require.Equal(t, 4, stats(t, db, x).Partitions)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := x.Instantiate(tx)
			for _, radius := range []float32{1, 10, 100, 1000} {
				q := []float32{radius, 0}
				exact := run(t, ins.Search(q).Exact().Limit(5))
				one := run(t, ins.Search(q).Probes(1).Limit(5))
				require.Equal(t, exact.keys, one.keys, radius)
				require.Less(t, one.scanned, exact.scanned, radius)
			}
			return nil
		}))
	})
}

func TestPartitionsAreReadAgainAfterARebuild(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f, _, _ := newClusteredFixture(t, db, vector.Euclidean, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		before := f.check(t, db)
		// A search, so that the index remembers the partitions that it has now.
		require.Equal(t, before.Partitions, stats(t, db, f.x).Partitions)

		// Most of the docs go, and the index is built again with other partitions, and fewer.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := f.docs.Instantiate(tx)
			for id := range f.byID {
				if id%5 != 0 {
					if err := store.Delete(id); err != nil {
						return err
					}
					delete(f.byID, id)
				}
			}
			return nil
		}))
		f.rebuild(t, db)
		after := f.check(t, db)
		require.Less(t, after.Partitions, before.Partitions)
		require.Equal(t, after.Partitions, stats(t, db, f.x).Partitions)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			exact := run(t, ins.Search(f.query).Exact().Limit(10))
			all := run(t, ins.Search(f.query).Probes(after.Partitions).Limit(10))
			require.Equal(t, exact.keys, all.keys)
			require.Equal(t, exact.scanned, all.scanned)
			return nil
		}))
	})
}
