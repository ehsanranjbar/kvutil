package vector_test

import (
	"errors"
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
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

const (
	oracleDims = 8
	oracleDocs = 300
)

// fixture is a store of docs with random embeddings, and an oracle that knows how near they are.
type fixture struct {
	metric vector.Metric
	docs   *extstore.Store[int64, doc]
	x      *vector.Index[int64, doc]
	byLang *index.FieldIndex[int64, doc, string]
	byID   map[int64]doc
	query  []float32
}

// newFixture writes the docs: some of them without a vector, and some with the vector of
// another one, so that there are ties.
func newFixture(t *testing.T, db kv.DB, metric vector.Metric) *fixture {
	t.Helper()

	f := &fixture{metric: metric, docs: extstore.New[int64, doc]("docs"), byID: map[int64]doc{}}
	f.x = vector.On(f.docs, "by_embedding", oracleDims, metric, embedding)
	f.byLang = index.On(f.docs, "by_lang", field.Path[doc, string]("Lang"))
	require.NoError(t, f.docs.Check(db))

	random := func(rng *rand.Rand) []float32 {
		v := make([]float32, oracleDims)
		for i := range v {
			v[i] = float32(rng.NormFloat64())
		}
		return v
	}

	rng := rand.New(rand.NewPCG(11, 22))
	langs := []string{"en", "en", "fr", "de"}
	var withVector []int64
	for id := int64(1); id <= oracleDocs; id++ {
		d := doc{Title: fmt.Sprint("doc-", id), Lang: langs[rng.IntN(len(langs))]}
		if id%20 == 0 {
			d.Title = "rare"
		}
		switch {
		case rng.IntN(10) == 0:
			// No vector.
		case len(withVector) > 0 && rng.IntN(8) == 0:
			d.Embedding = slices.Clone(f.byID[withVector[rng.IntN(len(withVector))]].Embedding)
			withVector = append(withVector, id)
		default:
			d.Embedding = random(rng)
			withVector = append(withVector, id)
		}
		f.byID[id] = d
	}
	put(t, db, f.docs, f.byID)
	f.query = random(rng)
	return f
}

// candidate is a record with its distance from the query as the oracle has it.
type candidate struct {
	id   int64
	dist float64
}

// trueDistance measures the distance in float64 from the vectors as they were given.
func trueDistance(metric vector.Metric, a, b []float32) float64 {
	var dot, na, nb, sq float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
		sq += (x - y) * (x - y)
	}
	switch metric {
	case vector.Cosine:
		return 1 - dot/math.Sqrt(na*nb)
	case vector.Euclidean:
		return math.Sqrt(sq)
	}
	return -dot
}

// oracle returns the records that have a vector and are kept, nearest first.
func (f *fixture) oracle(keep func(int64, *doc) bool) []candidate {
	var all []candidate
	for id, d := range f.byID {
		if len(d.Embedding) == 0 || !keep(id, &d) {
			continue
		}
		all = append(all, candidate{id, trueDistance(f.metric, f.query, d.Embedding)})
	}
	slices.SortFunc(all, func(a, b candidate) int {
		switch {
		case a.dist < b.dist:
			return -1
		case a.dist > b.dist:
			return 1
		}
		return int(a.id - b.id)
	})
	return all
}

func keepAll(int64, *doc) bool { return true }

// threshold returns a distance that a fraction of the records are within, and that no record is
// near enough to for rounding to make a difference.
func (f *fixture) threshold(fraction float64) float64 {
	all := f.oracle(keepAll)
	for m := int(float64(len(all)) * fraction); m+1 < len(all); m++ {
		if all[m+1].dist-all[m].dist > 1e-3 {
			return (all[m].dist + all[m+1].dist) / 2
		}
	}
	panic("no gap to put a threshold in")
}

// hit is a result as it is read from the results.
type hit struct {
	id   int64
	dist float32
}

// drain reads the results to the end, and closes them.
func drain(t *testing.T, res *vector.Results[int64, doc]) []hit {
	t.Helper()

	var hits []hit
	for ; res.Valid(); res.Next() {
		key, err := res.Key()
		require.NoError(t, err)
		hits = append(hits, hit{key, res.Distance()})
	}
	require.NoError(t, res.Close())
	require.Equal(t, len(hits), res.Len())
	return hits
}

// verify runs a search and compares what it finds with what the oracle says. A limit below 0 is
// no limit, and a distance of NaN is no distance. Rounding in float32 may put records that are
// nearly as near in another order than float64 does, so the comparison is of distances.
func (f *fixture) verify(t *testing.T, name string, s vector.Search[int64, doc],
	keep func(int64, *doc) bool, k int, within float64) {
	t.Helper()

	want := f.oracle(keep)
	if !math.IsNaN(within) {
		s = s.Within(float32(within))
		want = slices.DeleteFunc(want, func(c candidate) bool { return c.dist > within })
	}
	if k >= 0 {
		s = s.Limit(k)
		want = want[:min(k, len(want))]
	}

	res, err := s.Run()
	require.NoError(t, err, name)
	got := drain(t, res)
	require.Len(t, got, len(want), name)
	if len(got) == 0 {
		return
	}

	dists := map[int64]float64{}
	for id, d := range f.byID {
		if len(d.Embedding) > 0 {
			dists[id] = trueDistance(f.metric, f.query, d.Embedding)
		}
	}
	for i, h := range got {
		d := f.byID[h.id]
		require.True(t, keep(h.id, &d), "%s: %d is not wanted", name, h.id)
		require.InDelta(t, dists[h.id], h.dist, 1e-4, "%s: the distance of %d", name, h.id)
		if i > 0 {
			require.LessOrEqual(t, dists[got[i-1].id], dists[h.id]+1e-5, "%s: order at %d", name, i)
			require.LessOrEqual(t, got[i-1].dist, h.dist, "%s: order at %d", name, i)
			// The same vector is the same distance, so the keys are what orders them.
			if slices.Equal(f.byID[got[i-1].id].Embedding, d.Embedding) {
				require.Less(t, got[i-1].id, h.id, "%s: tie at %d", name, i)
			}
		}
	}
	require.InDelta(t, want[len(want)-1].dist, dists[got[len(got)-1].id], 1e-5, "%s: the last", name)

	// What the last result ties with is there if its key is less.
	last := got[len(got)-1]
	for id, d := range f.byID {
		if id < last.id && keep(id, &d) && len(d.Embedding) > 0 && slices.Equal(d.Embedding, f.byID[last.id].Embedding) {
			require.True(t, slices.ContainsFunc(got, func(h hit) bool { return h.id == id }),
				"%s: %d ties with the last and has a less key", name, id)
		}
	}
}

func TestSearchOracle(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f := newFixture(t, db, metric)
		none := math.NaN()

		english := cond.Func("english", func(d *doc) bool { return d.Lang == "en" })
		rare := cond.Func("rare", func(d *doc) bool { return d.Title == "rare" })
		isEnglish := func(_ int64, d *doc) bool { return d.Lang == "en" }
		isRare := func(_ int64, d *doc) bool { return d.Title == "rare" }
		inRange := func(id int64, _ *doc) bool { return id >= 100 && id < 200 }
		both := func(id int64, d *doc) bool { return isEnglish(id, d) && isRare(id, d) }

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			search := func() vector.Search[int64, doc] { return ins.Search(f.query) }
			fromRange := func() vector.Search[int64, doc] {
				it, err := f.docs.Instantiate(tx).Iterator(kvutil.From[int64](100).Until(200))
				require.NoError(t, err)
				return search().Among(it)
			}
			fromLang := func() vector.Search[int64, doc] {
				it, err := f.byLang.Instantiate(tx).Match("en")
				require.NoError(t, err)
				return search().Among(it)
			}

			for _, k := range []int{1, 10, 1000} {
				name := fmt.Sprintf("%v nearest %d", metric, k)
				f.verify(t, name, search(), keepAll, k, none)

				// Nearest is a search with a limit.
				nearest, err := ins.Nearest(f.query, k)
				require.NoError(t, err)
				limited, err := search().Limit(k).Run()
				require.NoError(t, err)
				require.Equal(t, drain(t, limited), drain(t, nearest), name)
			}
			f.verify(t, "no limit", search(), keepAll, -1, none)

			within := f.threshold(0.3)
			f.verify(t, "within", search(), keepAll, -1, within)
			f.verify(t, "within and limit", search(), keepAll, 5, within)
			f.verify(t, "within nothing", search(), keepAll, -1, f.oracle(keepAll)[0].dist-1)

			f.verify(t, "english", search().Where(english), isEnglish, -1, none)
			f.verify(t, "english limit", search().Where(english), isEnglish, 7, none)
			f.verify(t, "rare", search().Where(rare), isRare, -1, none)
			f.verify(t, "rare limit", search().Where(rare), isRare, 3, none)
			f.verify(t, "rare limit 1000", search().Where(rare), isRare, 1000, none)
			f.verify(t, "english, rare", search().Where(english, rare), both, -1, none)
			f.verify(t, "english then rare", search().Where(english).Where(rare), both, -1, none)
			f.verify(t, "where, limit and within", search().Where(english), isEnglish, 6, within)

			f.verify(t, "range", fromRange(), inRange, -1, none)
			f.verify(t, "range limit", fromRange(), inRange, 4, none)
			f.verify(t, "range within", fromRange(), inRange, -1, within)
			f.verify(t, "range where", fromRange().Where(rare), func(id int64, d *doc) bool {
				return inRange(id, d) && isRare(id, d)
			}, -1, none)
			f.verify(t, "lang", fromLang(), isEnglish, -1, none)
			f.verify(t, "lang limit", fromLang(), isEnglish, 9, none)
			f.verify(t, "lang within", fromLang(), isEnglish, 9, within)
			f.verify(t, "lang where limit", fromLang().Where(rare), both, 2, none)
			return nil
		}))
	})
}

// source is a source of records that counts how many times it is closed.
type source struct {
	iterutil.Iterator[int64, *doc]
	closed int
	err    error
}

func (s *source) Close() error {
	s.closed++
	return s.Iterator.Close()
}

// Err makes the source stop early with an error.
func (s *source) Err() error { return s.err }

func TestAmongClosesTheSource(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db, vector.Cosine)

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			open := func() *source {
				it, err := f.docs.Instantiate(tx).Iterator(kvutil.From[int64](100).Until(120))
				require.NoError(t, err)
				return &source{Iterator: it}
			}

			// On success.
			src := open()
			res, err := ins.Search(f.query).Among(src).Run()
			require.NoError(t, err)
			require.Equal(t, 1, src.closed)
			require.NotZero(t, res.Len())

			// On an error in the query.
			for name, q := range map[string][]float32{
				"dimensions": {1, 2},
				"nan":        {float32(math.NaN()), 0, 0, 0, 0, 0, 0, 0},
				"zero":       make([]float32, oracleDims),
			} {
				src = open()
				_, err = ins.Search(q).Among(src).Run()
				require.Error(t, err, name)
				require.Equal(t, 1, src.closed, name)
			}

			// On a negative limit, and on none.
			src = open()
			_, err = ins.Search(f.query).Among(src).Limit(-1).Run()
			require.Error(t, err)
			require.Equal(t, 1, src.closed)

			src = open()
			res, err = ins.Search(f.query).Among(src).Limit(0).Run()
			require.NoError(t, err)
			require.Zero(t, res.Len())
			require.Equal(t, 1, src.closed)

			// On an error of the source, which is returned.
			src = open()
			src.err = errors.New("boom")
			_, err = ins.Search(f.query).Among(src).Run()
			require.ErrorIs(t, err, src.err)
			require.Equal(t, 1, src.closed)

			// Another source replaces the one before, which is closed.
			first, second := open(), open()
			res, err = ins.Search(f.query).Among(first).Among(second).Run()
			require.NoError(t, err)
			require.Equal(t, 1, first.closed)
			require.Equal(t, 1, second.closed)
			require.NotZero(t, res.Len())
			return nil
		}))
	})
}

func TestQueryErrors(t *testing.T) {
	eachMetric(t, allMetrics, func(t *testing.T, db kv.DB, metric vector.Metric) {
		l := newLibrary(2, metric)
		l.ready(t, db)
		put(t, db, l.docs, map[int64]doc{1: {Embedding: []float32{1, 1}}})

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := l.x.Instantiate(tx)

			_, err := ins.Nearest([]float32{1}, 1)
			require.ErrorIs(t, err, vector.ErrDimensions)
			_, err = ins.Nearest(nil, 1)
			require.ErrorIs(t, err, vector.ErrDimensions)
			_, err = ins.Nearest([]float32{float32(math.NaN()), 1}, 1)
			require.ErrorIs(t, err, vector.ErrNotFinite)
			_, err = ins.Nearest([]float32{1, float32(math.Inf(-1))}, 1)
			require.ErrorIs(t, err, vector.ErrNotFinite)

			_, err = ins.Nearest([]float32{0, 0}, 1)
			if metric == vector.Cosine {
				require.ErrorIs(t, err, vector.ErrZeroVector)
			} else {
				require.NoError(t, err)
			}

			_, err = ins.Nearest([]float32{1, 1}, -1)
			require.Error(t, err)
			return nil
		}))
	})
}

func TestLimitZero(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db, vector.Cosine)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res, err := f.x.Instantiate(tx).Nearest(f.query, 0)
			require.NoError(t, err)
			require.False(t, res.Valid())
			require.Zero(t, res.Len())
			require.NoError(t, res.Close())
			return nil
		}))
	})
}

func TestTieOrder(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)

		// The same vector for several docs, written in no order, and one that is nearer.
		ids := []int64{1004, 1001, 1003, 1000, 1002}
		for _, id := range ids {
			put(t, db, l.docs, map[int64]doc{id: {Embedding: []float32{1, 1}}})
		}
		put(t, db, l.docs, map[int64]doc{5000: {Embedding: []float32{0, 0}}, 1: {Embedding: []float32{9, 9}}})

		require.Equal(t, []int64{5000, 1000, 1001, 1002, 1003, 1004, 1}, l.nearest(t, db, []float32{0, 0}, 10))
		// The tie decides who gets in the first ones: the keys that are less.
		require.Equal(t, []int64{5000, 1000, 1001}, l.nearest(t, db, []float32{0, 0}, 3))
		require.Equal(t, []int64{1000}, l.nearest(t, db, []float32{1, 1}, 1))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			res, err := l.x.Instantiate(tx).Search([]float32{0, 0}).
				Where(cond.Func("all", func(*doc) bool { return true })).Limit(4).Run()
			require.NoError(t, err)
			keys, err := iterutil.Of(res).Keys()
			require.NoError(t, err)
			require.Equal(t, []int64{5000, 1000, 1001, 1002}, keys)
			return nil
		}))
	})
}

func TestTieOrderOfASource(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)

		byID := map[int64]doc{}
		for id := int64(1); id <= 6; id++ {
			byID[id] = doc{Embedding: []float32{1, 1}}
		}
		put(t, db, l.docs, byID)

		// A source gives its records in an order of its own, here from the greatest key. Among
		// the same distances the keys that are less still get in, though they come last.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			src, err := l.docs.Instantiate(tx).ReverseIterator(kvutil.All[int64]())
			require.NoError(t, err)
			res, err := l.x.Instantiate(tx).Search([]float32{0, 0}).Among(src).Limit(3).Run()
			require.NoError(t, err)
			keys, err := iterutil.Of(res).Keys()
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2, 3}, keys)
			return nil
		}))
	})
}

func TestResultsCombineWithAnIndex(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db, vector.Euclidean)

		// The nearest 40 that are also english: the results are a source of a combination, like
		// the lookup of an index is.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res, err := f.x.Instantiate(tx).Nearest(f.query, 40)
			require.NoError(t, err)
			english, err := f.byLang.Instantiate(tx).Match("en")
			require.NoError(t, err)
			both, err := index.Intersect(res, english)
			require.NoError(t, err)
			got, err := iterutil.Of(both).Keys()
			require.NoError(t, err)

			var want []int64
			for _, c := range f.oracle(keepAll)[:40] {
				if f.byID[c.id].Lang == "en" {
					want = append(want, c.id)
				}
			}
			require.NotEmpty(t, want)
			require.ElementsMatch(t, want, got)
			return nil
		}))
	})
}

func TestWithinHasItsDistance(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)
		// Distances from the origin that are exact: 5 and 10.
		put(t, db, l.docs, map[int64]doc{1: {Embedding: []float32{3, 4}}, 2: {Embedding: []float32{6, 8}}})

		within := func(d float32) []int64 {
			var keys []int64
			require.NoError(t, db.View(func(tx kv.Tx) error {
				res, err := l.x.Instantiate(tx).Search([]float32{0, 0}).Within(d).Run()
				require.NoError(t, err)
				keys, err = iterutil.Of(res).Keys()
				return err
			}))
			return keys
		}
		require.Empty(t, within(4.99))
		require.Equal(t, []int64{1}, within(5))
		require.Equal(t, []int64{1}, within(9.99))
		require.Equal(t, []int64{1, 2}, within(10))
	})
}

func TestCosineDistanceIsNeverNegative(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db, vector.Cosine)

		// The inner product of a vector of length 1 with itself is 1 give or take the rounding,
		// and the distance of a doc from itself is 0 all the same.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			for id, d := range f.byID {
				if len(d.Embedding) == 0 {
					continue
				}
				res, err := ins.Nearest(d.Embedding, 1)
				require.NoError(t, err)
				require.True(t, res.Valid(), id)
				require.GreaterOrEqual(t, res.Distance(), float32(0), id)
				require.InDelta(t, 0, res.Distance(), 1e-6, id)
			}
			return nil
		}))
	})
}

func TestResults(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)
		put(t, db, l.docs, map[int64]doc{
			10: {Title: "origin", Embedding: []float32{0, 0}},
			20: {Title: "far", Embedding: []float32{3, 4}},
			30: {Title: "near", Embedding: []float32{1, 0}},
		})
		raw := func(id int64) []byte {
			b, err := l.docs.KeyCodec().Encode(id)
			require.NoError(t, err)
			return b
		}

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := l.x.Instantiate(tx)
			res, err := ins.Nearest([]float32{0, 0}, 2)
			require.NoError(t, err)

			require.Equal(t, 2, res.Len())
			require.True(t, res.Valid())
			contains := func(id int64) bool {
				ok, err := res.Contains(raw(id), nil)
				require.NoError(t, err)
				return ok
			}

			// Wherever the iterator is, the results are the same ones.
			require.True(t, contains(10))
			require.False(t, contains(20))
			require.True(t, contains(30))

			key, err := res.Key()
			require.NoError(t, err)
			require.Equal(t, int64(10), key)
			rawKey, err := res.RawKey()
			require.NoError(t, err)
			require.Equal(t, raw(10), rawKey)
			require.InDelta(t, 0, res.Distance(), 1e-6)
			first, err := res.Value()
			require.NoError(t, err)
			require.Equal(t, "origin", first.Title)
			again, err := res.Value()
			require.NoError(t, err)
			require.Same(t, first, again)

			res.Next()
			require.True(t, res.Valid())
			key, err = res.Key()
			require.NoError(t, err)
			require.Equal(t, int64(30), key)
			require.InDelta(t, 1, res.Distance(), 1e-6)
			require.True(t, contains(10))
			require.False(t, contains(20))

			res.Next()
			require.False(t, res.Valid())
			require.Equal(t, 2, res.Len())
			require.True(t, contains(30))
			res.Next()
			require.False(t, res.Valid())
			_, err = res.Key()
			require.Error(t, err)

			// A closed one still knows what it found.
			res, err = ins.Nearest([]float32{0, 0}, 2)
			require.NoError(t, err)
			require.NoError(t, res.Close())
			require.False(t, res.Valid())
			require.Equal(t, 2, res.Len())
			require.True(t, contains(10))
			require.True(t, contains(30))
			require.False(t, contains(20))
			require.NoError(t, res.Close())

			// It is an iterator like any other.
			res, err = ins.Search([]float32{0, 0}).Run()
			require.NoError(t, err)
			titles, err := iterutil.Of(res).Map(func(_ int64, d *doc) (string, error) { return d.Title, nil }).Values()
			require.NoError(t, err)
			require.Equal(t, []string{"origin", "near", "far"}, titles)

			res, err = ins.Search([]float32{0, 0}).Run()
			require.NoError(t, err)
			var (
				keys    []int64
				loopErr error
			)
			for key, d := range iterutil.Of(res).All(&loopErr) {
				keys = append(keys, key)
				require.NotEmpty(t, d.Title)
			}
			require.NoError(t, loopErr)
			require.Equal(t, []int64{10, 30, 20}, keys)
			require.False(t, res.Valid(), "the loop closed the results")

			res, err = ins.Search([]float32{0, 0}).Run()
			require.NoError(t, err)
			keys, err = iterutil.Of(res).Keys()
			require.NoError(t, err)
			require.Equal(t, []int64{10, 30, 20}, keys)
			return nil
		}))
	})
}

func TestSearchIsAValue(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db, vector.Cosine)

		english := cond.Func("english", func(d *doc) bool { return d.Lang == "en" })
		rare := cond.Func("rare", func(d *doc) bool { return d.Title == "rare" })
		french := cond.Func("french", func(d *doc) bool { return d.Lang == "fr" })
		isEnglish := func(_ int64, d *doc) bool { return d.Lang == "en" }
		isRare := func(_ int64, d *doc) bool { return d.Title == "rare" }

		require.NoError(t, db.View(func(tx kv.Tx) error {
			base := f.x.Instantiate(tx).Search(f.query).Where(english)
			a := base.Where(rare)
			b := base.Where(french)
			limited := base.Limit(3)
			within := base.Within(0.5)

			f.verify(t, "a", a, func(id int64, d *doc) bool { return isEnglish(id, d) && isRare(id, d) }, -1, math.NaN())
			f.verify(t, "b", b, func(int64, *doc) bool { return false }, -1, math.NaN())
			f.verify(t, "base", base, isEnglish, -1, math.NaN())
			f.verify(t, "limited", limited, isEnglish, 3, math.NaN())
			f.verify(t, "within", within, isEnglish, -1, 0.5)
			f.verify(t, "base again", base, isEnglish, -1, math.NaN())
			return nil
		}))
	})
}

func TestSearchInTheTransactionOfTheWrites(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Cosine)
		l.ready(t, db)
		put(t, db, l.docs, map[int64]doc{
			1: {Title: "east", Embedding: []float32{1, 0}},
			2: {Title: "north", Embedding: []float32{0, 1}},
		})

		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := l.docs.Instantiate(tx)
			ins := l.x.Instantiate(tx)
			first := func(q []float32) (int64, string) {
				res, err := ins.Nearest(q, 1)
				require.NoError(t, err)
				require.Equal(t, 1, res.Len())
				key, err := res.Key()
				require.NoError(t, err)
				d, err := res.Value()
				require.NoError(t, err)
				return key, d.Title
			}

			key, _ := first([]float32{1, 0.1})
			require.Equal(t, int64(1), key)

			// A write is seen by the next search, and so is the record that was written.
			require.NoError(t, store.Set(3, &doc{Title: "northeast", Embedding: []float32{1, 0.1}}))
			key, title := first([]float32{1, 0.1})
			require.Equal(t, int64(3), key)
			require.Equal(t, "northeast", title)

			require.NoError(t, store.Delete(3))
			key, _ = first([]float32{1, 0.1})
			require.Equal(t, int64(1), key)

			require.NoError(t, store.Set(1, &doc{Title: "south", Embedding: []float32{0, -1}}))
			key, _ = first([]float32{1, 0.1})
			require.Equal(t, int64(2), key)
			return nil
		}))
	})
}
