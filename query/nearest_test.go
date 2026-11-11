package query_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
)

// Article is a record with an embedding, which some of the records do not have.
type Article struct {
	Title     string
	Lang      string
	Year      int
	Embedding []float32
}

const (
	nearDims = 16
	nearDocs = 3000
)

var (
	artTitle = field.Path[Article, string]("Title")
	artLang  = field.Path[Article, string]("Lang")
	artYear  = field.Path[Article, int]("Year")
)

// nearEnv is a store of articles with indexes on the language and the year and a vector index of
// the embeddings, and the oracle that knows how near the articles are.
type nearEnv struct {
	db      kv.DB
	store   *extstore.Store[int64, Article]
	vx      *vector.Index[int64, Article]
	metric  vector.Metric
	byID    map[int64]Article
	centres [][]float32
	rng     *rand.Rand
}

// newNearEnv defines the store and writes the articles around a few centres: some of them without
// a vector, and some with the vector of the one before them, so that there are ties. An index that
// is partitioned is built once the articles are written.
func newNearEnv(t *testing.T, db kv.DB, metric vector.Metric, opts ...vector.Option) *nearEnv {
	t.Helper()

	e := &nearEnv{
		db: db, store: extstore.New[int64, Article]("articles"), metric: metric,
		byID: map[int64]Article{}, rng: rand.New(rand.NewPCG(5, 6)),
	}
	e.vx = vector.On(e.store, "by_embedding", nearDims, metric, func(a *Article) []float32 { return a.Embedding }, opts...)
	index.On(e.store, "by_lang", artLang)
	index.On(e.store, "by_year", artYear)
	require.NoError(t, e.store.Check(db))

	for range 20 {
		centre := make([]float32, nearDims)
		for i := range centre {
			centre[i] = float32(e.rng.NormFloat64())
		}
		e.centres = append(e.centres, centre)
	}
	langs := []string{"en", "fr", "de"}
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		store := e.store.Instantiate(tx)
		for id := int64(1); id <= nearDocs; id++ {
			a := Article{Title: fmt.Sprint("article-", id), Lang: langs[id%3], Year: 1990 + e.rng.IntN(30)}
			if id%100 == 0 {
				a.Title = "rare"
			}
			switch {
			case id%97 == 0:
			case id%50 == 0 && len(e.byID[id-1].Embedding) > 0:
				a.Embedding = slices.Clone(e.byID[id-1].Embedding)
			default:
				a.Embedding = e.draw()
			}
			e.byID[id] = a
			if err := store.Set(id, &a); err != nil {
				return err
			}
		}
		return nil
	}))
	if e.vx.PartitionSize() > 0 {
		require.NoError(t, vector.Rebuild(context.Background(), db, e.vx))
	}
	return e
}

// draw returns a vector around one of the centres.
func (e *nearEnv) draw() []float32 {
	centre := e.centres[e.rng.IntN(len(e.centres))]
	v := make([]float32, len(centre))
	for i := range v {
		v[i] = centre[i] + float32(e.rng.NormFloat64()*0.3)
	}
	return v
}

// distance measures the distance of an article from a vector in float64.
func (e *nearEnv) distance(q []float32, id int64) float64 {
	var dot, nq, nv, sq float64
	for i, c := range e.byID[id].Embedding {
		x, y := float64(q[i]), float64(c)
		dot += x * y
		nq += x * x
		nv += y * y
		sq += (x - y) * (x - y)
	}
	if e.metric == vector.Cosine {
		return 1 - dot/math.Sqrt(nq*nv)
	}
	return math.Sqrt(sq)
}

// candidate is an article and its distance from the query as the oracle has it.
type candidate struct {
	id   int64
	dist float64
}

// oracle returns the articles that have a vector and satisfy the conditions, nearest first.
func (e *nearEnv) oracle(t *testing.T, q []float32, conds []cond.Cond[Article]) []candidate {
	t.Helper()

	var all []candidate
	for id, a := range e.byID {
		if len(a.Embedding) == 0 || !satisfies(t, conds, a) {
			continue
		}
		all = append(all, candidate{id, e.distance(q, id)})
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

// satisfies reports whether an article satisfies all the conditions.
func satisfies(t *testing.T, conds []cond.Cond[Article], a Article) bool {
	t.Helper()

	for _, c := range conds {
		ok, err := c.Match(&a)
		require.NoError(t, err)
		if !ok {
			return false
		}
	}
	return true
}

// nearHit is an article that a query gave, and its distance.
type nearHit struct {
	id   int64
	dist float32
}

// hits runs the query, and returns the articles with their distances, which must all be there. The
// number of articles that Count gives must be the number of them.
func (e *nearEnv) hits(t *testing.T, q *query.Query[int64, Article]) []nearHit {
	t.Helper()

	var hits []nearHit
	require.NoError(t, e.db.View(func(tx kv.Tx) error {
		it := q.Run(tx)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			key, err := it.Key()
			require.NoError(t, err)
			a, err := it.Value()
			require.NoError(t, err)
			require.Equal(t, e.byID[key], *a)

			d, ok := query.Distance(it)
			require.True(t, ok, "the distance of %d", key)
			hits = append(hits, nearHit{key, d})
		}
		if err := it.Err(); err != nil {
			return err
		}

		_, ok := query.Distance(it)
		require.False(t, ok, "the iterator is not at a record")
		count, err := q.Count(tx)
		require.NoError(t, err)
		require.Equal(t, len(hits), count, "Count is the number of records that Run gives")
		return nil
	}))
	return hits
}

// requireOracle checks that the articles of a query are the ones that the oracle says, from the
// offset on and as many as the limit (all if it is negative). The articles are compared by their
// distances, as rounding in float32 may put articles that are nearly as near in another order.
func (e *nearEnv) requireOracle(
	t *testing.T, name string, q []float32, conds []cond.Cond[Article], offset, limit int, got []nearHit,
) {
	t.Helper()

	want := e.oracle(t, q, conds)
	want = want[min(offset, len(want)):]
	if limit >= 0 {
		want = want[:min(limit, len(want))]
	}
	require.Len(t, got, len(want), name)

	seen := map[int64]bool{}
	for i, h := range got {
		a, ok := e.byID[h.id]
		require.True(t, ok, name)
		require.NotEmpty(t, a.Embedding, "%s: %d has no vector", name, h.id)
		require.True(t, satisfies(t, conds, a), "%s: %d is not wanted", name, h.id)
		require.False(t, seen[h.id], "%s: %d comes twice", name, h.id)
		seen[h.id] = true

		require.InDelta(t, e.distance(q, h.id), h.dist, 1e-4, "%s: the distance of %d", name, h.id)
		require.InDelta(t, want[i].dist, e.distance(q, h.id), 1e-4, "%s: the distance at %d", name, i)
		if i > 0 {
			require.LessOrEqual(t, got[i-1].dist, h.dist, "%s: order at %d", name, i)
			// The same vector is at the same distance, so the keys are what orders them.
			if slices.Equal(e.byID[got[i-1].id].Embedding, a.Embedding) {
				require.Less(t, got[i-1].id, h.id, "%s: tie at %d", name, i)
			}
		}
	}
}

// recall returns the fraction of the articles that the oracle says that the query gave.
func (e *nearEnv) recall(t *testing.T, q []float32, conds []cond.Cond[Article], offset, limit int, got []nearHit) float64 {
	t.Helper()

	want := e.oracle(t, q, conds)
	want = want[min(offset, len(want)):]
	want = want[:min(limit, len(want))]
	if len(want) == 0 {
		return 1
	}
	n := 0
	for _, w := range want {
		if slices.ContainsFunc(got, func(h nearHit) bool { return h.id == w.id }) {
			n++
		}
	}
	return float64(n) / float64(len(want))
}

// nearCase is a query with Nearest, the plan it must have, and what it must give.
type nearCase struct {
	name  string
	where []cond.Cond[Article]
	hint  func(*query.Query[int64, Article]) *query.Query[int64, Article]
	// offset and limit are the page, with no limit if it is negative.
	offset, limit int
	// post is set if the search checks the conditions, which finds what is nearly the nearest in an
	// index that is partitioned, and not exactly the nearest.
	post bool
	// explain is the plan of an index that is not partitioned; "(exact)" is what the others replace.
	explain string
}

// query returns the query of the case, with a nearness to a vector.
func (c nearCase) query(e *nearEnv, near query.Nearness[int64, Article]) *query.Query[int64, Article] {
	q := query.From(e.store).Where(c.where...).Nearest(near)
	if c.hint != nil {
		q = c.hint(q)
	}
	if c.offset > 0 {
		q = q.Offset(c.offset)
	}
	if c.limit >= 0 {
		q = q.Limit(c.limit)
	}
	return q
}

func nearCases() []nearCase {
	en := artLang.Eq("en")
	recent := artYear.Gte(2005)
	rare := artTitle.Eq("rare")

	return []nearCase{
		{
			name: "no conditions", limit: 10, post: true,
			explain: "Limit 10\n└─ Nearest by_embedding (exact)",
		},
		{
			name: "no conditions, no limit", limit: -1, post: true,
			explain: "Nearest by_embedding (exact)",
		},
		{
			name: "equality on an indexed field", where: []cond.Cond[Article]{en}, limit: 10,
			explain: "Limit 10\n└─ Nearest by_embedding (exact)\n   └─ IndexLookup by_lang: 1 range",
		},
		{
			name: "range on an indexed field", where: []cond.Cond[Article]{recent}, limit: 10, post: true,
			explain: "Limit 10\n└─ Nearest by_embedding (exact), then check [Year >= 2005]",
		},
		{
			name: "equality and range", where: []cond.Cond[Article]{en, recent}, limit: 10,
			explain: "Limit 10\n└─ Nearest by_embedding (exact)\n   └─ IndexLookup by_lang: 1 range, then check [Year >= 2005]",
		},
		{
			name: "offset and limit", where: []cond.Cond[Article]{en}, offset: 5, limit: 10,
			explain: "Limit 10, offset 5\n└─ Nearest by_embedding (exact)\n   └─ IndexLookup by_lang: 1 range",
		},
		{
			name: "offset and limit, checked by the search", where: []cond.Cond[Article]{recent}, offset: 7, limit: 10, post: true,
			explain: "Limit 10, offset 7\n└─ Nearest by_embedding (exact), then check [Year >= 2005]",
		},
		{
			name: "offset only", where: []cond.Cond[Article]{en}, offset: 1000, limit: -1,
			explain: "Offset 1000\n└─ Nearest by_embedding (exact)\n   └─ IndexLookup by_lang: 1 range",
		},
		{
			name: "no limit", where: []cond.Cond[Article]{en}, limit: -1,
			explain: "Nearest by_embedding (exact)\n└─ IndexLookup by_lang: 1 range",
		},
		{
			name: "no limit, checked by the search", where: []cond.Cond[Article]{recent}, limit: -1, post: true,
			explain: "Nearest by_embedding (exact), then check [Year >= 2005]",
		},
		{
			name: "condition of no index", where: []cond.Cond[Article]{rare}, limit: 10, post: true,
			explain: "Limit 10\n└─ Nearest by_embedding (exact), then check [Title = \"rare\"]",
		},
		{
			name: "full scan", where: []cond.Cond[Article]{recent}, limit: 10,
			hint:    (*query.Query[int64, Article]).FullScan,
			explain: "Limit 10\n└─ Nearest by_embedding (exact)\n   └─ StoreScan, then check [Year >= 2005]",
		},
		{
			name: "full scan, no conditions", limit: 10,
			hint:    (*query.Query[int64, Article]).FullScan,
			explain: "Limit 10\n└─ Nearest by_embedding (exact)\n   └─ StoreScan",
		},
		{
			name: "using the index of the condition", where: []cond.Cond[Article]{en, recent}, limit: 10,
			hint:    func(q *query.Query[int64, Article]) *query.Query[int64, Article] { return q.Using("by_lang") },
			explain: "Limit 10\n└─ Nearest by_embedding (exact)\n   └─ IndexLookup by_lang: 1 range, then check [Year >= 2005]",
		},
		{
			name: "using another index than the one of the cheapest plan", where: []cond.Cond[Article]{recent}, limit: 10,
			hint:    func(q *query.Query[int64, Article]) *query.Query[int64, Article] { return q.Using("by_year") },
			explain: "Limit 10\n└─ Nearest by_embedding (exact)\n   └─ IndexLookup by_year: 1 range",
		},
		{
			name: "using the vector index", where: []cond.Cond[Article]{en}, limit: 10, post: true,
			hint:    func(q *query.Query[int64, Article]) *query.Query[int64, Article] { return q.Using("by_embedding") },
			explain: "Limit 10\n└─ Nearest by_embedding (exact), then check [Lang = \"en\"]",
		},
	}
}

func TestNearestOracle(t *testing.T) {
	for _, metric := range []vector.Metric{vector.Cosine, vector.Euclidean} {
		t.Run(metric.String(), func(t *testing.T) {
			testkv.Each(t, func(t *testing.T, db kv.DB) {
				// Without partitions the search that checks the conditions is exact too.
				e := newNearEnv(t, db, metric)
				for _, c := range nearCases() {
					t.Run(c.name, func(t *testing.T) {
						for range 3 {
							qv := e.draw()
							q := c.query(e, e.vx.To(qv))

							explain, err := q.Explain()
							require.NoError(t, err)
							require.Equal(t, c.explain, explain)

							got := e.hits(t, q)
							e.requireOracle(t, c.name, qv, c.where, c.offset, c.limit, got)
						}
					})
				}
			})
		})
	}
}

// nearVariants are the vector indexes whose searches are not exact.
var nearVariants = map[string]struct {
	opts     []vector.Option
	describe string
}{
	"partitioned": {[]vector.Option{vector.Partitioned(100)}, "partitioned"},
	"quantized":   {[]vector.Option{vector.Partitioned(100), vector.Quantized()}, "partitioned, quantized"},
}

func TestNearestPartitioned(t *testing.T) {
	for name, variant := range nearVariants {
		for _, metric := range []vector.Metric{vector.Cosine, vector.Euclidean} {
			t.Run(name+"/"+metric.String(), func(t *testing.T) {
				testkv.Each(t, func(t *testing.T, db kv.DB) {
					e := newNearEnv(t, db, metric, variant.opts...)

					for _, c := range nearCases() {
						t.Run(c.name, func(t *testing.T) {
							explain := strings.ReplaceAll(c.explain, "(exact)", "("+variant.describe+")")

							// The search that checks the conditions reads the nearest partitions only. It
							// finds nearly what the oracle does, and as many as there are.
							var recall float64
							const queries = 8
							for range queries {
								qv := e.draw()
								q := c.query(e, e.vx.To(qv))
								got, err := q.Explain()
								require.NoError(t, err)
								require.Equal(t, explain, got)

								if !c.post {
									// Every record that the access path finds is ranked: it is exact.
									hits := e.hits(t, q)
									e.requireOracle(t, c.name, qv, c.where, c.offset, c.limit, hits)
									continue
								}
								if c.limit < 0 {
									// The records of the nearest partitions are all it gives.
									_ = e.hits(t, q)
									continue
								}

								hits := e.hits(t, q)
								want := len(e.oracle(t, qv, c.where))
								require.Len(t, hits, min(c.limit, max(want-c.offset, 0)), "the number of records")
								for _, h := range hits {
									a := e.byID[h.id]
									require.True(t, satisfies(t, c.where, a))
									require.NotEmpty(t, a.Embedding)
									require.InDelta(t, e.distance(qv, h.id), h.dist, 1e-4)
								}
								recall += e.recall(t, qv, c.where, c.offset, c.limit, hits)
							}
							if c.post && c.limit >= 0 {
								t.Logf("recall@%d %.3f", c.limit, recall/queries)
								require.GreaterOrEqual(t, recall/queries, 0.9, "recall")
							}
						})
					}
				})
			})
		}
	}
}

func TestNearestRecordsWithoutVector(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Euclidean)
		withVector := 0
		for _, a := range e.byID {
			if len(a.Embedding) > 0 {
				withVector++
			}
		}
		require.Less(t, withVector, nearDocs)

		q := query.From(e.store).Nearest(e.vx.To(e.draw()))
		require.Len(t, e.hits(t, q), withVector)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			count, err := q.Count(tx)
			require.Equal(t, withVector, count)
			return err
		}))

		// With a condition on the language, and the page of the count.
		for id := int64(97); id < nearDocs; id += 97 {
			require.Empty(t, e.byID[id].Embedding)
		}
		en := query.From(e.store).Where(artLang.Eq("en")).Nearest(e.vx.To(e.draw()))
		wantEnglish := 0
		for _, a := range e.byID {
			if a.Lang == "en" && len(a.Embedding) > 0 {
				wantEnglish++
			}
		}
		require.Len(t, e.hits(t, en), wantEnglish)
		require.Len(t, e.hits(t, en.Offset(wantEnglish-3).Limit(10)), 3)
	})
}

func TestNearestDistances(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Cosine)
		qv := e.draw()
		en := query.From(e.store).Where(artLang.Eq("en")).Nearest(e.vx.To(qv))

		require.NoError(t, db.View(func(tx kv.Tx) error {
			// The distances of Run and the ones of the rows are the same, and not decreasing.
			ranked := en.Offset(3).Limit(12)
			it := ranked.Run(tx)
			defer it.Close()
			var keys []int64
			var dists []float32
			for ; it.Valid(); it.Next() {
				key, err := it.Key()
				require.NoError(t, err)
				d, ok := query.Distance(it)
				require.True(t, ok)
				keys, dists = append(keys, key), append(dists, d)
			}
			require.NoError(t, it.Err())
			require.Len(t, keys, 12)
			require.True(t, slices.IsSorted(dists))
			require.Greater(t, dists[len(dists)-1], dists[0])

			// The Iterator inside is as good as the AdvancedIterator.
			again := ranked.Run(tx)
			defer again.Close()
			d, ok := query.Distance(again.Iterator)
			require.True(t, ok)
			require.Equal(t, dists[0], d)

			rows := ranked.Select(artTitle).Run(tx)
			defer rows.Close()
			for i := 0; rows.Valid(); i++ {
				key, err := rows.Key()
				require.NoError(t, err)
				row, err := rows.Value()
				require.NoError(t, err)
				require.Equal(t, keys[i], key)
				require.Equal(t, keys[i], row.Key())

				d, ok := row.Distance()
				require.True(t, ok)
				require.Equal(t, dists[i], d)
				title, ok := row.Get(artTitle)
				require.True(t, ok)
				require.Equal(t, e.byID[key].Title, title)
				record, err := row.Record()
				require.NoError(t, err)
				require.Equal(t, e.byID[key], *record)
				rows.Next()
			}
			require.NoError(t, rows.Err())

			// The rows of a query with its records in an order of fields have no distance, and neither
			// does a query that is not ordered by one.
			plain := query.From(e.store).Where(artLang.Eq("en")).OrderBy(artYear).Limit(3)
			plainRows := plain.Select().Run(tx)
			defer plainRows.Close()
			require.True(t, plainRows.Valid())
			row, err := plainRows.Value()
			require.NoError(t, err)
			_, ok = row.Distance()
			require.False(t, ok)

			for _, q := range []*query.Query[int64, Article]{
				plain, query.From(e.store).Limit(3), query.From(e.store).Where(artLang.Eq("en")).Limit(3),
			} {
				it := q.Run(tx)
				for ; it.Valid(); it.Next() {
					_, ok := query.Distance(it)
					require.False(t, ok)
				}
				require.NoError(t, it.Err())
				require.NoError(t, it.Close())
			}

			// Nor does an iterator that is not of a query.
			_, ok = query.Distance[int64, Article](nil)
			require.False(t, ok)
			return nil
		}))
	})
}

func TestNearestOrder(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Cosine)
		near := e.vx.To(e.draw())
		base := query.From(e.store).Where(artLang.Eq("en"))

		// OrderBy after Nearest replaces it, and so does Nearest after OrderBy.
		for name, q := range map[string]*query.Query[int64, Article]{
			"order after":   base.Nearest(near).OrderBy(artYear).Limit(5),
			"nearest after": base.OrderBy(artYear).Nearest(near).Limit(5),
		} {
			explain, err := q.Explain()
			require.NoError(t, err, name)
			if name == "order after" {
				require.NotContains(t, explain, "Nearest", name)
				require.Contains(t, explain, "Year", name)
			} else {
				require.Contains(t, explain, "Nearest by_embedding", name)
				require.NotContains(t, explain, "Sort", name)
				require.NotContains(t, explain, "TopN", name)
			}
		}
		require.NoError(t, db.View(func(tx kv.Tx) error {
			keys, err := base.Nearest(near).OrderBy(artYear).Limit(5).Run(tx).Keys()
			require.NoError(t, err)
			require.Len(t, keys, 5)
			years := make([]int, len(keys))
			for i, k := range keys {
				years[i] = e.byID[k].Year
			}
			require.True(t, slices.IsSorted(years))
			return nil
		}))
	})
}

func TestNearestHints(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Cosine)
		q := query.From(e.store).Where(artLang.Eq("en"), artYear.Gte(2000)).Nearest(e.vx.To(e.draw()))

		// UsingAll reads several indexes, which a search ranks one set of records of.
		_, err := q.UsingAll("by_lang", "by_year").Explain()
		require.ErrorIs(t, err, query.ErrHint)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := q.UsingAll("by_lang", "by_year").Run(tx)
			defer it.Close()
			require.False(t, it.Valid())
			require.ErrorIs(t, it.Err(), query.ErrHint)
			return nil
		}))

		// A hint of an index that does not exist, or cannot serve the query, is the error that it is
		// without Nearest.
		_, err = q.Using("by_nothing").Explain()
		require.ErrorIs(t, err, query.ErrHint)
		_, err = query.From(e.store).Where(artLang.Eq("en")).Nearest(e.vx.To(e.draw())).Using("by_year").Explain()
		require.NoError(t, err, "an index read in full serves a query when it has an entry for every article")
	})
}

func TestNearestEdges(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Cosine)
		near := e.vx.To(e.draw())
		base := query.From(e.store).Nearest(near)

		// Nothing is read where nothing can be there.
		for name, q := range map[string]*query.Query[int64, Article]{
			"limit 0":       base.Limit(0),
			"contradiction": base.Where(artLang.Eq("en"), artLang.Eq("fr")).Limit(5),
		} {
			explain, err := q.Explain()
			require.NoError(t, err, name)
			require.Equal(t, "Empty", explain, name)
			require.Empty(t, e.hits(t, q), name)
		}

		_, err := base.Offset(-1).Explain()
		require.ErrorIs(t, err, query.ErrNegative)
		_, err = base.Limit(-1).Explain()
		require.ErrorIs(t, err, query.ErrNegative)
		_, err = base.MaxKeys(-1).Explain()
		require.ErrorIs(t, err, query.ErrNegative)

		// An offset past the end gives nothing.
		require.Empty(t, e.hits(t, base.Offset(nearDocs*2).Limit(5)))
		require.Empty(t, e.hits(t, base.Where(artLang.Eq("en")).Offset(nearDocs*2)))

		// A vector that does not suit the index is an error of the run.
		bad := query.From(e.store).Where(artLang.Eq("en")).Nearest(e.vx.To([]float32{1, 2}))
		require.NoError(t, db.View(func(tx kv.Tx) error {
			for _, q := range []*query.Query[int64, Article]{bad, bad.Using("by_embedding")} {
				it := q.Run(tx)
				require.False(t, it.Valid())
				require.Error(t, it.Err())
				require.NoError(t, it.Close())
				_, err := q.Count(tx)
				require.Error(t, err)
			}
			return nil
		}))

		require.Panics(t, func() { query.From(e.store).Nearest(nil) })
	})
}

// undescribed is a nearness that says nothing about how it searches.
type undescribed struct {
	vector.Near[int64, Article]
}

func (undescribed) Describe() string { return "" }

// plain is a nearness whose results have no distance.
type plain struct {
	vector.Near[int64, Article]
}

func (p plain) Nearest(
	tx kv.Tx, among iterutil.Iterator[int64, *Article], conds []cond.Cond[Article], limit int,
) (iterutil.Iterator[int64, *Article], error) {
	it, err := p.Near.Nearest(tx, among, conds, limit)
	if err != nil {
		return nil, err
	}
	return struct {
		iterutil.Iterator[int64, *Article]
	}{it}, nil
}

func TestNearestOfOtherNearness(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Cosine)
		qv := e.draw()

		q := query.From(e.store).Where(artLang.Eq("en")).Nearest(undescribed{e.vx.To(qv)}).Limit(5)
		explain, err := q.Explain()
		require.NoError(t, err)
		require.Equal(t, "Limit 5\n└─ Nearest by_embedding\n   └─ IndexLookup by_lang: 1 range", explain)
		e.requireOracle(t, "undescribed", qv, []cond.Cond[Article]{artLang.Eq("en")}, 0, 5, e.hits(t, q))

		// The distance is given if the results have it.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			it := query.From(e.store).Nearest(plain{e.vx.To(qv)}).Limit(5).Run(tx)
			defer it.Close()
			require.True(t, it.Valid())
			_, ok := query.Distance(it)
			require.False(t, ok)
			return it.Err()
		}))

		// The options of the nearness are in the plan, and are followed.
		opts := query.From(e.store).Nearest(e.vx.To(qv).Probes(2).Within(0.5)).Limit(5)
		explain, err = opts.Explain()
		require.NoError(t, err)
		require.Equal(t, "Limit 5\n└─ Nearest by_embedding (exact, within 0.5)", explain)
		for _, h := range e.hits(t, opts) {
			require.LessOrEqual(t, h.dist, float32(0.5))
		}
	})
}

func TestNearestIsADefinition(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		e := newNearEnv(t, db, vector.Euclidean)
		q1, q2 := e.draw(), e.draw()
		base := query.From(e.store).Where(artLang.Eq("en"))

		a := base.Nearest(e.vx.To(q1)).Limit(5)
		before := e.hits(t, a)
		explain, err := a.Explain()
		require.NoError(t, err)

		// One query is run in two transactions, and gives the same.
		require.Equal(t, before, e.hits(t, a))
		var inTx [2][]int64
		for i := range inTx {
			require.NoError(t, db.View(func(tx kv.Tx) (err error) {
				inTx[i], err = a.Run(tx).Keys()
				return err
			}))
		}
		require.Equal(t, inTx[0], inTx[1])

		// Queries that are derived from one do not interfere with it or each other.
		b := base.Nearest(e.vx.To(q2)).Limit(5)
		c := a.Limit(3)
		d := a.OrderBy(artYear)
		e.requireOracle(t, "b", q2, []cond.Cond[Article]{artLang.Eq("en")}, 0, 5, e.hits(t, b))
		e.requireOracle(t, "c", q1, []cond.Cond[Article]{artLang.Eq("en")}, 0, 3, e.hits(t, c))
		require.Equal(t, before, e.hits(t, a))
		explainAgain, err := a.Explain()
		require.NoError(t, err)
		require.Equal(t, explain, explainAgain)
		explainD, err := d.Explain()
		require.NoError(t, err)
		require.NotContains(t, explainD, "Nearest")
		explainBase, err := base.Explain()
		require.NoError(t, err)
		require.NotContains(t, explainBase, "Nearest")
		require.Len(t, e.hits(t, a), 5)

		// Run at once from several goroutines.
		errs := make(chan error, 8)
		for range cap(errs) {
			go func() {
				errs <- db.View(func(tx kv.Tx) error {
					keys, err := a.Run(tx).Keys()
					if err == nil && len(keys) != 5 {
						err = errors.New("not five keys")
					}
					return err
				})
			}()
		}
		for range cap(errs) {
			require.NoError(t, <-errs)
		}
	})
}
