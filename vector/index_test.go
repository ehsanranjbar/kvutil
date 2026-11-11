package vector_test

import (
	"context"
	"math"
	"testing"

	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
	"github.com/stretchr/testify/require"
)

// doc is a record with an embedding.
type doc struct {
	Title     string
	Lang      string
	Embedding []float32
}

// The results are iterators of keyed sources, like the lookups of an index.
var _ index.Keyed[int64, doc] = (*vector.Results[int64, doc])(nil)

func embedding(d *doc) []float32 { return d.Embedding }

// library is a store of docs, with a vector index if it is defined with one.
type library struct {
	docs *extstore.Store[int64, doc]
	x    *vector.Index[int64, doc]
}

// newLibrary defines the store of docs and an index on their embeddings.
func newLibrary(dims int, metric vector.Metric, opts ...vector.Option) *library {
	docs := extstore.New[int64, doc]("docs")
	return &library{docs: docs, x: vector.On(docs, "by_embedding", dims, metric, embedding, opts...)}
}

// allMetrics are the metrics that the index measures distances by.
var allMetrics = []vector.Metric{vector.Cosine, vector.Euclidean, vector.Dot}

// eachMetric runs the test on a new DB of every engine, for each of the metrics.
func eachMetric(t *testing.T, metrics []vector.Metric, test func(t *testing.T, db kv.DB, metric vector.Metric)) {
	for _, metric := range metrics {
		t.Run(metric.String(), func(t *testing.T) {
			testkv.Each(t, func(t *testing.T, db kv.DB) { test(t, db, metric) })
		})
	}
}

// ready checks a new database against the definition, which is how a program starts.
func (l *library) ready(t *testing.T, db kv.DB) {
	t.Helper()
	require.NoError(t, l.docs.Check(db))
}

// put sets docs in a transaction of their own.
func put(t *testing.T, db kv.DB, docs *extstore.Store[int64, doc], byID map[int64]doc) {
	t.Helper()

	require.NoError(t, db.Update(func(tx kv.Tx) error {
		store := docs.Instantiate(tx)
		for id, d := range byID {
			if err := store.Set(id, &d); err != nil {
				return err
			}
		}
		return nil
	}))
}

// trySet sets a doc and returns the error, committing the transaction whatever it is.
func trySet(t *testing.T, db kv.DB, docs *extstore.Store[int64, doc], id int64, d doc) error {
	t.Helper()

	var setErr error
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		setErr = docs.Instantiate(tx).Set(id, &d)
		return nil
	}))
	return setErr
}

// del deletes a doc in a transaction of its own.
func del(t *testing.T, db kv.DB, docs *extstore.Store[int64, doc], id int64) {
	t.Helper()
	require.NoError(t, db.Update(func(tx kv.Tx) error { return docs.Instantiate(tx).Delete(id) }))
}

// entries returns the number of keys under the prefix of the index.
func entries[K, V any](t *testing.T, db kv.DB, x *vector.Index[K, V]) int {
	t.Helper()

	n := 0
	require.NoError(t, db.View(func(tx kv.Tx) error {
		p := x.Prefix()
		it, err := tx.Iterator(p, kv.PrefixEnd(p))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			n++
		}
		return nil
	}))
	return n
}

// nearest returns the keys of the k nearest docs to the query.
func (l *library) nearest(t *testing.T, db kv.DB, q []float32, k int) []int64 {
	t.Helper()

	var keys []int64
	require.NoError(t, db.View(func(tx kv.Tx) error {
		res, err := l.x.Instantiate(tx).Nearest(q, k)
		if err != nil {
			return err
		}
		keys, err = iterutil.Of(res).Keys()
		return err
	}))
	return keys
}

// vectorOf returns the vector that the index has for a doc.
func (l *library) vectorOf(t *testing.T, db kv.DB, id int64) ([]float32, error) {
	t.Helper()

	var v []float32
	err := db.View(func(tx kv.Tx) (err error) {
		v, err = l.x.Instantiate(tx).Vector(id)
		return err
	})
	return v, err
}

func TestMaintenance(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Cosine)
		l.ready(t, db)
		q := []float32{1, 0}

		put(t, db, l.docs, map[int64]doc{
			1: {Title: "east", Embedding: []float32{1, 0}},
			2: {Title: "north", Embedding: []float32{0, 1}},
			3: {Title: "west", Embedding: []float32{-1, 0}},
		})
		require.Equal(t, 3, entries(t, db, l.x))
		require.Equal(t, []int64{1, 2, 3}, l.nearest(t, db, q, 10))

		// A record with another vector.
		put(t, db, l.docs, map[int64]doc{1: {Title: "west-ish", Embedding: []float32{-1, 0.1}}})
		require.Equal(t, 3, entries(t, db, l.x))
		require.Equal(t, []int64{2, 1, 3}, l.nearest(t, db, q, 10))
		v, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.InDelta(t, -0.99504, v[0], 1e-4)
		require.InDelta(t, 0.09950, v[1], 1e-4)

		// A record that loses its vector leaves the index.
		put(t, db, l.docs, map[int64]doc{2: {Title: "north"}})
		require.Equal(t, 2, entries(t, db, l.x))
		_, err = l.vectorOf(t, db, 2)
		require.ErrorIs(t, err, kv.ErrNotFound)
		require.Equal(t, []int64{1, 3}, l.nearest(t, db, q, 10))

		// And gets in again with one.
		put(t, db, l.docs, map[int64]doc{2: {Title: "north", Embedding: []float32{0, 2}}})
		require.Equal(t, 3, entries(t, db, l.x))
		require.Equal(t, []int64{2, 1, 3}, l.nearest(t, db, q, 10))

		// A deleted record leaves it.
		del(t, db, l.docs, 3)
		require.Equal(t, 2, entries(t, db, l.x))
		_, err = l.vectorOf(t, db, 3)
		require.ErrorIs(t, err, kv.ErrNotFound)
		require.Equal(t, []int64{2, 1}, l.nearest(t, db, q, 10))

		// Deleting a record that has no vector, and one that does not exist, does nothing.
		put(t, db, l.docs, map[int64]doc{4: {Title: "empty"}, 5: {Title: "empty too", Embedding: []float32{}}})
		require.Equal(t, 2, entries(t, db, l.x))
		del(t, db, l.docs, 4)
		del(t, db, l.docs, 5)
		del(t, db, l.docs, 99)
		require.Equal(t, 2, entries(t, db, l.x))

		del(t, db, l.docs, 1)
		del(t, db, l.docs, 2)
		require.Zero(t, entries(t, db, l.x))
		require.Empty(t, l.nearest(t, db, q, 10))
	})
}

func TestRecordsWithoutVector(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)

		put(t, db, l.docs, map[int64]doc{
			1: {Title: "far", Embedding: []float32{9, 9}},
			2: {Title: "none"},
			3: {Title: "near", Embedding: []float32{1, 1}},
		})
		require.Equal(t, 2, entries(t, db, l.x))
		require.Equal(t, []int64{3, 1}, l.nearest(t, db, []float32{0, 0}, 10))
	})
}

func TestInvalidVectors(t *testing.T) {
	nan := float32(math.NaN())
	inf := float32(math.Inf(1))

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Cosine)
		l.ready(t, db)

		for name, tc := range map[string]struct {
			vec []float32
			err error
		}{
			"too short": {[]float32{1}, vector.ErrDimensions},
			"too long":  {[]float32{1, 2, 3}, vector.ErrDimensions},
			"nan":       {[]float32{1, nan}, vector.ErrNotFinite},
			"infinity":  {[]float32{inf, 1}, vector.ErrNotFinite},
			"zero":      {[]float32{0, 0}, vector.ErrZeroVector},
		} {
			err := trySet(t, db, l.docs, 1, doc{Title: name, Embedding: tc.vec})
			require.ErrorIs(t, err, tc.err, name)

			// The record is not stored, and the index does not have it.
			require.NoError(t, db.View(func(tx kv.Tx) error {
				ok, err := l.docs.Instantiate(tx).Has(1)
				require.False(t, ok, name)
				return err
			}))
			require.Zero(t, entries(t, db, l.x), name)
		}

		// The message has both numbers.
		err := trySet(t, db, l.docs, 1, doc{Embedding: []float32{1, 2, 3}})
		require.ErrorContains(t, err, "3")
		require.ErrorContains(t, err, "2")

		// A write that fails leaves the record as it was.
		put(t, db, l.docs, map[int64]doc{1: {Title: "ok", Embedding: []float32{1, 0}}})
		require.ErrorIs(t, trySet(t, db, l.docs, 1, doc{Title: "bad", Embedding: []float32{nan, 0}}), vector.ErrNotFinite)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			d, err := l.docs.Instantiate(tx).Get(1)
			require.NoError(t, err)
			require.Equal(t, "ok", d.Title)
			return nil
		}))
		require.Equal(t, 1, entries(t, db, l.x))
		v, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.InDeltaSlice(t, []float32{1, 0}, v, 1e-6)
	})

	// A zero vector has no direction only where the cosine is measured.
	eachMetric(t, []vector.Metric{vector.Euclidean, vector.Dot}, func(t *testing.T, db kv.DB, metric vector.Metric) {
		l := newLibrary(2, metric)
		l.ready(t, db)

		require.NoError(t, trySet(t, db, l.docs, 1, doc{Embedding: []float32{0, 0}}))
		require.Equal(t, 1, entries(t, db, l.x))
		require.ErrorIs(t, trySet(t, db, l.docs, 2, doc{Embedding: []float32{nan, 0}}), vector.ErrNotFinite)
		require.ErrorIs(t, trySet(t, db, l.docs, 2, doc{Embedding: []float32{0, inf}}), vector.ErrNotFinite)
		require.ErrorIs(t, trySet(t, db, l.docs, 2, doc{Embedding: []float32{0}}), vector.ErrDimensions)
		// Components that are finite, of a vector whose distances would not be.
		require.ErrorIs(t, trySet(t, db, l.docs, 2, doc{Embedding: []float32{1e20, -1e20}}), vector.ErrNotFinite)
		require.NoError(t, trySet(t, db, l.docs, 3, doc{Embedding: []float32{1e18, -1e18}}))
		del(t, db, l.docs, 3)
		require.Equal(t, []int64{1}, l.nearest(t, db, []float32{0, 0}, 5))
	})
}

func TestCosineStoresUnitVectors(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Cosine)
		l.ready(t, db)

		given := []float32{3, 4}
		put(t, db, l.docs, map[int64]doc{1: {Embedding: given}})
		require.Equal(t, []float32{3, 4}, given, "the slice of the caller is not modified")

		v, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.InDeltaSlice(t, []float32{0.6, 0.8}, v, 1e-6)

		// The record itself keeps what it was given.
		require.NoError(t, db.View(func(tx kv.Tx) error {
			d, err := l.docs.Instantiate(tx).Get(1)
			require.NoError(t, err)
			require.Equal(t, []float32{3, 4}, d.Embedding)
			return nil
		}))

		// Neither is the slice of a query.
		q := []float32{30, 40}
		require.Equal(t, []int64{1}, l.nearest(t, db, q, 1))
		require.Equal(t, []float32{30, 40}, q)
	})

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)

		put(t, db, l.docs, map[int64]doc{1: {Embedding: []float32{3, 4}}})
		v, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.Equal(t, []float32{3, 4}, v, "the other metrics store the vector as it is")
	})
}

func TestUnchangedVector(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Cosine)
		l.ready(t, db)

		put(t, db, l.docs, map[int64]doc{
			1: {Title: "a", Lang: "en", Embedding: []float32{3, 4}},
			2: {Title: "b", Lang: "en", Embedding: []float32{4, 3}},
		})
		before, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)

		// Other fields change and the vector does not.
		put(t, db, l.docs, map[int64]doc{1: {Title: "a2", Lang: "fr", Embedding: []float32{3, 4}}})
		require.Equal(t, 2, entries(t, db, l.x))
		after, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.Equal(t, before, after)
		require.Equal(t, []int64{1, 2}, l.nearest(t, db, []float32{3, 4}, 2))

		// A record that is set again, in the same transaction.
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := l.docs.Instantiate(tx)
			for _, e := range [][]float32{{1, 0}, {1, 0}, {0, 1}, {0, 1}} {
				if err := store.Set(1, &doc{Embedding: e}); err != nil {
					return err
				}
			}
			return nil
		}))
		v, err := l.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.InDeltaSlice(t, []float32{0, 1}, v, 1e-6)
		require.Equal(t, 2, entries(t, db, l.x))
	})
}

func TestCorruptEntry(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		l := newLibrary(2, vector.Cosine)
		l.ready(t, db)
		put(t, db, l.docs, map[int64]doc{
			1: {Embedding: []float32{1, 0}},
			2: {Embedding: []float32{0, 1}},
		})

		// An entry that is not a vector of the dimensions of the index.
		rawKey, err := l.docs.KeyCodec().Encode(2)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return tx.Set(append(append(append([]byte{}, l.x.Prefix()...), 'v'), rawKey...), []byte{1, 2, 3})
		}))

		_, err = l.vectorOf(t, db, 2)
		require.ErrorIs(t, err, vector.ErrCorrupt)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			_, err := l.x.Instantiate(tx).Nearest([]float32{1, 0}, 2)
			require.ErrorIs(t, err, vector.ErrCorrupt)
			return nil
		}))
	})
}

func TestDefinitionPanics(t *testing.T) {
	define := func(dims int, metric vector.Metric, get func(*doc) []float32) func() {
		return func() {
			vector.On(extstore.New[int64, doc]("docs"), "by_embedding", dims, metric, get)
		}
	}

	require.NotPanics(t, define(2, vector.Cosine, embedding))
	require.Panics(t, define(0, vector.Cosine, embedding))
	require.Panics(t, define(-3, vector.Cosine, embedding))
	require.Panics(t, define(2, vector.Cosine, nil))
	require.Panics(t, define(2, 0, embedding))
	require.Panics(t, define(2, vector.Metric(9), embedding))

	// The name of an extension is taken once.
	docs := extstore.New[int64, doc]("docs")
	vector.On(docs, "by_embedding", 2, vector.Cosine, embedding)
	require.Panics(t, func() { vector.On(docs, "by_embedding", 2, vector.Cosine, embedding) })
}

func TestAccessors(t *testing.T) {
	l := newLibrary(8, vector.Dot, vector.Versioned("m1"), vector.Versioned("m2"))
	require.Equal(t, "by_embedding", l.x.Name())
	require.Equal(t, 8, l.x.Dims())
	require.Equal(t, vector.Dot, l.x.Metric())
	require.NotEmpty(t, l.x.Prefix())
}

// shapes are records whose order by distance depends on the metric, for a query along the x axis.
var shapes = map[int64]doc{
	1: {Title: "long", Embedding: []float32{10, 1}},
	2: {Title: "diagonal", Embedding: []float32{1, 1}},
	3: {Title: "short", Embedding: []float32{0.9, 0}},
}

func TestMigration(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		ctx := context.Background()
		q := []float32{1, 0}

		// A program without the index writes the docs.
		v1 := extstore.New[int64, doc]("docs")
		require.NoError(t, v1.Check(db))
		put(t, db, v1, shapes)

		// The next one defines the index, which has to be built.
		v2 := newLibrary(2, vector.Cosine)
		require.ErrorIs(t, v2.docs.Check(db), migrate.ErrNeedsMigration)
		report, err := v2.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.NotEmpty(t, report.String())
		require.NoError(t, v2.docs.Check(db))
		require.Equal(t, 3, entries(t, db, v2.x))
		require.Equal(t, []int64{3, 1, 2}, v2.nearest(t, db, q, 10))

		again, err := v2.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.Empty(t, again.String())

		// Another version of the vectors builds it again.
		v3 := newLibrary(2, vector.Cosine, vector.Versioned("v2"))
		require.ErrorIs(t, v3.docs.Check(db), migrate.ErrNeedsMigration)
		report, err = v3.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.NotEmpty(t, report.String())
		require.NoError(t, v3.docs.Check(db))
		require.Equal(t, []int64{3, 1, 2}, v3.nearest(t, db, q, 10))

		// So does another metric, under which the vectors are stored as they are and the nearest are others.
		v4 := newLibrary(2, vector.Euclidean, vector.Versioned("v2"))
		require.ErrorIs(t, v4.docs.Check(db), migrate.ErrNeedsMigration)
		_, err = v4.docs.Migrate(ctx, db)
		require.NoError(t, err)
		require.NoError(t, v4.docs.Check(db))
		require.Equal(t, []int64{3, 2, 1}, v4.nearest(t, db, q, 10))
		v, err := v4.vectorOf(t, db, 1)
		require.NoError(t, err)
		require.Equal(t, []float32{10, 1}, v)

		// And another number of dimensions: the docs have to be valid for it.
		v5 := newLibrary(3, vector.Euclidean, vector.Versioned("v2"))
		require.ErrorIs(t, v5.docs.Check(db), migrate.ErrNeedsMigration)
		_, err = v5.docs.Migrate(ctx, db)
		require.ErrorIs(t, err, vector.ErrDimensions)
	})
}

func TestRebuild(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// More docs than a batch of a build has.
		const n = 1200
		l := newLibrary(2, vector.Euclidean)
		l.ready(t, db)

		withVector := 0
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			store := l.docs.Instantiate(tx)
			for id := int64(1); id <= n; id++ {
				d := doc{Embedding: []float32{float32(id), float32(id % 10)}}
				if id%7 == 0 {
					d.Embedding = nil
				} else {
					withVector++
				}
				if err := store.Set(id, &d); err != nil {
					return err
				}
			}
			return nil
		}))
		require.Equal(t, withVector, entries(t, db, l.x))
		want := l.nearest(t, db, []float32{500, 0}, 20)
		require.Len(t, want, 20)

		// The entries are gone, and the build brings them back.
		p := l.x.Prefix()
		require.NoError(t, kv.DeleteRange(context.Background(), db, p, kv.PrefixEnd(p)))
		require.Zero(t, entries(t, db, l.x))
		require.NoError(t, vector.Rebuild(context.Background(), db, l.x))
		require.Equal(t, withVector, entries(t, db, l.x))
		require.Equal(t, want, l.nearest(t, db, []float32{500, 0}, 20))

		// Building it again deletes what is there, so it makes no difference.
		require.NoError(t, vector.Rebuild(context.Background(), db, l.x))
		require.Equal(t, withVector, entries(t, db, l.x))

		// A build that is cancelled stops.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, vector.Rebuild(ctx, db, l.x), context.Canceled)
	})
}

func TestRebuildWithBadRecord(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		// A program without the index writes docs, one of which has a vector that is not valid.
		plain := extstore.New[int64, doc]("docs")
		require.NoError(t, plain.Check(db))
		put(t, db, plain, map[int64]doc{
			1:   {Embedding: []float32{1, 0}},
			77:  {Embedding: []float32{1, 0, 0}},
			100: {Embedding: []float32{0, 1}},
		})

		l := newLibrary(2, vector.Cosine)
		err := vector.Rebuild(context.Background(), db, l.x)
		require.ErrorIs(t, err, vector.ErrDimensions)
		require.ErrorContains(t, err, "77")

		_, err = l.docs.Migrate(context.Background(), db)
		require.ErrorIs(t, err, vector.ErrDimensions)
		require.ErrorContains(t, err, "77")
	})
}
