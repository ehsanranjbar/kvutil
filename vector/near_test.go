package vector_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/vector"
)

func TestNearDescribe(t *testing.T) {
	q := []float32{1, 0, 0, 0}
	exact := newLibrary(4, vector.Cosine).x
	partitioned := newLibrary(4, vector.Cosine, vector.Partitioned(100)).x
	quantized := newLibrary(4, vector.Cosine, vector.Partitioned(100), vector.Quantized()).x

	for name, tc := range map[string]struct {
		near vector.Near[int64, doc]
		want string
	}{
		"unpartitioned":              {exact.To(q), "exact"},
		"unpartitioned, with probes": {exact.To(q).Probes(3).Rescore(5), "exact"},
		"unpartitioned, within":      {exact.To(q).Within(0.5), "exact, within 0.5"},
		"partitioned":                {partitioned.To(q), "partitioned"},
		"probes":                     {partitioned.To(q).Probes(3), "partitioned, probes 3"},
		"within":                     {partitioned.To(q).Within(0.25), "partitioned, within 0.25"},
		"exact option":               {partitioned.To(q).Probes(3).Exact(), "exact"},
		"exact option, within":       {quantized.To(q).Exact().Within(2), "exact, within 2"},
		"quantized":                  {quantized.To(q), "partitioned, quantized"},
		"quantized, everything": {
			quantized.To(q).Within(0.25).Rescore(50).Probes(2), "partitioned, quantized, probes 2, rescore 50, within 0.25",
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.near.Describe())
			require.Equal(t, "by_embedding", tc.near.IndexName())
		})
	}

	// It is a value: the options make other ones.
	base := partitioned.To(q)
	_ = base.Probes(2).Exact().Within(1)
	require.Equal(t, "partitioned", base.Describe())
}

// closeCounter is a source that counts how many times it is closed.
type closeCounter struct {
	iterutil.Iterator[int64, *doc]
	closed int
}

func (c *closeCounter) Close() error {
	c.closed++
	return c.Iterator.Close()
}

func TestNearNearest(t *testing.T) {
	eachMetric(t, []vector.Metric{vector.Cosine, vector.Euclidean}, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 3000, vector.Partitioned(100))
		f.rebuild(t, db)
		vectors := f.withVector()
		partitions := stats(t, db, f.x).Partitions
		rare := cond.Func("rare", func(d *doc) bool { return d.Title == "rare" })

		// distances reads what the iterator of a Nearness gives.
		type distancer interface{ Distance() float32 }
		read := func(it iterutil.Iterator[int64, *doc]) (keys []int64, dists []float32, scanned int) {
			defer it.Close()
			res, ok := it.(*vector.Results[int64, doc])
			require.True(t, ok, "the iterator is the results of a search")
			require.Implements(t, (*distancer)(nil), it)
			for ; it.Valid(); it.Next() {
				key, err := it.Key()
				require.NoError(t, err)
				keys = append(keys, key)
				dists = append(dists, it.(distancer).Distance())
			}
			return keys, dists, res.Scanned()
		}

		require.NoError(t, db.View(func(tx kv.Tx) error {
			ins := f.x.Instantiate(tx)
			f.query = cl.draw(rng)

			// It is the search of the index.
			it, err := f.x.To(f.query).Nearest(tx, nil, nil, 10)
			require.NoError(t, err)
			keys, dists, _ := read(it)
			want, err := ins.Search(f.query).Limit(10).Run()
			require.NoError(t, err)
			require.Equal(t, drain(t, want), func() (hits []hit) {
				for i := range keys {
					hits = append(hits, hit{keys[i], dists[i]})
				}
				return hits
			}())

			// A negative limit is no limit.
			it, err = f.x.To(f.query).Exact().Nearest(tx, nil, nil, -1)
			require.NoError(t, err)
			keys, _, scanned := read(it)
			require.Len(t, keys, vectors)
			require.Equal(t, vectors, scanned)

			// The options are the search's.
			it, err = f.x.To(f.query).Probes(1).Nearest(tx, nil, nil, 10)
			require.NoError(t, err)
			keys, _, scanned = read(it)
			require.Equal(t, run(t, ins.Search(f.query).Limit(10).Probes(1)), found{keys, scanned})
			it, err = f.x.To(f.query).Probes(partitions).Nearest(tx, nil, nil, 10)
			require.NoError(t, err)
			_, _, scanned = read(it)
			require.Equal(t, vectors, scanned)
			it, err = f.x.To(f.query).Exact().Nearest(tx, nil, nil, 10)
			require.NoError(t, err)
			keys, _, _ = read(it)
			require.Equal(t, run(t, ins.Search(f.query).Limit(10).Exact()).keys, keys)

			within := float32(f.threshold(0.05))
			it, err = f.x.To(f.query).Exact().Within(within).Nearest(tx, nil, nil, -1)
			require.NoError(t, err)
			_, dists, _ = read(it)
			require.NotEmpty(t, dists)
			require.Less(t, len(dists), vectors/4)
			for _, d := range dists {
				require.LessOrEqual(t, d, within)
			}

			// The conditions are checked on the records.
			it, err = f.x.To(f.query).Nearest(tx, nil, []cond.Cond[doc]{rare}, 5)
			require.NoError(t, err)
			keys, _, _ = read(it)
			require.Len(t, keys, 5)
			for _, k := range keys {
				require.Equal(t, "rare", f.byID[k].Title)
			}
			it, err = f.x.To(f.query).Exact().Nearest(tx, nil, []cond.Cond[doc]{rare}, 5)
			require.NoError(t, err)
			exact, _, _ := read(it)
			require.Equal(t, run(t, ins.Search(f.query).Limit(5).Exact().Where(rare)).keys, exact)

			// Only the records of the source are candidates, and the source is closed.
			source, err := f.byLang.Instantiate(tx).Match("fr")
			require.NoError(t, err)
			counter := &closeCounter{Iterator: source}
			it, err = f.x.To(f.query).Nearest(tx, counter, nil, -1)
			require.NoError(t, err)
			require.Equal(t, 1, counter.closed)
			keys, _, _ = read(it)
			require.NotEmpty(t, keys)
			for _, k := range keys {
				require.Equal(t, "fr", f.byID[k].Lang)
			}
			return nil
		}))
	})
}

func TestNearErrorsCloseTheSource(t *testing.T) {
	eachMetric(t, []vector.Metric{vector.Euclidean}, func(t *testing.T, db kv.DB, metric vector.Metric) {
		f, cl, rng := newClusteredFixture(t, db, metric, 300, vector.Partitioned(100))
		f.rebuild(t, db)

		q := cl.draw(rng)
		require.NoError(t, db.View(func(tx kv.Tx) error {
			for name, near := range map[string]vector.Near[int64, doc]{
				"too few dimensions": f.x.To(q[:3]),
				"no dimensions":      f.x.To(nil),
				"no probes":          f.x.To(q).Probes(0),
				"no candidates":      f.x.To(q).Rescore(-1),
			} {
				source, err := f.byLang.Instantiate(tx).Match("en")
				require.NoError(t, err)
				counter := &closeCounter{Iterator: source}

				it, err := near.Nearest(tx, counter, nil, 10)
				require.Error(t, err, name)
				require.Nil(t, it, name)
				require.Equal(t, 1, counter.closed, name)
			}

			// And the source of a search that goes well is closed once too.
			source, err := f.byLang.Instantiate(tx).Match("en")
			require.NoError(t, err)
			counter := &closeCounter{Iterator: source}
			it, err := f.x.To(q).Nearest(tx, counter, nil, 0)
			require.NoError(t, err)
			require.False(t, it.Valid())
			require.Equal(t, 1, counter.closed)
			return it.Close()
		}))
	})
}
