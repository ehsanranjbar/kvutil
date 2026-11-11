package vector_test

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
)

// countingDocs is a value codec of docs that counts the docs decoded in full and in part.
type countingDocs struct {
	codec.ValueCodec[doc]
	full, partial *atomic.Int64
}

func (c countingDocs) Decode(bz []byte) (doc, error) {
	c.full.Add(1)
	return c.ValueCodec.Decode(bz)
}

func (c countingDocs) PartialDecode(fields []string) (func([]byte, *doc) error, bool) {
	decode, ok := codec.PartialOf(c.ValueCodec, fields)
	if !ok {
		return nil, false
	}
	return func(bz []byte, d *doc) error {
		c.partial.Add(1)
		return decode(bz, d)
	}, true
}

// partialResult is a record that a filtered search found.
type partialResult struct {
	id   int64
	dist float32
	doc  doc
}

func TestFilteredSearchDecodesPartially(t *testing.T) {
	lang := field.Path[doc, string]("Lang")
	title := field.Path[doc, string]("Title")

	// docs have a vector of their own, and a few of the languages.
	rng := rand.New(rand.NewPCG(5, 6))
	docs := map[int64]doc{}
	for id := int64(1); id <= 300; id++ {
		v := make([]float32, oracleDims)
		for i := range v {
			v[i] = float32(rng.NormFloat64())
		}
		docs[id] = doc{
			Title:     fmt.Sprint("doc-", id),
			Lang:      []string{"en", "en", "fr", "de"}[rng.IntN(4)],
			Embedding: v,
		}
	}
	q := make([]float32, oracleDims)
	for i := range q {
		q[i] = float32(rng.NormFloat64())
	}

	// search runs the filtered search on a store whose codec is the given one, and returns what it
	// found with what was decoded to find it.
	search := func(t *testing.T, db kv.DB, c func(codec.ValueCodec[doc]) codec.ValueCodec[doc],
		limit int, conds ...cond.Cond[doc]) ([]partialResult, int64, int64) {
		t.Helper()

		var full, partial atomic.Int64
		var vc codec.ValueCodec[doc] = countingDocs{ValueCodec: codec.ValueFor[doc](nil), full: &full, partial: &partial}
		if c != nil {
			vc = c(vc)
		}
		store := extstore.NewWithCodecs[int64, doc]("docs", nil, vc)
		x := vector.On(store, "by_embedding", oracleDims, vector.Cosine, embedding)
		require.NoError(t, store.Check(db))
		put(t, db, store, docs)
		full.Store(0)
		partial.Store(0)

		var found []partialResult
		require.NoError(t, db.View(func(tx kv.Tx) error {
			res, err := x.Instantiate(tx).Search(q).Where(conds...).Limit(limit).Run()
			require.NoError(t, err)
			defer res.Close()
			for ; res.Valid(); res.Next() {
				id, err := res.Key()
				require.NoError(t, err)
				d, err := res.Value()
				require.NoError(t, err)
				found = append(found, partialResult{id, res.Distance(), *d})
			}
			return nil
		}))
		return found, full.Load(), partial.Load()
	}

	for _, test := range []struct {
		name  string
		conds []cond.Cond[doc]
		match func(doc) bool
	}{
		{"a scalar field", []cond.Cond[doc]{lang.Eq("fr")}, func(d doc) bool { return d.Lang == "fr" }},
		{"two fields", []cond.Cond[doc]{lang.Eq("en"), title.Gt("doc-5")}, func(d doc) bool { return d.Lang == "en" && d.Title > "doc-5" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			const limit = 6

			testkv.Each(t, func(t *testing.T, db kv.DB) {
				found, full, partial := search(t, db, nil, limit, test.conds...)
				require.Len(t, found, limit)
				for _, r := range found {
					require.True(t, test.match(r.doc), "%d: %+v", r.id, r.doc)
					require.Equal(t, docs[r.id], r.doc, "%d is whole", r.id)
				}
				// The docs that are not wanted are not decoded in full: they are decoded in part,
				// and the ones that are wanted are decoded in both ways.
				require.EqualValues(t, limit, full)
				require.Greater(t, partial, int64(limit))
				require.Less(t, partial, int64(len(docs)))
			})

			// The same results as with a codec that decodes in full.
			testkv.Each(t, func(t *testing.T, db kv.DB) {
				found, _, _ := search(t, db, nil, limit, test.conds...)
				plain, full, partial := search(t, db, codec.WithoutPartial[doc], limit, test.conds...)
				require.Equal(t, found, plain)
				require.Zero(t, partial)
				require.Greater(t, full, int64(limit))
			})
		})
	}

	// A condition that is not known to read some fields only makes the docs decode in full.
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		french := cond.Func("french", func(d *doc) bool { return d.Lang == "fr" })
		found, full, partial := search(t, db, nil, 6, french)
		require.Len(t, found, 6)
		require.Zero(t, partial)
		require.Greater(t, full, int64(6))

		declared := cond.Func("french", func(d *doc) bool { return d.Lang == "fr" }).Reading("Lang")
		again, full, partial := search(t, db, nil, 6, declared)
		require.Equal(t, found, again)
		require.EqualValues(t, 6, full)
		require.Greater(t, partial, int64(6))
	})
}
