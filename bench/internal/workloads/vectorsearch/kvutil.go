package vectorsearch

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/vector"
)

type doc = dataset.Doc

type docStore = extstore.Store[int64, doc]

var (
	fLang = field.Path[doc, string]("Lang")
	fYear = field.Path[doc, int]("Year")
)

// The names of the indexes of the documents.
const (
	idxEmbedding = "by_embedding"
	idxLang      = "by_lang"
	idxYear      = "by_year"
)

// newStore returns the store of the documents, without indexes.
func newStore() *docStore { return extstore.New[int64, doc]("docs") }

// vectorOptions are the options of the vector index of a variant.
func vectorOptions(kind string) ([]vector.Option, string) {
	switch kind {
	case "partitioned":
		return []vector.Option{vector.Partitioned(0)}, "vector.Partitioned(0)"
	case "quantized":
		return []vector.Option{vector.Partitioned(0), vector.Quantized()}, "vector.Partitioned(0), vector.Quantized()"
	}
	return nil, "vector.On, not partitioned"
}

// define adds the indexes to a store: the vector index, and one each on the language and the year.
func define(st *docStore, kind string) *vector.Index[int64, doc] {
	opts, _ := vectorOptions(kind)
	emb := vector.On(st, idxEmbedding, dataset.VectorDims, vector.Cosine, func(d *doc) []float32 { return d.Embedding }, opts...)
	index.On(st, idxLang, fLang)
	index.On(st, idxYear, fYear)
	return emb
}

// kvTarget is a database of Badger with the documents in it.
type kvTarget struct {
	kind string
	d    *data
	db   *kvsys.DB
	// base loads the documents, and has no index. docs is the store that reads and writes them,
	// with the indexes that build defined.
	base, docs *docStore
	emb        *vector.Index[int64, doc]
}

func openKV(ctx context.Context, sys *kvsys.System, kind string, d *data) (target, error) {
	db, err := sys.Open("vectors-" + kind)
	if err != nil {
		return nil, err
	}
	base := newStore()
	if _, err := base.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return &kvTarget{kind: kind, d: d, db: db, base: base, docs: base}, nil
}

// load writes the documents with a bulk writer, before there is an index to keep up to date.
func (t *kvTarget) load(context.Context) error {
	_, err := wl.BulkLoad(t.db, 0, int64(t.d.n), 1, func() func(kv.BulkWriter, int64) error {
		d := &doc{}
		prefix := bytes.Clone(t.base.DataPrefix())
		return func(w kv.BulkWriter, id int64) error {
			t.d.vectors.GenInto(d, id)
			key, err := t.base.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			value, err := t.base.ValueCodec().Encode(*d)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(prefix), key...), value)
		}
	})
	return err
}

// build defines the indexes on a store that has the prefix of the one that loaded the documents,
// and migrates the database to it, which builds them over the documents that are there: the two
// B-trees, and the vector index (with its partitions, if it has any).
func (t *kvTarget) build(ctx context.Context) error {
	st := newStore()
	emb := define(st, t.kind)
	report, err := st.Migrate(ctx, t.db)
	if err != nil {
		return err
	}
	built := 0
	for _, a := range report.Actions {
		if a.What == migrate.Built {
			built++
		}
	}
	if built != 3 {
		return fmt.Errorf("the migration built %d of 3 indexes: %s", built, report)
	}
	t.docs, t.emb = st, emb
	return nil
}

func (t *kvTarget) settle(context.Context) error { return t.db.Sync() }

func (t *kvTarget) sizes(context.Context) (harness.Sizes, error) {
	n, err := t.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (t *kvTarget) close(context.Context) { t.db.Destroy() }

// partitions returns the number of partitions that the vector index has.
func (t *kvTarget) partitions() (int, error) {
	var stats vector.Stats
	err := t.db.View(func(tx kv.Tx) error {
		var err error
		stats, err = t.emb.Instantiate(tx).Stats()
		return err
	})
	return stats.Partitions, err
}

// knobs are the probes of the partitions at once, twice and four times the default, and, for the
// index that has codes, the number of candidates that are re-ranked by their vectors.
func (t *kvTarget) knobs(context.Context) ([]knob, error) {
	if t.kind == "exact" {
		return nil, nil
	}
	parts, err := t.partitions()
	if err != nil {
		return nil, err
	}
	ks := probeKnobs(parts)
	if t.kind == "quantized" && len(ks) > 0 {
		for _, r := range []int{50, 100, 300} {
			ks = append(ks, knob{name: fmt.Sprintf("rescore-%d", r), kind: "rescore", value: r})
		}
	}
	return ks, nil
}

func (t *kvTarget) insert(_ context.Context, d *doc) error {
	return t.db.Update(func(tx kv.Tx) error { return t.docs.Instantiate(tx).Set(d.ID, d) })
}

// maintain runs a maintainer of the partitions while documents are inserted, as a program that has
// a partitioned index would.
func (t *kvTarget) maintain(ctx context.Context) (func() error, error) {
	if t.kind == "exact" {
		return func() error { return nil }, nil
	}
	var (
		mu    sync.Mutex
		first error
	)
	m := vector.NewMaintainer(t.db, []vector.Source{t.docs}, vector.WithErrorHandler(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if first == nil {
			first = err
		}
	}))
	m.Spawn(ctx)
	return func() error {
		if err := m.Shutdown(context.WithoutCancel(ctx)); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if first != nil {
			return fmt.Errorf("the maintainer: %w", first)
		}
		return nil
	}, nil
}

// kvSearcher is the search of a case: a query of package query that is built, planned and run for
// each vector.
type kvSearcher struct {
	t  *kvTarget
	f  filter
	kb *knob
}

func (t *kvTarget) searcher(_ context.Context, f filter, kb *knob) (searcher, error) {
	return &kvSearcher{t: t, f: f, kb: kb}, nil
}

func (s *kvSearcher) close() {}

// query builds the query of a vector: the nearest documents that satisfy the filter. The planner
// chooses how the filter and the search are put together.
func (s *kvSearcher) query(q []float32) *query.Query[int64, doc] {
	near := s.t.emb.To(q)
	if s.kb != nil {
		switch s.kb.kind {
		case "probes":
			near = near.Probes(s.kb.value)
		case "rescore":
			near = near.Rescore(s.kb.value)
		}
	}
	qy := query.From(s.t.docs)
	switch s.f {
	case rareFilter:
		qy = qy.Where(fLang.Eq("is"))
	case broadFilter:
		qy = qy.Where(fYear.Gte(2000))
	}
	return qy.Nearest(near).Limit(k)
}

func (s *kvSearcher) search(_ context.Context, q []float32, dst []int64) ([]int64, error) {
	qy := s.query(q)
	start := len(dst)
	err := s.t.db.View(func(tx kv.Tx) error {
		dst = dst[:start]
		it := qy.Run(tx)
		defer it.Close()
		for ; it.Valid(); it.Next() {
			id, err := it.Key()
			if err != nil {
				return err
			}
			dst = append(dst, id)
		}
		return it.Err()
	})
	return dst, err
}

func (s *kvSearcher) info(context.Context) (map[string]string, error) {
	var survey plans.Survey
	for _, q := range s.t.d.queries[:planSample] {
		text, err := s.query(q).Explain()
		if err != nil {
			return nil, err
		}
		survey.Add(plans.KV(text))
	}
	info := survey.Info()
	_, info["index"] = vectorOptions(s.t.kind)
	return info, nil
}
