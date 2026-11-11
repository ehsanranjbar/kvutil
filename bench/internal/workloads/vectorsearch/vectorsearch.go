// Package vectorsearch is the workload of searching by the nearest vector: kvutil's vector index
// (package vector, asked for through query.Nearest) against PostgreSQL with pgvector, on documents
// that have an embedding of 384 dimensions, a language and a year.
//
// A variant is a way of searching, on a database of its own that has the documents loaded before the
// indexes are built (the load and the build are phases of the run):
//
//	kvutil      exact        the vector index, not partitioned: every search compares the query with
//	                         every vector
//	            partitioned  vector.Partitioned: a search reads the nearest partitions only
//	            quantized    vector.Partitioned and vector.Quantized: it reads the codes of the
//	                         vectors of the nearest partitions, and re-ranks the best by their vectors
//	PostgreSQL  seqscan      no index on the vectors: every search compares the query with every row
//	            hnsw         an HNSW index (m 16, ef_construction 64)
//	            ivfflat      an IVFFlat index (lists: a thousandth of the rows, 10 at least)
//
// The cases are the 10 nearest documents (knn), with a filter that few documents satisfy
// (knn-filter-rare) and one that most do (knn-filter-broad), the knobs that trade recall for time
// (knn-sweep-*), and inserts with the indexes kept up to date (insert). Before a query case is
// timed, the 256 queries of its pool are run once and the share of the exact 10 nearest that they
// find (recall@10) is put in the result, so that the throughput of an approximate search is never
// read without what it gives up. The exact answers are computed in Go (see truth.go), once for the
// run, and are the same for both systems.
package vectorsearch

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/bookdb"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

const (
	// planSample is the number of queries whose plans are recorded.
	planSample = 20
	// verifyRuns is the number of queries of a case that are verified.
	verifyRuns = 4
)

type workload struct{}

func (workload) Name() string  { return "vector-search" }
func (workload) Group() string { return "vector" }
func (workload) Description() string {
	return "the 10 nearest of 384-dimension embeddings, filtered and not, with recall: kvutil's vector index against pgvector (exact, HNSW, IVFFlat); inserts"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		var vs []harness.Variant
		for _, kind := range []string{"exact", "partitioned", "quantized"} {
			vs = append(vs, &variant{name: kind, open: func(ctx context.Context, rc *harness.RunContext, d *data) (target, error) {
				return openKV(ctx, rc.Sys.(*kvsys.System), kind, d)
			}})
		}
		return vs
	case *pgsys.System:
		var vs []harness.Variant
		for _, kind := range []string{"seqscan", "hnsw", "ivfflat"} {
			vs = append(vs, &variant{name: kind, open: func(ctx context.Context, rc *harness.RunContext, d *data) (target, error) {
				return openPG(ctx, rc.Sys.(*pgsys.System), kind, d)
			}})
		}
		return vs
	}
	return nil
}

// knob is a setting of the search that a sweep varies.
type knob struct {
	// name labels the case: knn-sweep-<name>.
	name string
	// kind is what is set: probes (partitions, kvutil's and IVFFlat's), rescore (kvutil's
	// candidates to re-rank) or ef_search (HNSW's).
	kind  string
	value int
	// mult is the multiple of the default number of probes, for the probes of a sweep.
	mult int
}

func (k knob) params() map[string]any {
	p := map[string]any{"knob": k.kind, "value": k.value}
	if k.mult > 0 {
		p["probes_x"] = k.mult
	}
	return p
}

// ceilSqrt returns the square root of n, rounded up: the default number of partitions that kvutil
// reads, and the number of probes that the IVFFlat cases use.
func ceilSqrt(n int) int {
	r := 1
	for r*r < n {
		r++
	}
	return r
}

// probeKnobs returns the knobs that read once, twice and four times the default number of
// partitions (the square root of their number, rounded up), none of them past the partitions there
// are.
func probeKnobs(partitions int) []knob {
	if partitions < 2 {
		return nil
	}
	def := ceilSqrt(partitions)
	var ks []knob
	for _, m := range []int{1, 2, 4} {
		v := min(def*m, partitions)
		if len(ks) > 0 && ks[len(ks)-1].value == v {
			continue
		}
		ks = append(ks, knob{name: fmt.Sprintf("probes-x%d", m), kind: "probes", value: v, mult: m})
	}
	return ks
}

// searcher runs the search of a case: a filter, and the options of a knob.
type searcher interface {
	// search returns the ids of the nearest documents of the query, nearest first, appended to dst.
	search(ctx context.Context, q []float32, dst []int64) ([]int64, error)
	// info says how the system searches, for the result: the plans of the first queries and the
	// settings.
	info(ctx context.Context) (map[string]string, error)
	close()
}

// target is the database of a variant.
type target interface {
	// load writes the documents [0, n), and build makes the indexes over them.
	load(ctx context.Context) error
	build(ctx context.Context) error
	// settle makes what was written durable and, for PostgreSQL, analyzed.
	settle(ctx context.Context) error
	sizes(ctx context.Context) (harness.Sizes, error)
	// knobs are the settings that the sweep varies, once the indexes are built.
	knobs(ctx context.Context) ([]knob, error)
	// searcher prepares the search of a filter with the defaults of the variant, or with a knob.
	searcher(ctx context.Context, f filter, kb *knob) (searcher, error)
	insert(ctx context.Context, d *dataset.Doc) error
	// maintain starts whatever keeps the index while documents are inserted, and returns what
	// stops it.
	maintain(ctx context.Context) (stop func() error, err error)
	close(ctx context.Context)
}

type variant struct {
	name string
	open func(ctx context.Context, rc *harness.RunContext, d *data) (target, error)
}

func (v *variant) Name() string { return v.name }

// workerState is what a worker keeps between operations.
type workerState struct {
	ids []int64
	doc dataset.Doc
}

// queryCases are the cases of the search, each at 1 worker and at the run's concurrency.
var queryCases = []struct {
	name string
	f    filter
}{
	{"knn", noFilter},
	{"knn-filter-rare", rareFilter},
	{"knn-filter-broad", broadFilter},
}

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	d := dataOf(rc.Params.Seed, dataset.VectorDocs(rc.Params.Scale))
	t, err := v.open(ctx, rc, d)
	if err != nil {
		return err
	}
	defer t.close(context.WithoutCancel(ctx))
	rc.SizeFn = t.sizes

	if err := prepare(ctx, rc, t, d); err != nil {
		return err
	}

	for _, c := range queryCases {
		if err := measureSearch(ctx, rc, t, d, c.name, c.f, nil, harness.Levels(rc.Params.Concurrency)); err != nil {
			return fmt.Errorf("%s: %w", c.name, err)
		}
	}

	knobs, err := t.knobs(ctx)
	if err != nil {
		return err
	}
	for i := range knobs {
		name := "knn-sweep-" + knobs[i].name
		if err := measureSearch(ctx, rc, t, d, name, noFilter, &knobs[i], []int{1}); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}

	return v.measureInsert(ctx, rc, t, d)
}

// prepare loads the documents and builds the indexes, as the phases load and build.
func prepare(ctx context.Context, rc *harness.RunContext, t target, d *data) error {
	n := int64(d.n)
	if err := bookdb.Phase(ctx, rc, "load", func(ctx context.Context) (int64, error) { return n, t.load(ctx) }); err != nil {
		return err
	}
	if err := t.settle(ctx); err != nil {
		return err
	}
	if err := bookdb.Phase(ctx, rc, "build", func(ctx context.Context) (int64, error) { return n, t.build(ctx) }); err != nil {
		return err
	}
	return t.settle(ctx)
}

// measureSearch finds the recall of a search and then times it at the levels of concurrency. Each
// operation picks a query of the pool at random and checks that the search found as many documents
// as there are to find.
func measureSearch(ctx context.Context, rc *harness.RunContext, t target, d *data, name string, f filter, kb *knob, levels []int) error {
	s, err := t.searcher(ctx, f, kb)
	if err != nil {
		return err
	}
	defer s.close()
	info, err := s.info(ctx)
	if err != nil {
		return err
	}
	recall, err := measureRecall(ctx, s, d, f, max(1, rc.Params.Concurrency))
	if err != nil {
		return fmt.Errorf("recall: %w", err)
	}
	info["recall_at_10"] = fmt.Sprintf("%.4f", recall)
	info["filter"] = f.describe()
	rc.Logf("   %s: recall@10 %.4f", name, recall)

	params := map[string]any{"filter": f.describe()}
	if kb != nil {
		for key, val := range kb.params() {
			params[key] = val
		}
	}
	op := func(ctx context.Context, w *harness.Worker) error {
		qi := w.Rand.IntN(len(d.queries))
		st := w.State.(*workerState)
		w.Begin()
		ids, err := s.search(ctx, d.queries[qi], st.ids[:0])
		if err != nil {
			return err
		}
		st.ids = ids
		if want := d.truth.want(f, qi); len(ids) != want {
			return fmt.Errorf("found %d documents, want %d", len(ids), want)
		}
		return nil
	}
	for _, conc := range levels {
		err := rc.Measure(ctx, harness.Case{
			Name: name, Concurrency: conc, Params: params, Info: info,
			Extra: map[string]float64{"recall_at_10": recall},
			Init:  func(w *harness.Worker) { w.State = &workerState{} },
		}, op)
		if err != nil {
			return err
		}
	}
	return nil
}

// measureRecall runs the queries of the pool once and returns the mean share of the exact nearest
// documents that the search found.
func measureRecall(ctx context.Context, s searcher, d *data, f filter, workers int) (float64, error) {
	recalls := make([]float64, len(d.queries))
	var (
		wg      sync.WaitGroup
		next    atomic.Int64
		errOnce sync.Once
		first   error
	)
	for range min(workers, len(d.queries)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ids []int64
			for {
				qi := int(next.Add(1) - 1)
				if qi >= len(d.queries) || ctx.Err() != nil {
					return
				}
				var err error
				ids, err = s.search(ctx, d.queries[qi], ids[:0])
				if err != nil {
					errOnce.Do(func() { first = err })
					return
				}
				recalls[qi] = d.truth.recall(f, qi, ids)
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return 0, first
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var sum float64
	for _, r := range recalls {
		sum += r
	}
	return sum / float64(len(recalls)), nil
}

// measureInsert times inserting new documents, each in a transaction (or statement) of its own,
// with the indexes kept up to date. The ids are those after the documents that were loaded.
func (v *variant) measureInsert(ctx context.Context, rc *harness.RunContext, t target, d *data) error {
	stop, err := t.maintain(ctx)
	if err != nil {
		return err
	}
	var fresh atomic.Int64
	fresh.Store(int64(d.n))
	for _, conc := range harness.Levels(rc.Params.Concurrency) {
		err = rc.Measure(ctx, harness.Case{
			Name: "insert", Concurrency: conc,
			Init: func(w *harness.Worker) { w.State = &workerState{} },
		}, func(ctx context.Context, w *harness.Worker) error {
			doc := &w.State.(*workerState).doc
			d.vectors.GenInto(doc, fresh.Add(1)-1)
			w.Begin()
			return t.insert(ctx, doc)
		})
		if err != nil {
			break
		}
	}
	if serr := stop(); err == nil {
		err = serr
	}
	return err
}

// Verify has the variant that searches exactly answer the first queries, without a filter and with
// the rare one: the ids of the 10 nearest documents, sorted so that exact ties cannot tell the
// systems apart. kvutil's and PostgreSQL's must be the same, and the same as the brute force in Go
// that the recall is scored against, which kvutil adds. The approximate variants answer different
// questions, and their recall is in the results of a run.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	v := (workload{}).Variants(sys)[0].(*variant)
	d := dataOf(p.Seed, dataset.VectorDocs(p.Scale))
	rc := &harness.RunContext{Params: p, Sys: sys, Workload: "vector-search", Variant: "verify"}

	t, err := v.open(ctx, rc, d)
	if err != nil {
		return nil, err
	}
	defer t.close(context.WithoutCancel(ctx))
	if err := prepare(ctx, rc, t, d); err != nil {
		return nil, err
	}

	line := func(label string, qi int, ids []int64) string {
		ids = slices.Clone(ids)
		slices.Sort(ids)
		parts := make([]string, len(ids))
		for i, id := range ids {
			parts[i] = strconv.FormatInt(id, 10)
		}
		return fmt.Sprintf("%s #%d: %s", label, qi, strings.Join(parts, ","))
	}
	out := harness.Output{Variant: v.name, Group: "exact"}
	for _, c := range queryCases[:2] {
		s, err := t.searcher(ctx, c.f, nil)
		if err != nil {
			return nil, err
		}
		for qi := range verifyRuns {
			ids, err := s.search(ctx, d.queries[qi], nil)
			if err != nil {
				s.close()
				return nil, fmt.Errorf("%s #%d: %w", c.name, qi, err)
			}
			out.Lines = append(out.Lines, line(c.name, qi, ids))
		}
		s.close()
	}
	outs := []harness.Output{out}

	if _, ok := sys.(*kvsys.System); ok {
		brute := harness.Output{Variant: "brute-force", Group: "exact"}
		for _, c := range queryCases[:2] {
			for qi := range verifyRuns {
				brute.Lines = append(brute.Lines, line(c.name, qi, d.truth.ids[c.f][qi]))
			}
		}
		outs = append(outs, brute)
	}
	return outs, nil
}
