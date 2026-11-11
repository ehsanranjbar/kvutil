package vectorsearch

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// naiveTruth is the exact answer the slow way: every distance, sorted, for each filter.
func naiveTruth(vs *dataset.Vectors, n int, queries [][]float32) [numFilters][][]int64 {
	var out [numFilters][][]int64
	cosine := func(a, b []float32) float64 {
		var dot, na, nb float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		return 1 - dot/math.Sqrt(na*nb)
	}
	for f := range numFilters {
		for _, q := range queries {
			var all []neighbour
			for id := range int64(n) {
				d := vs.Gen(id)
				if f.matches(d) {
					all = append(all, neighbour{cosine(d.Embedding, q), id})
				}
			}
			slices.SortFunc(all, func(a, b neighbour) int {
				switch {
				case a.before(b):
					return -1
				case b.before(a):
					return 1
				}
				return 0
			})
			var ids []int64
			for _, nb := range all[:min(k, len(all))] {
				ids = append(ids, nb.id)
			}
			out[f] = append(out[f], ids)
		}
	}
	return out
}

func TestTruthMatchesTheNaiveComputation(t *testing.T) {
	const n = 700
	vs := dataset.NewVectors(3, n)
	queries := vs.Queries(8)

	got := computeTruth(vs, n, queries)
	want := naiveTruth(vs, n, queries)
	for f := range numFilters {
		if !reflect.DeepEqual(got.ids[f], want[f]) {
			t.Errorf("filter %s:\n got %v\nwant %v", f, got.ids[f], want[f])
		}
	}
	// The rare filter has about 14 documents here, so each answer is full.
	for qi := range queries {
		if got.want(rareFilter, qi) != k || got.want(noFilter, qi) != k {
			t.Errorf("query %d wants %d and %d", qi, got.want(noFilter, qi), got.want(rareFilter, qi))
		}
	}

	// It does not depend on how many CPUs share the work.
	old := runtime.GOMAXPROCS(1)
	one := computeTruth(vs, n, queries)
	runtime.GOMAXPROCS(old)
	if !reflect.DeepEqual(one, got) {
		t.Error("the truth depends on GOMAXPROCS")
	}
}

func TestTruthOfAFilterThatLetsFewThrough(t *testing.T) {
	// 40 documents: the rare filter has fewer than 10 of them.
	const n = 40
	vs := dataset.NewVectors(1, n)
	tr := computeTruth(vs, n, vs.Queries(3))
	matching := 0
	for id := range int64(n) {
		if rareFilter.matches(vs.Gen(id)) {
			matching++
		}
	}
	if matching >= k {
		t.Fatalf("%d matching documents: the test needs fewer than %d", matching, k)
	}
	for qi := range 3 {
		if tr.want(rareFilter, qi) != matching || tr.want(noFilter, qi) != k {
			t.Errorf("query %d: %d and %d", qi, tr.want(noFilter, qi), tr.want(rareFilter, qi))
		}
	}
}

func TestRecall(t *testing.T) {
	tr := &truth{}
	tr.ids[noFilter] = [][]int64{{1, 2, 3, 4}, {}}
	for _, c := range []struct {
		found []int64
		want  float64
	}{
		{[]int64{1, 2, 3, 4}, 1},
		{[]int64{4, 3, 2, 1}, 1},
		{[]int64{1, 2, 9, 8}, 0.5},
		{[]int64{9}, 0},
		{nil, 0},
	} {
		if got := tr.recall(noFilter, 0, c.found); got != c.want {
			t.Errorf("recall of %v is %v, want %v", c.found, got, c.want)
		}
	}
	// A query with nothing to find is found whole.
	if got := tr.recall(noFilter, 1, nil); got != 1 {
		t.Errorf("recall of nothing against nothing is %v", got)
	}
}

func TestTopKKeepsTheNearestInOrder(t *testing.T) {
	var top topK
	for i := range 50 {
		top.offer(neighbour{dist: float64((i * 7) % 50), id: int64(i)})
	}
	// A tie at the distance of the tenth: the lower id stays.
	top.offer(neighbour{dist: 9, id: 1000})
	top.offer(neighbour{dist: 9, id: -1})
	if len(top.items) != k {
		t.Fatalf("%d items", len(top.items))
	}
	for i := 1; i < k; i++ {
		if !top.items[i-1].before(top.items[i]) {
			t.Errorf("not in order: %v", top.items)
		}
	}
	if top.items[0].dist != 0 || top.items[k-1] != (neighbour{9, -1}) {
		t.Errorf("items %v", top.items)
	}
}

func TestProbeKnobs(t *testing.T) {
	values := func(ks []knob) string {
		var parts []string
		for _, k := range ks {
			parts = append(parts, fmt.Sprintf("%s=%d", k.name, k.value))
		}
		return strings.Join(parts, " ")
	}
	for partitions, want := range map[int]string{
		0:   "",
		1:   "",
		2:   "probes-x1=2",
		8:   "probes-x1=3 probes-x2=6 probes-x4=8",
		16:  "probes-x1=4 probes-x2=8 probes-x4=16",
		366: "probes-x1=20 probes-x2=40 probes-x4=80",
	} {
		if got := values(probeKnobs(partitions)); got != want {
			t.Errorf("%d partitions: %q, want %q", partitions, got, want)
		}
	}
	for _, c := range []struct{ n, want int }{{1, 1}, {2, 2}, {9, 3}, {10, 4}, {250, 16}} {
		if got := ceilSqrt(c.n); got != c.want {
			t.Errorf("ceilSqrt(%d) = %d", c.n, got)
		}
	}
}

// The statements that PostgreSQL is sent, and the settings of its sessions, which need no server.
func TestPostgresStatements(t *testing.T) {
	if got, want := searchSQL("s.docs", noFilter), "SELECT id FROM s.docs ORDER BY embedding <=> $1::vector LIMIT 10"; got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	if got := searchSQL("s.docs", rareFilter); !strings.Contains(got, " WHERE lang = 'is' ORDER BY embedding <=> $1::vector LIMIT 10") {
		t.Errorf("rare: %s", got)
	}
	if got := searchSQL("s.docs", broadFilter); !strings.Contains(got, " WHERE year >= 2000 ORDER BY") {
		t.Errorf("broad: %s", got)
	}
	if got := createTableSQL("s.docs"); !strings.Contains(got, "embedding vector(384) NOT NULL") {
		t.Errorf("table: %s", got)
	}
	if got := insertSQL("s.docs"); got != "INSERT INTO s.docs (id, lang, year, embedding) VALUES ($1, $2, $3, $4)" {
		t.Errorf("insert: %s", got)
	}

	idx := func(kind string) string { return strings.Join(indexSQL("s.docs", kind, 100), "\n") }
	if got := idx("seqscan"); strings.Contains(got, "embedding") || !strings.Contains(got, "(lang)") || !strings.Contains(got, "(year)") {
		t.Errorf("seqscan: %s", got)
	}
	if got := idx("hnsw"); !strings.Contains(got, "USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)") {
		t.Errorf("hnsw: %s", got)
	}
	if got := idx("ivfflat"); !strings.Contains(got, "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)") {
		t.Errorf("ivfflat: %s", got)
	}

	for rows, want := range map[int]int{100: 10, 5000: 10, 100_000: 100, 250_000: 250, 1_000_000: 1000, 4_000_000: 2000} {
		if got := ivfLists(rows); got != want {
			t.Errorf("ivfLists(%d) = %d, want %d", rows, got, want)
		}
	}

	for _, c := range []struct {
		kind string
		f    filter
		kb   *knob
		want string
	}{
		{"seqscan", noFilter, nil, ""},
		{"seqscan", rareFilter, nil, ""},
		{"hnsw", noFilter, nil, "hnsw.ef_search = 40"},
		{"hnsw", rareFilter, nil, "hnsw.ef_search = 40; hnsw.iterative_scan = relaxed_order"},
		{"hnsw", noFilter, &knob{kind: "ef_search", value: 200}, "hnsw.ef_search = 200"},
		{"ivfflat", noFilter, nil, "ivfflat.probes = 10"},
		{"ivfflat", broadFilter, nil, "ivfflat.probes = 10; ivfflat.iterative_scan = relaxed_order"},
		{"ivfflat", noFilter, &knob{kind: "probes", value: 40}, "ivfflat.probes = 40"},
	} {
		got := strings.Join(searchSettings(c.kind, c.f, c.kb, 100), "; ")
		if got != c.want {
			t.Errorf("%s %s: %q, want %q", c.kind, c.f, got, c.want)
		}
	}
}

// The variants of kvutil run every case on a small dataset: the exact one finds all of the exact
// nearest documents, the partitioned ones nearly all, and the plans are those that the planner is
// expected to choose.
func TestKvutilVariantsRunEveryCase(t *testing.T) {
	const n = 5000
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), testutil.Params(n))

	byVariant := map[string]map[string]*metrics.Result{}
	count := map[string]int{}
	for i := range rs {
		r := &rs[i]
		if byVariant[r.Variant] == nil {
			byVariant[r.Variant] = map[string]*metrics.Result{}
		}
		key := r.Case + "/" + strconv.Itoa(r.Params.Concurrency)
		byVariant[r.Variant][key] = r
		if r.Case == "load" || r.Case == "build" {
			if r.Metrics.Records != n || r.Metrics.RecordsPerSec <= 0 {
				t.Errorf("%s/%s: records=%d per second=%v", r.Variant, r.Case, r.Metrics.Records, r.Metrics.RecordsPerSec)
			}
			if r.Metrics.DiskBytes <= 0 {
				t.Errorf("%s/%s: no size on disk", r.Variant, r.Case)
			}
			continue
		}
		count[r.Variant]++
		m := r.Metrics
		if m.Ops == 0 || m.Errors != 0 {
			t.Errorf("%s/%s: ops=%d errors=%d %v", r.Variant, r.Case, m.Ops, m.Errors, m.ErrorSample)
		}
		if r.Case == "insert" {
			continue
		}
		t.Logf("%-11s %-24s c=%d recall %.4f  %s", r.Variant, r.Case, r.Params.Concurrency, m.Extra["recall_at_10"], m.Info["plan_nodes"])

		// Every query case says its recall, as a number and as text.
		recall, ok := m.Extra["recall_at_10"]
		if !ok || m.Info["recall_at_10"] != fmt.Sprintf("%.4f", recall) {
			t.Errorf("%s/%s: recall extra %v info %q", r.Variant, r.Case, m.Extra, m.Info["recall_at_10"])
		}
		switch r.Variant {
		case "exact":
			if recall != 1 {
				t.Errorf("exact/%s: recall %v", r.Case, recall)
			}
		default:
			// The cases that use the defaults find nearly all; a sweep goes down to a few candidates
			// to re-rank, which can find less.
			floor := 0.9
			if strings.HasPrefix(r.Case, "knn-sweep") {
				floor = 0.5
			}
			if recall < floor {
				t.Errorf("%s/%s: recall %v, want %v at least", r.Variant, r.Case, recall, floor)
			}
		}
		if m.Info["plan_nodes"] == "" || m.Info["index"] == "" || m.Info["filter"] == "" {
			t.Errorf("%s/%s: info %v", r.Variant, r.Case, m.Info)
		}
	}

	// The cases of each variant: three at two levels, the insert at two, and the sweeps.
	for variant, want := range map[string]int{"exact": 8, "partitioned": 11, "quantized": 14} {
		if count[variant] != want {
			var names []string
			for key := range byVariant[variant] {
				names = append(names, key)
			}
			slices.Sort(names)
			t.Errorf("%s: %d timed results, want %d: %v", variant, count[variant], want, names)
		}
	}
	for _, key := range []string{"knn-sweep-probes-x1/1", "knn-sweep-probes-x4/1"} {
		if byVariant["partitioned"][key] == nil || byVariant["quantized"][key] == nil {
			t.Errorf("no %s", key)
		}
	}
	for _, key := range []string{"knn-sweep-rescore-50/1", "knn-sweep-rescore-100/1", "knn-sweep-rescore-300/1"} {
		if byVariant["quantized"][key] == nil || byVariant["partitioned"][key] != nil {
			t.Errorf("%s: quantized %v, partitioned %v", key, byVariant["quantized"][key] != nil, byVariant["partitioned"][key] != nil)
		}
		if r := byVariant["quantized"][key]; r != nil && (r.Params.Case["knob"] != "rescore" || r.Params.Case["value"] == nil) {
			t.Errorf("%s: params %v", key, r.Params.Case)
		}
	}

	// The plans. A search with no filter reads the vector index alone; the rare filter is read
	// through the index of the language and ranked; the broad one is checked on the nearest.
	for variant, results := range byVariant {
		if got := results["knn/1"].Metrics.Info["plan_nodes"]; got != "Limit > Nearest[by_embedding]" {
			t.Errorf("%s: knn plan %q", variant, got)
		}
		if got := results["knn-filter-rare/2"].Metrics.Info["plan_nodes"]; got != "Limit > Nearest[by_embedding] > IndexLookup[by_lang]" {
			t.Errorf("%s: rare plan %q", variant, got)
		}
		if got := results["knn-filter-broad/2"].Metrics.Info["plan_nodes"]; got != "Limit > Nearest[by_embedding]" {
			t.Errorf("%s: broad plan %q", variant, got)
		}
		if got := results["knn-filter-broad/2"].Metrics.Info["plan"]; !strings.Contains(got, "then check [Year >= 2000]") {
			t.Errorf("%s: broad plan %q", variant, got)
		}
		if got := results["knn/1"].Metrics.Info["plan"]; !strings.Contains(got, "Nearest by_embedding ("+map[string]string{
			"exact": "exact", "partitioned": "partitioned", "quantized": "partitioned, quantized",
		}[variant]+")") {
			t.Errorf("%s: knn plan %q", variant, got)
		}
		// The size on disk is there after the timed cases too.
		if results["insert/2"].Metrics.DiskBytes <= 0 {
			t.Errorf("%s: no size after the inserts", variant)
		}
	}
	if got := byVariant["quantized"]["knn-sweep-probes-x2/1"].Metrics.Info["plan"]; !strings.Contains(got, "probes ") {
		t.Errorf("sweep plan %q", got)
	}
	if got := byVariant["quantized"]["knn-sweep-rescore-300/1"].Metrics.Info["plan"]; !strings.Contains(got, "rescore 300") {
		t.Errorf("sweep plan %q", got)
	}
}

// The exact variant of kvutil answers as the brute force in Go does, which is what PostgreSQL's
// sequential scan is verified against.
func TestVerifyKvutilIsExact(t *testing.T) {
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), testutil.Params(1500))
	if len(diffs) > 0 {
		t.Fatalf("the exact search and the brute force differ:\n%s", strings.Join(diffs, "\n"))
	}
	if len(outs) != 2 || outs[0].Variant != "exact" || outs[1].Variant != "brute-force" {
		t.Fatalf("outputs: %v", outs)
	}
	if want := 2 * verifyRuns; len(outs[0].Lines) != want {
		t.Errorf("%d lines, want %d", len(outs[0].Lines), want)
	}
	for _, line := range outs[0].Lines {
		_, ids, _ := strings.Cut(line, ": ")
		if got := len(strings.Split(ids, ",")); got != k {
			t.Errorf("%q has %d ids", line, got)
		}
	}
}

func TestVariantsOfEachSystem(t *testing.T) {
	w := workload{}
	if w.Name() != "vector-search" || w.Group() != "vector" {
		t.Errorf("%s in %s", w.Name(), w.Group())
	}
	var got []string
	for _, v := range w.Variants(testutil.Kvutil(t)) {
		got = append(got, v.Name())
	}
	if strings.Join(got, ",") != "exact,partitioned,quantized" {
		t.Errorf("kvutil variants %v", got)
	}
}
