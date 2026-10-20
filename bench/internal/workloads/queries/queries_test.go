package queries

import (
	"sort"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/plans"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

// oracle answers a case from the books themselves, with no index and no database.
func oracle(c *qcase, books []*book, a args) answer {
	var found []*book
	for _, b := range books {
		if matches(c.name, b, a) {
			found = append(found, b)
		}
	}
	switch c.name {
	case "author-year-order":
		sort.Slice(found, func(i, j int) bool {
			if found[i].Year != found[j].Year {
				return found[i].Year > found[j].Year
			}
			return found[i].ID < found[j].ID
		})
		found = found[:min(10, len(found))]
	case "top-n-sort":
		sort.Slice(found, func(i, j int) bool {
			if found[i].Score != found[j].Score {
				return found[i].Score > found[j].Score
			}
			return found[i].ID < found[j].ID
		})
		found = found[:min(10, len(found))]
	case "deep-paging":
		sort.Slice(found, func(i, j int) bool {
			if found[i].Year != found[j].Year {
				return found[i].Year < found[j].Year
			}
			return found[i].ID < found[j].ID
		})
		from := min(int(a.I[0]), len(found))
		found = found[from:min(from+20, len(found))]
	}

	var ans answer
	switch c.kind {
	case countKind:
		ans.count = len(found)
	case selectKind:
		for _, b := range found {
			ans.lines = append(ans.lines, b.Title+"|"+itoa(b.Price))
		}
	default:
		for _, b := range found {
			ans.ids = append(ans.ids, b.ID)
			ans.lines = append(ans.lines, dataset.BookCanon(b))
		}
	}
	return ans
}

func hasTag(b *book, tag string) bool {
	for _, t := range b.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

func matches(name string, b *book, a args) bool {
	switch name {
	case "point-lookup":
		return b.Code == a.S[0]
	case "author-few", "author-many", "index-only-select":
		return b.Author == a.S[0]
	case "year-narrow", "year-wide", "top-n-sort", "count":
		return b.Year >= a.I[0] && b.Year <= a.I[1]
	case "author-year-order":
		return b.Author == a.S[0] && b.Year >= a.I[0] && b.Year <= a.I[1]
	case "deep-paging":
		return true
	case "in-list-20":
		for _, code := range a.S {
			if b.Code == code {
				return true
			}
		}
		return false
	case "or-two-indexes":
		return b.Author == a.S[0] || b.Year == a.I[0]
	case "and-two-indexes":
		return b.Year == a.I[0] && hasTag(b, a.S[0])
	case "tag-membership":
		return hasTag(b, a.S[0])
	case "like-prefix":
		return strings.HasPrefix(b.Title, a.S[0])
	case "lower-title":
		return strings.ToLower(b.Title) == a.S[0]
	case "conditional-index":
		return b.InPrint && b.Price >= a.I[0] && b.Price <= a.I[1]
	}
	panic("no oracle for " + name)
}

// Every way of asking kvutil answers every case as the books do, and so do the answers that
// the verification compares with PostgreSQL's.
func TestKvutilWaysAgreeWithTheBooks(t *testing.T) {
	p := testutil.Params(3000)
	outs, diffs := testutil.Verify(t, workload{}, testutil.Kvutil(t), p)
	if len(outs) != 3 {
		t.Fatalf("%d outputs, want 3", len(outs))
	}
	if len(diffs) > 0 {
		t.Fatalf("the ways differ:\n%s", strings.Join(diffs, "\n"))
	}

	spec := p.Dataset()
	n := int64(p.Scale)
	books := make([]*book, n)
	for i := range books {
		books[i] = spec.GenBook(int64(i) + 1)
	}
	d := newData(spec, n)
	pools := buildPools(d, p.Seed, "")

	nonEmpty := map[string]bool{}
	line := 0
	for _, c := range cases() {
		for i := 0; i < verifyRuns; i++ {
			a := pools[c.name][i].a
			ans := oracle(c, books, a)
			want := c.format(a, ans)
			for _, out := range outs {
				if got := out.Lines[line]; got != want {
					t.Errorf("%s: %s\n got  %s\n want %s", out.Variant, c.name, got, want)
				}
			}
			if len(ans.lines) > 0 || ans.count > 0 {
				nonEmpty[c.name] = true
			}
			line++
		}
	}
	for _, c := range cases() {
		if !nonEmpty[c.name] {
			t.Errorf("%s: every answer is empty", c.name)
		}
	}
}

// The statements of PostgreSQL have the parameters that the arguments give, and the text with the
// values in it has none left.
func TestStatementsAndTexts(t *testing.T) {
	spec := dataset.Spec{Seed: 1, Scale: 2000, Values: dataset.Small}
	d := newData(spec, 2000)
	pools := buildPools(d, 1, "kvb.books")
	if len(d.few) == 0 || len(d.many) == 0 {
		t.Fatalf("authors: %d with few books, %d with many", len(d.few), len(d.many))
	}
	for _, c := range cases() {
		st := c.statement("kvb.books")
		if !strings.HasPrefix(st, "SELECT ") || !strings.Contains(st, " FROM kvb.books") {
			t.Errorf("%s: %s", c.name, st)
		}
		for i, ent := range pools[c.name] {
			if i >= 50 {
				break
			}
			for k := range ent.pgArgs {
				if !strings.Contains(st, "$"+itoa(int64(k+1))) {
					t.Errorf("%s: no $%d in %s", c.name, k+1, st)
				}
			}
			if strings.Contains(ent.pgText, "$") {
				t.Errorf("%s: a parameter is left in %s", c.name, ent.pgText)
			}
			if ent.pgText != plans.Inline(st, ent.pgArgs) {
				t.Errorf("%s: %s", c.name, ent.pgText)
			}
		}
	}

	// Two builds of the pools are the same, and the cases of the pools are different.
	again := buildPools(d, 1, "kvb.books")
	other := buildPools(d, 2, "kvb.books")
	same, differ := 0, 0
	for name, pool := range pools {
		for i := range pool {
			if pool[i].pgText != again[name][i].pgText {
				t.Fatalf("%s: the pool is not the same twice", name)
			}
			if pool[i].pgText == other[name][i].pgText {
				same++
			} else {
				differ++
			}
		}
	}
	if differ < same {
		t.Errorf("a different seed gives the same arguments: %d same, %d differ", same, differ)
	}
}

func TestDataPicksAuthors(t *testing.T) {
	spec := dataset.Spec{Seed: 1, Scale: 100_000, Values: dataset.Small}
	d := newData(spec, 100_000)
	counts := spec.AuthorCounts(100_000)
	rank := map[string]int{}
	for r := range counts {
		rank[dataset.AuthorName(r)] = r
	}
	for _, a := range d.few {
		if c := counts[rank[a]]; c < 1 || c > 4 {
			t.Errorf("%s has %d books, not a few", a, c)
		}
	}
	if len(d.many) != 20 {
		t.Errorf("%d popular authors", len(d.many))
	}
	for _, a := range d.many {
		if r := rank[a]; r < 20 || r >= 40 || counts[r] < 50 {
			t.Errorf("%s of rank %d has %d books", a, r, counts[r])
		}
	}
	if d.offset != 10_000 {
		t.Errorf("offset %d", d.offset)
	}
	if small := newData(dataset.Spec{Seed: 1, Scale: 2000}, 2000); small.offset != 850 {
		t.Errorf("offset %d at scale 2000", small.offset)
	}
}

// Every case runs in every way, with the plan that kvutil chose recorded, and the planner reads
// the indexes that the cases were designed for.
func TestRunEmitsEveryCaseWithItsPlan(t *testing.T) {
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), testutil.Params(600))

	count := map[string]int{}
	plan := map[string]string{}
	for _, r := range rs {
		m := r.Metrics
		if r.Case == "load" || r.Case == "build" {
			if m.Records != 600 {
				t.Errorf("%s/%s: %+v", r.Variant, r.Case, m)
			}
			count[r.Variant+"/"+r.Case]++
			continue
		}
		if m.Ops == 0 || m.Errors != 0 {
			t.Errorf("%s/%s: ops=%d errors=%d %v", r.Variant, r.Case, m.Ops, m.Errors, m.ErrorSample)
		}
		if m.Info["plan_nodes"] == "" || m.Info["plan"] == "" {
			t.Errorf("%s/%s: no plan: %v", r.Variant, r.Case, m.Info)
		}
		count[r.Variant+"/"+r.Case]++
		plan[r.Case] = m.Info["plan_nodes"]
	}
	if count["typed/load"] != 1 || count["typed/build"] != 1 || count["sql/load"] != 0 {
		t.Errorf("the load and the build are reported by the first way only: %v", count)
	}
	for _, v := range []string{"typed", "sql", "prepared"} {
		for _, c := range cases() {
			if got := count[v+"/"+c.name]; got != 2 {
				t.Errorf("%s/%s: %d results, want 2", v, c.name, got)
			}
		}
	}

	for name, want := range map[string]string{
		"point-lookup":      "IndexLookup[by_code]",
		"author-year-order": "Limit > IndexLookup[by_author_year]",
		"top-n-sort":        "Limit > TopN > IndexLookup[by_year]",
		"or-two-indexes":    "Union > (IndexLookup[by_author] + IndexLookup[by_year])",
		"tag-membership":    "IndexLookup[by_tags]",
		"like-prefix":       "StoreScan",
		"lower-title":       "IndexLookup[by_title_lower]",
		"conditional-index": "IndexLookup[in_print_by_price]",
		"index-only-select": "IndexOnlyScan[by_author_cover]",
		"count":             "IndexOnlyScan[by_year]",
	} {
		if plan[name] != want {
			t.Errorf("%s: plan %q, want %q", name, plan[name], want)
		}
	}
	if !strings.HasPrefix(plan["and-two-indexes"], "Intersect") {
		t.Errorf("and-two-indexes: plan %q", plan["and-two-indexes"])
	}
}
