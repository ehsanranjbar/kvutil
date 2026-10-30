package relations

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/testutil"
)

func TestModelIsConsistent(t *testing.T) {
	m := newModel(1, 10_000, dataset.Small)
	if m.parents != 1000 || m.lefts != 500 || m.rights != 1000 {
		t.Fatalf("parents %d lefts %d rights %d", m.parents, m.lefts, m.rights)
	}
	var children int64
	for id := int64(1); id <= m.parents; id++ {
		children += int64(m.childCount[id])
	}
	if children != m.children {
		t.Errorf("the parents have %d children in all, want %d", children, m.children)
	}
	// The law is skewed: the first parent has about ln 2 / ln(P+1) of the children.
	if share := float64(m.childCount[1]) / float64(m.children); share < 0.08 || share > 0.2 {
		t.Errorf("the first parent has %.3f of the children", share)
	}

	// The pairs of a left record are all different, and their number is the one of the model.
	var pairs int64
	for l := int64(1); l <= m.lefts; l++ {
		seen := map[int64]bool{}
		for j := int64(0); j < int64(m.leftCount[l]); j++ {
			r := m.rightOf(l, j)
			if r < 1 || r > m.rights || seen[r] {
				t.Fatalf("left %d: pair %d is the right record %d, out of range or taken", l, j, r)
			}
			seen[r] = true
			pairs++
		}
	}
	if pairs != m.pairs {
		t.Errorf("%d pairs, the model says %d", pairs, m.pairs)
	}

	names := map[string]bool{}
	for _, tr := range m.parentTiers() {
		names[tr.Name] = true
		for _, id := range tr.pool {
			if c := m.childCount[id]; c < tr.Lo || c > tr.Hi {
				t.Errorf("the parent %d of the tier %s has %d children", id, tr.Name, c)
			}
		}
	}
	for _, n := range []string{"few", "typical", "many"} {
		if !names[n] {
			t.Errorf("no parent in the tier %s at 10,000 children", n)
		}
	}
	if len(m.leftTiers()) == 0 {
		t.Error("no left record in any tier")
	}
}

func TestPairAtWalksThePairsInOrder(t *testing.T) {
	m := newModel(1, 10_000, dataset.Small)
	i := int64(0)
	for l := int64(1); l <= m.lefts; l++ {
		for j := int64(0); j < int64(m.leftCount[l]); j++ {
			if gotL, gotR := m.pairAt(i); gotL != l || gotR != m.rightOf(l, j) {
				t.Fatalf("pair %d is (%d, %d), want (%d, %d)", i, gotL, gotR, l, m.rightOf(l, j))
			}
			i++
		}
	}
	if i != m.pairs {
		t.Errorf("walked %d pairs, the model has %d", i, m.pairs)
	}
}

func TestKvutilVerifyMatchesTheModel(t *testing.T) {
	p := testutil.Params(2000)
	outs, _ := testutil.Verify(t, workload{}, testutil.Kvutil(t), p)
	if len(outs) != 1 {
		t.Fatalf("%d outputs", len(outs))
	}
	m := newModel(p.Seed, p.Scale, p.Values)
	lines := outs[0].Lines
	has := func(want string) {
		t.Helper()
		for _, l := range lines {
			if strings.Contains(l, want) {
				return
			}
		}
		t.Errorf("no line has %q in\n%s", want, strings.Join(lines, "\n"))
	}

	has(fmt.Sprintf("loaded: parents=%d children=%d lefts=%d rights=%d pairs=%d", m.parents, m.children, m.lefts, m.rights, m.pairs))
	has(fmt.Sprintf("children of 1: %d, the keys agree: true", m.childCount[1]))
	has(fmt.Sprintf("children of %d: %d, the keys agree: true", m.parents, m.childCount[m.parents]))
	has("a child under a parent that does not exist: rejected")

	// A child and a pair were added; the cascades took the children of two parents, and the pairs
	// of a left and of a right record.
	children := m.children + 1 - int64(m.childCount[1]) - int64(m.childCount[m.parents])
	has(fmt.Sprintf("after the cascades: parents=%d children=%d lefts=%d rights=%d", m.parents-2, children, m.lefts-1, m.rights-1))
	has("children of 1: 0, the keys agree: true")
	has(fmt.Sprintf("children of %d: 0, the keys agree: true", m.parents))
	has("after the cascades rights of 1 (keys): 0")
	for _, l := range lines {
		if strings.Contains(l, "was accepted") {
			t.Error(l)
		}
	}
}

func TestRunEmitsTheCases(t *testing.T) {
	p := testutil.Params(3000)
	rs := testutil.Run(t, workload{}, testutil.Kvutil(t), p)

	seen := map[string]bool{}
	for _, r := range rs {
		seen[r.Case] = true
		switch {
		case strings.HasPrefix(r.Case, "load-"), r.Case == "cascade-limit":
		default:
			if r.Metrics.Ops == 0 || r.Metrics.Errors != 0 {
				t.Errorf("%s: ops=%d errors=%d %v", r.Case, r.Metrics.Ops, r.Metrics.Errors, r.Metrics.ErrorSample)
			}
		}
		if r.Case == "cascade-limit" {
			x := r.Metrics.Extra
			if x["largest_cascade_that_fit_children"] < 1024 {
				t.Errorf("cascade limit: %v", x)
			}
			t.Logf("cascade limit: %v", x)
		}
	}
	for _, c := range []string{
		"load-parents", "load-children", "load-entities", "load-pairs",
		"children-few", "children-typical", "children-many", "children-keys-few", "children-keys-many",
		"parent-of", "insert-child", "rights-keys-typical", "rights-data-many", "rights-records-many",
		"lefts-keys", "lefts-data", "lefts-records", "pair-get", "pair-set",
		"delete-parent-few", "delete-parent-many", "delete-left-few", "delete-left-many", "cascade-limit",
	} {
		if !seen[c] {
			t.Errorf("no result for %s", c)
		}
	}
}

func TestStatementsAreComplete(t *testing.T) {
	// A statement that is formatted wrong has a %! in it.
	for _, stmt := range []string{
		sqlInsertChild, sqlChildren, sqlChildKeys, sqlParentOf, sqlSetPair, sqlGetPair,
		sqlRightsKeys, sqlRightsData, sqlRightsRecords, sqlLeftsKeys, sqlLeftsData, sqlLeftsRecords,
	} {
		if got := fmt.Sprintf(stmt, "s"); strings.Contains(got, "%!") || strings.Contains(got, "%[") {
			t.Errorf("statement is wrong: %s", got)
		}
	}
	if got := fmt.Sprintf(sqlDeleteOf, "s", "parents"); got != "DELETE FROM s.parents WHERE id = $1" {
		t.Errorf("delete statement is %q", got)
	}
	if ddl := fmt.Sprintf(pgSchema, "s"); strings.Contains(ddl, "%!") || strings.Count(ddl, "ON DELETE CASCADE") != 3 {
		t.Errorf("schema is wrong:\n%s", ddl)
	}
	for _, m := range listModes {
		if titleCase(m) == string(m) {
			t.Errorf("titleCase(%s) did not change it", m)
		}
	}
}

// The levels of a cascade case delete a part of the pool each. The pool is in the order of the
// ids, so the first part has the largest records; shuffled, every part has about the same.
func TestShuffledPoolsSplitEvenly(t *testing.T) {
	m := newModel(1, 200_000, dataset.Small)
	sum := func(pool []int64, counts []int32, i int) (n int64) {
		for _, id := range slice(pool, i, 4) {
			n += int64(counts[id])
		}
		return n
	}
	checked := 0
	defer func() {
		if checked == 0 {
			t.Error("no tier was big enough to check")
		}
	}()
	for _, kind := range []struct {
		tiers  []tier
		counts []int32
	}{{m.parentTiers(), m.childCount}, {m.leftTiers(), m.leftCount}} {
		for _, tr := range kind.tiers {
			if len(tr.pool) < 100 {
				continue
			}
			checked++
			shuf := shuffled(tr.pool, 1)
			if len(shuf) != len(tr.pool) || fmt.Sprint(shuf) != fmt.Sprint(shuffled(tr.pool, 1)) || fmt.Sprint(shuf) == fmt.Sprint(shuffled(tr.pool, 2)) {
				t.Fatalf("tier %s: the shuffle must keep the records, and depend on the seed and on nothing else", tr.Name)
			}
			seen := map[int64]bool{}
			for _, id := range shuf {
				seen[id] = true
			}
			if len(seen) != len(tr.pool) {
				t.Errorf("tier %s: the shuffle lost records", tr.Name)
			}
			lo, hi := int64(1<<62), int64(0)
			for i := 0; i < 4; i++ {
				n := sum(shuf, kind.counts, i)
				lo, hi = min(lo, n), max(hi, n)
			}
			if float64(hi) > 1.25*float64(lo) {
				t.Errorf("tier %s: the parts of the shuffled pool delete %d to %d, not about the same", tr.Name, lo, hi)
			}
		}
	}
}
