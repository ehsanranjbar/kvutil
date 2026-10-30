// Package relations is the workload of the relations between records: a parent and its children
// and a many-to-many relation with data on the pairs, as kvutil keeps them (relation.Associate and
// relation.New) and as PostgreSQL does (foreign keys with ON DELETE CASCADE, an index on the foreign
// key, a join table).
//
// There is one variant, relations, which has the cases of both pairs of stores:
//
//	load-*              the parents, the children (each with its parent checked), the records of
//	                    the two sides, the pairs
//	children-T          the children of a parent of the tier T, with their records; children-keys-T
//	                    their keys only; parent-of the parent of a child
//	insert-child        a new child under a parent drawn from the same law as the data
//	rights-M-T, lefts-M the records that a left record is related to (T the tier of the left
//	                    record) or that a right record is: M is keys, data (the pair's data) or
//	                    records (the records of the other side too)
//	pair-set, pair-get  upsert of a pair, and its data by both records
//	delete-parent-T, delete-left-T
//	                    deleting a record with its children or pairs (the cascade), in the tier T
//
// kvutil also finds how many children a cascade can delete in one transaction (cascade-limit).
package relations

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
)

func init() { harness.Register(workload{}) }

type workload struct{}

func (workload) Name() string  { return "relations" }
func (workload) Group() string { return "relations" }
func (workload) Description() string {
	return "parent/child (Associate vs foreign key with cascade) and many-to-many with pair data (Relation vs join table): inserts, lists, keys only, joins, cascading deletes; skewed numbers of children"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System, *pgsys.System:
		return []harness.Variant{&variant{}}
	}
	return nil
}

// listMode says how much of a list of related records is read.
type listMode string

const (
	modeKeys    listMode = "keys"    // the keys of the related records only
	modeData    listMode = "data"    // and the data of each pair
	modeRecords listMode = "records" // and the related records
)

var listModes = []listMode{modeKeys, modeData, modeRecords}

// target is the relations of a system.
type target interface {
	LoadParents(ctx context.Context, m *model, workers int) error
	LoadChildren(ctx context.Context, m *model, workers int) error
	LoadEntities(ctx context.Context, m *model, workers int) error
	LoadPairs(ctx context.Context, m *model, workers int) error

	// InsertChild adds a child under a parent, which has to exist.
	InsertChild(ctx context.Context, c *child, parent int64) error
	// Children reads the children of a parent with their records, in the order of their ids.
	Children(ctx context.Context, parent int64, fn func(*child)) error
	ChildKeys(ctx context.Context, parent int64, fn func(id int64)) error
	// ParentOf returns the parent of a child, or nil if the child does not exist.
	ParentOf(ctx context.Context, child int64) (*parent, error)
	// DeleteParent deletes a parent and its children.
	DeleteParent(ctx context.Context, id int64) error

	SetPair(ctx context.Context, left, right int64, d *pairData) error
	// GetPair returns the data of a pair, or nil if the records are not related.
	GetPair(ctx context.Context, left, right int64) (*pairData, error)
	// Rights reads what a left record is related to, in the order of the keys of the right
	// side; data and rec are nil in the modes that do not read them.
	Rights(ctx context.Context, left int64, mode listMode, fn func(id int64, d *pairData, rec *entity)) error
	Lefts(ctx context.Context, right int64, mode listMode, fn func(id int64, d *pairData, rec *entity)) error
	DeleteLeft(ctx context.Context, id int64) error
	DeleteRight(ctx context.Context, id int64) error

	// Count returns the number of parents, children, lefts, rights or pairs.
	Count(ctx context.Context, what string) (int64, error)
	Settle(ctx context.Context) error
	Sizes(ctx context.Context) (harness.Sizes, error)
	Close(ctx context.Context) error
}

type variant struct{}

func (*variant) Name() string { return "relations" }

func open(ctx context.Context, sys harness.System, p harness.Params) (target, error) {
	switch s := sys.(type) {
	case *kvsys.System:
		return openKV(ctx, s)
	case *pgsys.System:
		return openPG(ctx, s)
	}
	return nil, fmt.Errorf("relations does not run on %s", sys.Name())
}

func loadWorkers(p harness.Params) int { return min(max(p.Concurrency, 1), 4) }

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	p := rc.Params
	m := newModel(p.Seed, p.Scale, p.Values)
	t, err := open(ctx, rc.Sys, p)
	if err != nil {
		return err
	}
	defer t.Close(context.WithoutCancel(ctx))
	rc.SizeFn = t.Sizes

	shape := map[string]any{"parents": m.parents, "children": m.children, "lefts": m.lefts, "rights": m.rights, "pairs": m.pairs}
	w := loadWorkers(p)
	for _, ph := range []struct {
		name    string
		records int64
		load    func(context.Context, *model, int) error
	}{
		{"load-parents", m.parents, t.LoadParents},
		{"load-children", m.children, t.LoadChildren},
		{"load-entities", m.lefts + m.rights, t.LoadEntities},
		{"load-pairs", m.pairs, t.LoadPairs},
	} {
		err := rc.Phase(ctx, harness.Case{Name: ph.name, Concurrency: w, Params: shape}, func(ctx context.Context) (int64, error) {
			return ph.records, ph.load(ctx, m, w)
		})
		if err != nil {
			return err
		}
		if err := t.Settle(ctx); err != nil {
			return err
		}
	}

	ptiers, ltiers := m.parentTiers(), m.leftTiers()
	levels := harness.Levels(p.Concurrency)

	// The children of a parent.
	for _, tr := range ptiers {
		tp := map[string]any{"tier": tr.Name, "tier_mean_children": tr.meanSize, "tier_parents": len(tr.pool)}
		for _, c := range levels {
			err := rc.Measure(ctx, harness.Case{Name: "children-" + tr.Name, Concurrency: c, Unit: "children", Params: tp},
				func(ctx context.Context, w *harness.Worker) error {
					id := tr.pool[w.Rand.IntN(len(tr.pool))]
					w.Begin()
					n := 0
					err := t.Children(ctx, id, func(*child) { n++ })
					w.Add(n)
					return err
				})
			if err != nil {
				return err
			}
		}
		for _, c := range levels {
			err := rc.Measure(ctx, harness.Case{Name: "children-keys-" + tr.Name, Concurrency: c, Unit: "children", Params: tp},
				func(ctx context.Context, w *harness.Worker) error {
					id := tr.pool[w.Rand.IntN(len(tr.pool))]
					w.Begin()
					n := 0
					err := t.ChildKeys(ctx, id, func(int64) { n++ })
					w.Add(n)
					return err
				})
			if err != nil {
				return err
			}
		}
	}
	for _, c := range levels {
		err := rc.Measure(ctx, harness.Case{Name: "parent-of", Concurrency: c},
			func(ctx context.Context, w *harness.Worker) error {
				id := 1 + w.Rand.Int64N(m.children)
				w.Begin()
				pr, err := t.ParentOf(ctx, id)
				if err == nil && pr == nil {
					err = fmt.Errorf("child %d has no parent", id)
				}
				return err
			})
		if err != nil {
			return err
		}
	}

	// A new child under a parent that the data's law draws: the checks of the parent and the
	// writes of the references are in the time.
	var nextChild atomic.Int64
	nextChild.Store(m.children)
	for _, c := range levels {
		err := rc.Measure(ctx, harness.Case{Name: "insert-child", Concurrency: c, Init: func(w *harness.Worker) { w.State = &child{} }},
			func(ctx context.Context, w *harness.Worker) error {
				id := nextChild.Add(1)
				ch := w.State.(*child)
				m.genChild(id, ch)
				pid := m.parentFromUniform(w.Rand.Float64())
				w.Begin()
				return t.InsertChild(ctx, ch, pid)
			})
		if err != nil {
			return err
		}
	}

	// The many-to-many relation.
	for _, tr := range ltiers {
		if tr.Name == "few" {
			continue // the lists of the tier typical and of the tier many say what it costs
		}
		tp := map[string]any{"tier": tr.Name, "tier_mean_pairs": tr.meanSize, "tier_lefts": len(tr.pool)}
		for _, mode := range listModes {
			for _, c := range levels {
				err := rc.Measure(ctx, harness.Case{Name: fmt.Sprintf("rights-%s-%s", mode, tr.Name), Concurrency: c, Unit: "pairs", Params: tp},
					func(ctx context.Context, w *harness.Worker) error {
						id := tr.pool[w.Rand.IntN(len(tr.pool))]
						w.Begin()
						n := 0
						err := t.Rights(ctx, id, mode, func(int64, *pairData, *entity) { n++ })
						w.Add(n)
						return err
					})
				if err != nil {
					return err
				}
			}
		}
	}
	for _, mode := range listModes {
		for _, c := range levels {
			err := rc.Measure(ctx, harness.Case{Name: fmt.Sprintf("lefts-%s", mode), Concurrency: c, Unit: "pairs",
				Params: map[string]any{"mean_pairs": float64(m.pairs) / float64(m.rights)}},
				func(ctx context.Context, w *harness.Worker) error {
					id := 1 + w.Rand.Int64N(m.rights)
					w.Begin()
					n := 0
					err := t.Lefts(ctx, id, mode, func(int64, *pairData, *entity) { n++ })
					w.Add(n)
					return err
				})
			if err != nil {
				return err
			}
		}
	}
	for _, c := range levels {
		err := rc.Measure(ctx, harness.Case{Name: "pair-get", Concurrency: c},
			func(ctx context.Context, w *harness.Worker) error {
				l := 1 + w.Rand.Int64N(m.lefts)
				r := m.rightOf(l, w.Rand.Int64N(int64(m.leftCount[l])))
				w.Begin()
				d, err := t.GetPair(ctx, l, r)
				if err == nil && d == nil {
					err = fmt.Errorf("the pair (%d, %d) is not there", l, r)
				}
				return err
			})
		if err != nil {
			return err
		}
	}
	for _, c := range levels {
		err := rc.Measure(ctx, harness.Case{Name: "pair-set", Concurrency: c},
			func(ctx context.Context, w *harness.Worker) error {
				l := 1 + w.Rand.Int64N(m.lefts)
				r := 1 + w.Rand.Int64N(m.rights)
				d := m.data(l, r)
				d.Since += w.Rand.Int64N(1000)
				w.Begin()
				return t.SetPair(ctx, l, r, &d)
			})
		if err != nil {
			return err
		}
	}

	// The cascades use up what they delete: each level deletes its own part of the pool. Parents
	// with many children are the costly ones; the pool of a tier is split between the levels.
	for _, tr := range ptiers {
		if tr.Name == "typical" {
			continue
		}
		for i, c := range levels {
			pool := slice(tr.pool, i, len(levels))
			if len(pool) == 0 {
				continue
			}
			cur := newCursor(pool)
			err := rc.Measure(ctx, harness.Case{Name: "delete-parent-" + tr.Name, Concurrency: c, Ops: int64(len(pool)), WarmupOps: int64(len(pool)) / 10,
				Unit: "children", Params: map[string]any{"tier": tr.Name, "tier_mean_children": tr.meanSize, "pool": len(pool)}},
				func(ctx context.Context, w *harness.Worker) error {
					id, ok := cur.take()
					if !ok {
						return harness.ErrDone
					}
					w.Begin()
					err := t.DeleteParent(ctx, id)
					w.Add(int(m.childCount[id]))
					return err
				})
			if err != nil {
				return err
			}
		}
	}
	for _, tr := range ltiers {
		if tr.Name == "typical" {
			continue
		}
		for i, c := range levels {
			pool := slice(tr.pool, i, len(levels))
			if len(pool) == 0 {
				continue
			}
			cur := newCursor(pool)
			err := rc.Measure(ctx, harness.Case{Name: "delete-left-" + tr.Name, Concurrency: c, Ops: int64(len(pool)), WarmupOps: int64(len(pool)) / 10,
				Unit: "pairs", Params: map[string]any{"tier": tr.Name, "tier_mean_pairs": tr.meanSize, "pool": len(pool)}},
				func(ctx context.Context, w *harness.Worker) error {
					id, ok := cur.take()
					if !ok {
						return harness.ErrDone
					}
					w.Begin()
					err := t.DeleteLeft(ctx, id)
					w.Add(int(m.leftCount[id]))
					return err
				})
			if err != nil {
				return err
			}
		}
	}

	if kt, ok := t.(*kvTarget); ok {
		return kt.cascadeLimit(ctx, rc)
	}
	return nil
}

// cursor hands out the records of a pool one by one, each once.
type cursor struct {
	next atomic.Int64
	pool []int64
}

func newCursor(pool []int64) *cursor { return &cursor{pool: pool} }

func (c *cursor) take() (int64, bool) {
	i := c.next.Add(1) - 1
	if i >= int64(len(c.pool)) {
		return 0, false
	}
	return c.pool[i], true
}

// canon is the text of a child that is the same on both systems.
func (c *child) canon() string {
	sum := sha1.Sum(c.Payload)
	return fmt.Sprintf("%d|%s|%d|%s|%d:%s", c.ID, c.Title, c.Qty, c.Status, len(c.Payload), hex.EncodeToString(sum[:3]))
}

func digestOf(lines []string) string {
	sum := sha1.Sum([]byte(strings.Join(lines, "\n")))
	return hex.EncodeToString(sum[:6])
}

// Verify loads a small dataset, asks the lists of the children and of the pairs of some records,
// changes some, deletes some parents and records with their children and pairs, and asks again.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	lines, err := (&variant{}).verify(ctx, sys, p)
	if err != nil {
		return nil, err
	}
	return []harness.Output{{Variant: "relations", Lines: lines}}, nil
}

func (v *variant) verify(ctx context.Context, sys harness.System, p harness.Params) ([]string, error) {
	m := newModel(p.Seed, p.Scale, p.Values)
	t, err := open(ctx, sys, p)
	if err != nil {
		return nil, err
	}
	defer t.Close(context.WithoutCancel(ctx))
	for _, load := range []func(context.Context, *model, int) error{t.LoadParents, t.LoadChildren, t.LoadEntities, t.LoadPairs} {
		if err := load(ctx, m, 2); err != nil {
			return nil, err
		}
	}

	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	counts := func(stage string) error {
		var parts []string
		for _, what := range []string{"parents", "children", "lefts", "rights", "pairs"} {
			n, err := t.Count(ctx, what)
			if err != nil {
				return err
			}
			parts = append(parts, fmt.Sprintf("%s=%d", what, n))
		}
		add("%s: %s", stage, strings.Join(parts, " "))
		return nil
	}

	childrenOf := func(pid int64) error {
		var cs []string
		var keys []int64
		if err := t.Children(ctx, pid, func(c *child) { cs = append(cs, c.canon()) }); err != nil {
			return err
		}
		if err := t.ChildKeys(ctx, pid, func(id int64) { keys = append(keys, id) }); err != nil {
			return err
		}
		agree := len(keys) == len(cs)
		add("children of %d: %d, the keys agree: %t, digest %s", pid, len(cs), agree, digestOf(cs))
		return nil
	}
	parentOf := func(cid int64) error {
		pr, err := t.ParentOf(ctx, cid)
		if err != nil {
			return err
		}
		if pr == nil {
			add("parent of %d: none", cid)
			return nil
		}
		add("parent of %d: %d %s %s %d", cid, pr.ID, pr.Name, pr.Region, pr.Created.Unix())
		return nil
	}
	pairsOf := func(stage string, left, right int64) error {
		for _, mode := range listModes {
			var rs []string
			err := t.Rights(ctx, left, mode, func(id int64, d *pairData, rec *entity) { rs = append(rs, describePair(id, d, rec)) })
			if err != nil {
				return err
			}
			add("%s rights of %d (%s): %d, digest %s", stage, left, mode, len(rs), digestOf(rs))
			var ls []string
			err = t.Lefts(ctx, right, mode, func(id int64, d *pairData, rec *entity) { ls = append(ls, describePair(id, d, rec)) })
			if err != nil {
				return err
			}
			add("%s lefts of %d (%s): %d, digest %s", stage, right, mode, len(ls), digestOf(ls))
		}
		return nil
	}

	if err := counts("loaded"); err != nil {
		return nil, err
	}
	// The parents with the most, some in the middle and the last, and a child of each.
	sample := []int64{1, 2, 3, m.parents / 4, m.parents / 2, m.parents}
	for _, pid := range sample {
		if err := childrenOf(pid); err != nil {
			return nil, err
		}
	}
	for _, cid := range []int64{1, 17, m.children / 2, m.children} {
		if err := parentOf(cid); err != nil {
			return nil, err
		}
	}
	for _, l := range []int64{1, 2, 3, m.lefts / 2} {
		if err := pairsOf("loaded", l, 1+(l*7)%m.rights); err != nil {
			return nil, err
		}
	}
	for _, l := range []int64{1, 4} {
		for j := int64(0); j < 3; j++ {
			r := m.rightOf(l, j)
			d, err := t.GetPair(ctx, l, r)
			if err != nil {
				return nil, err
			}
			add("pair (%d, %d): %s", l, r, describeData(d))
		}
	}
	absent := int64(0)
	for r := int64(1); r <= m.rights; r++ {
		taken := false
		for j := int64(0); j < int64(m.leftCount[2]); j++ {
			if m.rightOf(2, j) == r {
				taken = true
			}
		}
		if !taken {
			absent = r
			break
		}
	}
	if absent > 0 {
		d, err := t.GetPair(ctx, 2, absent)
		if err != nil {
			return nil, err
		}
		add("pair (2, %d): %s", absent, describeData(d))
	}

	// Changes: a child under a parent, a pair set and a pair set again with other data.
	c := &child{}
	m.genChild(m.children+1, c)
	if err := t.InsertChild(ctx, c, 5); err != nil {
		return nil, err
	}
	m.genChild(m.children+2, c)
	if err := t.InsertChild(ctx, c, m.parents+1000); err == nil {
		add("a child under a parent that does not exist was accepted")
	} else {
		add("a child under a parent that does not exist: rejected")
	}
	for _, pr := range [][2]int64{{3, 7}, {3, 7}, {1, m.rightOf(1, 0)}} {
		d := m.data(pr[0], pr[1])
		d.Since += 5
		if err := t.SetPair(ctx, pr[0], pr[1], &d); err != nil {
			return nil, err
		}
	}
	if err := counts("changed"); err != nil {
		return nil, err
	}
	if err := childrenOf(5); err != nil {
		return nil, err
	}
	if err := pairsOf("changed", 3, 7); err != nil {
		return nil, err
	}

	// The cascades: the parent with the most children, one with few, a left and a right record.
	gone := m.parents
	for _, pid := range []int64{1, gone} {
		if err := t.DeleteParent(ctx, pid); err != nil {
			return nil, err
		}
	}
	if err := t.DeleteLeft(ctx, 1); err != nil {
		return nil, err
	}
	if err := t.DeleteRight(ctx, m.rightOf(2, 1)); err != nil {
		return nil, err
	}
	if err := counts("after the cascades"); err != nil {
		return nil, err
	}
	for _, pid := range []int64{1, gone, 2} {
		if err := childrenOf(pid); err != nil {
			return nil, err
		}
	}
	for id := int64(1); id <= m.children; id++ {
		if m.parentOf(id) == 1 {
			if err := parentOf(id); err != nil {
				return nil, err
			}
			break
		}
	}
	if err := pairsOf("after the cascades", 1, m.rightOf(2, 1)); err != nil {
		return nil, err
	}
	if err := pairsOf("after the cascades", 2, m.rightOf(2, 2)); err != nil {
		return nil, err
	}
	return lines, nil
}

func describeData(d *pairData) string {
	if d == nil {
		return "not related"
	}
	return fmt.Sprintf("%s/%d", d.Role, d.Since)
}

func describePair(id int64, d *pairData, rec *entity) string {
	s := fmt.Sprint(id)
	if d != nil {
		s += "|" + describeData(d)
	}
	if rec != nil {
		s += fmt.Sprintf("|%s/%s", rec.Name, rec.Kind)
	}
	return s
}
