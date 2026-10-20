package query_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/internal/keyhash"
	"github.com/ehsanranjbar/kvutil/internal/testkv"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// TestCombinedPlans checks the plans that read several indexes, as Explain shows them.
func TestCombinedPlans(t *testing.T) {
	items := defineItems()
	from := func() *query.Query[int64, Item] { return query.From(items) }

	for _, test := range []struct {
		name string
		q    *query.Query[int64, Item]
		want string
	}{
		// A disjunction is a union of the indexes that serve its branches.
		{"or of two indexes", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n└─ IndexLookup by_code: 1 range"},
		{"or of three branches", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"), tags.Eq("t1"))),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n├─ IndexLookup by_code: 1 range\n└─ IndexLookup by_tag_rank: 1 range"},
		{"the other conditions are those of each branch", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005")), extra.Eq(3)),
			"Union\n├─ IndexLookup by_group_rank: 1 range, then check [Extra = 3]\n└─ IndexLookup by_code: 1 range, then check [Extra = 3]"},
		{"a branch of two conditions", from().Where(cond.Or(cond.And(group.Eq("a"), rank.Gt(5)), code.Eq("i0005"))),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n└─ IndexLookup by_code: 1 range"},
		{"the other condition makes a branch cheaper", from().Where(cond.Or(group.Eq("a"), extra.Eq(1)), code.Eq("i0005")),
			"IndexLookup by_code: 1 range, then check [(Group = \"a\") or (Extra = 1)]"},
		{"a union that is dearer than an index", from().Where(cond.Or(extra.Eq(1), extra.Eq(2), note.Eq("n1")), group.Eq("a")),
			"IndexLookup by_group_rank: 1 range, then check [(Extra = 1) or (Extra = 2) or (Note = \"n1\")]"},
		{"a branch that no index serves", from().Where(cond.Or(group.Eq("a"), extra.Eq(1))),
			`StoreScan, then check [(Group = "a") or (Extra = 1)]`},
		{"a branch of a field that is optional", from().Where(cond.Or(group.Eq("a"), label.Eq("l1"))),
			`StoreScan, then check [(Group = "a") or (Label = "l1")]`},
		{"a branch that nothing satisfies is left out", from().Where(cond.Or(group.Eq("a"), cond.And(code.Eq("i0005"), code.Eq("i0006")), tags.Eq("t1"))),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n└─ IndexLookup by_tag_rank: 1 range"},
		{"a union of records of an index of some", from().Where(cond.Or(
			cond.And(ready.Eq(true), rank.Eq(3)), cond.And(ready.Eq(true), code.Eq("i0007")))),
			"Union\n├─ IndexLookup ready_by_rank: 1 range, then check [Ready = true]\n└─ IndexLookup by_code: 1 range, then check [Ready = true]"},
		{"two disjunctions, the cheapest is read", from().Where(
			cond.Or(group.Eq("a"), tags.Eq("t1")), cond.Or(code.Eq("i0005"), code.Eq("i0007"), note.Eq("n1"))),
			"Union\n├─ IndexLookup by_code: 1 range, then check [(Group = \"a\") or (Tags = \"t1\")]\n├─ IndexLookup by_code: 1 range, then check [(Group = \"a\") or (Tags = \"t1\")]\n└─ IndexLookup by_note: 1 range, then check [(Group = \"a\") or (Tags = \"t1\")]"},

		// The records of a union come in no order.
		{"union, sorted", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).OrderBy(rank),
			"Sort by Rank\n└─ Union\n   ├─ IndexLookup by_group_rank: 1 range\n   └─ IndexLookup by_code: 1 range"},
		{"union, top records", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).OrderBy(rank.Desc(), code).Limit(10).Offset(2),
			"Limit 10, offset 2\n└─ TopN by Rank desc, Code\n   └─ Union\n      ├─ IndexLookup by_group_rank: 1 range\n      └─ IndexLookup by_code: 1 range"},
		{"union, a limit stops early", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).Limit(5),
			"Limit 5\n└─ Union\n   ├─ IndexLookup by_group_rank: 1 range\n   └─ IndexLookup by_code: 1 range"},
		{"union, an offset", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).Offset(5),
			"Offset 5\n└─ Union\n   ├─ IndexLookup by_group_rank: 1 range\n   └─ IndexLookup by_code: 1 range"},

		// A disjunction of one field is a condition of one index.
		{"or of values is in", from().Where(cond.Or(group.Eq("a"), group.Eq("b"), group.In("c"))),
			`IndexLookup by_group_rank: 3 ranges`},
		{"or of values is in, in order", from().Where(cond.Or(group.Eq("b"), group.Eq("a"))).OrderBy(group, rank.Desc()),
			`IndexLookup by_group_rank (in order of Group, Rank desc): 2 ranges`},
		{"or of values is in, of several values", from().Where(cond.Or(tags.Eq("t1"), tags.Eq("t2"))),
			`IndexLookup by_tag_rank: 2 ranges`},
		{"or of values is in, of a unique field", from().Where(cond.Or(code.Eq("i0005"), code.In("i0006", "i0005"))),
			`IndexLookup by_code: 2 ranges`},
		{"or of values and another condition", from().Where(cond.Or(group.Eq("a"), group.Eq("b")), rank.Gt(3)),
			`IndexLookup by_group_rank: 2 ranges`},
		{"or of values is merged with the other values", from().Where(cond.Or(group.Eq("a"), group.Eq("b")), group.In("b", "c")),
			`IndexLookup by_group_rank: 1 range`},
		{"or of values that nothing satisfies", from().Where(cond.Or(group.Eq("a"), group.Eq("b")), group.Eq("c")),
			`Empty`},
		{"or of a value and a range stays", from().Where(cond.Or(group.Eq("a"), group.Gt("c"))),
			`StoreScan, then check [(Group = "a") or (Group > "c")]`},
		{"or of a field with a function stays", from().Where(cond.Or(group.Eq("a"), cond.Func("odd", func(it *Item) bool { return it.Rank%2 == 1 }))),
			`StoreScan, then check [(Group = "a") or (odd())]`},

		// A conjunction is an intersection of the indexes that serve its conditions.
		{"and of two indexes", from().Where(group.Eq("a"), tags.Eq("t1")),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"and of two indexes, and a condition", from().Where(group.Eq("a"), tags.Eq("t1"), extra.Eq(1)),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"; Extra = 1]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"and of two indexes, of which one has too many keys", from().Where(group.Eq("a"), note.Gte("n1")),
			"IndexLookup by_group_rank: 1 range, then check [Note >= \"n1\"]"},
		{"and of two indexes that serve a range", from().Where(group.Between("a", "c"), note.Between("n1", "n2")),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Note >= \"n1\" and Note <= \"n2\"]\n└─ IndexKeys by_note: 1 range"},
		{"and of two indexes that serve a range, with no room", from().Where(group.Between("a", "c"), note.Between("n1", "n2")).MaxKeys(99_999),
			"IndexLookup by_group_rank: 1 range, then check [Note >= \"n1\" and Note <= \"n2\"]"},
		{"and of indexes, one of which is enough", from().Where(code.Eq("i0007"), group.Eq("a")),
			`IndexLookup by_group_code: 1 range`},
		{"and of indexes, one of which is unique", from().Where(code.Eq("i0007"), tags.Eq("t1")),
			"IndexLookup by_code: 1 range, then check [Tags = \"t1\"]"},
		{"and of indexes of conditions that are the same", from().Where(group.Eq("a"), rank.Eq(3)),
			`IndexLookup by_group_rank: 1 range`},
		{"and of indexes that are wide", from().Where(group.Gte("b"), note.Gte("n1")),
			"IndexLookup by_group_rank: 1 range, then check [Note >= \"n1\"]"},
		{"and of an index of some records, which is enough", from().Where(ready.Eq(true), rank.Eq(3), note.Eq("n1")),
			"IndexLookup ready_by_rank: 1 range, then check [Ready = true; Note = \"n1\"]"},
		{"and with a function", from().Where(group.Eq("a"), tags.Eq("t1"), cond.Func("odd", func(it *Item) bool { return it.Rank%2 == 1 })),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"; odd()]\n└─ IndexKeys by_tag_rank: 1 range"},

		// The order of an intersection is that of the index that is streamed.
		{"intersection, in order of the index", from().Where(group.Eq("a"), tags.Eq("t1")).OrderBy(rank.Desc()),
			"Intersect\n├─ IndexLookup by_group_rank (in order of Rank desc): 1 range, then check [Tags = \"t1\"]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"intersection, in order of the other index", from().Where(group.Eq("a"), tags.Eq("t1")).OrderBy(extra),
			"Sort by Extra\n└─ Intersect\n   ├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n   └─ IndexKeys by_tag_rank: 1 range"},
		{"intersection, top records", from().Where(group.Eq("a"), tags.Eq("t1")).OrderBy(extra).Limit(5),
			"Limit 5\n└─ TopN by Extra\n   └─ Intersect\n      ├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n      └─ IndexKeys by_tag_rank: 1 range"},
		{"a limit of an order that one index gives is enough", from().Where(group.Eq("a"), tags.Eq("t1")).OrderBy(rank.Desc()).Limit(5),
			"Limit 5\n└─ IndexLookup by_group_rank (in order of Rank desc): 1 range, then check [Tags = \"t1\"]"},
		{"intersection, a limit that a scan is enough for", from().Where(group.Eq("a"), tags.Eq("t1")).Limit(5),
			"Limit 5\n└─ StoreScan, then check [Group = \"a\"; Tags = \"t1\"]"},
		{"intersection, a limit that is forced", from().Where(group.Eq("a"), tags.Eq("t1")).Limit(5).UsingAll("by_group_rank", "by_tag_rank"),
			"Limit 5\n└─ Intersect\n   ├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n   └─ IndexKeys by_tag_rank: 1 range"},

		// The keys that are held in memory.
		{"no room for keys, and", from().Where(group.Eq("a"), tags.Eq("t1")).MaxKeys(0),
			"IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]"},
		{"room for few keys, and", from().Where(group.Eq("a"), tags.Eq("t1")).MaxKeys(9_999),
			"IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]"},
		{"room for the keys, and", from().Where(group.Eq("a"), tags.Eq("t1")).MaxKeys(10_000),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"no room for keys, or", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).MaxKeys(0),
			`StoreScan, then check [(Group = "a") or (Code = "i0005")]`},
		{"room for few keys, or", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).MaxKeys(9_000),
			`StoreScan, then check [(Group = "a") or (Code = "i0005")]`},
		{"room for the keys, or", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).MaxKeys(10_001),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n└─ IndexLookup by_code: 1 range"},
		{"the keys of a unique index are few", from().Where(cond.Or(code.Eq("i0005"), tags.Eq("t1"), code.In("i0001", "i0002"))).MaxKeys(10_005),
			"Union\n├─ IndexLookup by_code: 1 range\n├─ IndexLookup by_tag_rank: 1 range\n└─ IndexLookup by_code: 2 ranges"},

		// The hints.
		{"all, an intersection", from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_tag_rank", "by_group_rank"),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"all, an intersection that is dearer", from().Where(code.Eq("i0007"), tags.Eq("t1")).UsingAll("by_code", "by_tag_rank"),
			"Intersect\n├─ IndexLookup by_tag_rank: 1 range, then check [Code = \"i0007\"]\n└─ IndexKeys by_code: 1 range"},
		{"all, an intersection of three", from().Where(group.Eq("a"), tags.Eq("t1"), note.Eq("n1")).UsingAll("by_note", "by_tag_rank", "by_group_rank"),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"; Note = \"n1\"]\n├─ IndexKeys by_tag_rank: 1 range\n└─ IndexKeys by_note: 1 range"},
		{"all, an intersection of three, in order", from().Where(group.Eq("a"), tags.Eq("t1"), note.Eq("n1")).OrderBy(rank).UsingAll("by_note", "by_tag_rank", "by_group_rank"),
			"Sort by Rank\n└─ Intersect\n   ├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"; Note = \"n1\"]\n   ├─ IndexKeys by_tag_rank: 1 range\n   └─ IndexKeys by_note: 1 range"},
		{"all, an intersection of three, in order of the first", from().Where(group.Eq("a"), tags.Eq("t1"), note.Eq("n1")).OrderBy(rank.Desc()).UsingAll("by_note", "by_tag_rank", "by_group_rank"),
			"Intersect\n├─ IndexLookup by_group_rank (in order of Rank desc): 1 range, then check [Tags = \"t1\"; Note = \"n1\"]\n├─ IndexKeys by_tag_rank: 1 range\n└─ IndexKeys by_note: 1 range"},
		{"all, a union", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).UsingAll("by_code", "by_group_rank"),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n└─ IndexLookup by_code: 1 range"},
		{"all, a union that is not the cheapest", from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).UsingAll("by_group_code", "by_code"),
			"Union\n├─ IndexLookup by_group_code: 1 range\n└─ IndexLookup by_code: 1 range"},
		{"all, a union of one index", from().Where(cond.Or(cond.And(group.Eq("a"), rank.Eq(1)), cond.And(group.Eq("b"), rank.Eq(2)))).UsingAll("by_group_rank"),
			"Union\n├─ IndexLookup by_group_rank: 1 range\n└─ IndexLookup by_group_rank: 1 range"},
		{"the last hint wins, all then one", from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_tag_rank", "by_group_rank").Using("by_tag_rank"),
			"IndexLookup by_tag_rank: 1 range, then check [Group = \"a\"]"},
		{"the last hint wins, one then all", from().Where(group.Eq("a"), tags.Eq("t1")).Using("by_tag_rank").UsingAll("by_tag_rank", "by_group_rank"),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"the last hint wins, all then a scan", from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_tag_rank", "by_group_rank").FullScan(),
			`StoreScan, then check [Group = "a"; Tags = "t1"]`},
		{"the last hint wins, a scan then all", from().Where(group.Eq("a"), tags.Eq("t1")).FullScan().UsingAll("by_tag_rank", "by_group_rank"),
			"Intersect\n├─ IndexLookup by_group_rank: 1 range, then check [Tags = \"t1\"]\n└─ IndexKeys by_tag_rank: 1 range"},
		{"empty with all", from().Where(rank.Gt(5), rank.Lt(3)).UsingAll("by_code", "by_group_rank"), `Empty`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.q.Explain()
			require.NoError(t, err)
			require.Equal(t, test.want, got)
			// A query is planned once.
			again, err := test.q.Explain()
			require.NoError(t, err)
			require.Equal(t, got, again)
		})
	}
}

func TestCombinedHintsThatCannotBeFollowed(t *testing.T) {
	items := defineItems()
	from := func() *query.Query[int64, Item] { return query.From(items) }

	for name, q := range map[string]*query.Query[int64, Item]{
		"an index that there is not":    from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank", "nope"),
		"nothing":                       from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll(),
		"one index of an intersection":  from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank"),
		"an index that serves nothing":  from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank", "by_note"),
		"an index of the same":          from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank", "by_group_code"),
		"an index that cannot be read":  from().Where(group.Eq("a"), score.Eq(2)).UsingAll("by_group_rank", "by_score_label"),
		"an index of some records":      from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank", "ready_by_rank"),
		"an index too many":             from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank", "by_tag_rank", "by_note"),
		"no room for keys":              from().Where(group.Eq("a"), tags.Eq("t1")).MaxKeys(0).UsingAll("by_group_rank", "by_tag_rank"),
		"room for few keys":             from().Where(group.Eq("a"), tags.Eq("t1")).UsingAll("by_group_rank", "by_tag_rank").MaxKeys(100),
		"a union with no room":          from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).MaxKeys(0).UsingAll("by_group_rank", "by_code"),
		"a union of too few indexes":    from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).UsingAll("by_code"),
		"a union of too many indexes":   from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).UsingAll("by_code", "by_group_rank", "by_note"),
		"a union with a branch left":    from().Where(cond.Or(group.Eq("a"), extra.Eq(1))).UsingAll("by_group_rank", "by_code"),
		"a union and a scan":            from().Where(cond.Or(group.Eq("a"), code.Eq("i0005"))).UsingAll("by_group_rank", "by_note"),
		"neither an or nor an and":      from().OrderBy(code).UsingAll("by_code", "by_group_code"),
		"unknown, and a limit of 0":     from().Where(group.Eq("a")).Limit(0).UsingAll("nope", "by_code"),
		"unknown, and nothing to read":  from().Where(rank.Gt(5), rank.Lt(3)).UsingAll("nope", "by_code"),
		"an index of one that is wrong": from().Where(group.Eq("a"), tags.Eq("t1")).Using("by_note").UsingAll("by_note", "by_code"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := q.Explain()
			require.ErrorIs(t, err, query.ErrHint)

			// Running it fails the same, through the iterator.
			require.NoError(t, testkv.NewMem(t).View(func(tx kv.Tx) error {
				it := q.Run(tx)
				defer it.Close()
				require.False(t, it.Valid())
				require.ErrorIs(t, it.Err(), query.ErrHint)
				return nil
			}))
		})
	}

	t.Run("negative number of keys", func(t *testing.T) {
		_, err := from().Where(group.Eq("a")).MaxKeys(-1).Explain()
		require.ErrorIs(t, err, query.ErrNegative)
		// The last number is the one.
		_, err = from().Where(group.Eq("a")).MaxKeys(-1).MaxKeys(5).Explain()
		require.NoError(t, err)

		require.NoError(t, testkv.NewMem(t).View(func(tx kv.Tx) error {
			it := from().MaxKeys(-3).Run(tx)
			defer it.Close()
			require.False(t, it.Valid())
			require.ErrorIs(t, it.Err(), query.ErrNegative)
			return nil
		}))
	})
}

// TestOrBecomesIn checks that a disjunction of the values of one field gives the records of the
// values, in every plan.
func TestOrBecomesIn(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		from := func() *query.Query[int64, Item] { return query.From(f.store) }

		for name, test := range map[string]struct {
			q     *query.Query[int64, Item]
			where []cond.Cond[Item]
		}{
			"values": {
				from().Where(cond.Or(group.Eq("a"), group.Eq("c"), group.Eq("zz"))),
				[]cond.Cond[Item]{cond.Or(group.Eq("a"), group.Eq("c"), group.Eq("zz"))},
			},
			"values and in": {
				from().Where(cond.Or(rank.In(1, 2), rank.Eq(2), rank.Eq(7))).OrderBy(code),
				[]cond.Cond[Item]{cond.Or(rank.In(1, 2), rank.Eq(2), rank.Eq(7))},
			},
			"values of several": {
				from().Where(cond.Or(tags.Eq("t0"), tags.Eq("t3"))),
				[]cond.Cond[Item]{cond.Or(tags.Eq("t0"), tags.Eq("t3"))},
			},
			"values of an optional field": {
				from().Where(cond.Or(note.Eq("n0"), note.Eq("n2"))).OrderBy(code.Desc()),
				[]cond.Cond[Item]{cond.Or(note.Eq("n0"), note.Eq("n2"))},
			},
			"values of a field with no index": {
				from().Where(cond.Or(extra.Eq(1), extra.Eq(4))),
				[]cond.Cond[Item]{cond.Or(extra.Eq(1), extra.Eq(4))},
			},
			"nested": {
				from().Where(cond.Or(cond.Or(rank.Eq(1), rank.Eq(2)), cond.Or(rank.Eq(3), cond.Or(rank.Eq(2), rank.Eq(9))))),
				[]cond.Cond[Item]{cond.Or(cond.Or(rank.Eq(1), rank.Eq(2)), cond.Or(rank.Eq(3), cond.Or(rank.Eq(2), rank.Eq(9))))},
			},
			"values and another field": {
				from().Where(cond.Or(group.Eq("a"), group.Eq("b")), cond.Or(rank.Eq(1), rank.Eq(2))),
				[]cond.Cond[Item]{cond.Or(group.Eq("a"), group.Eq("b")), cond.Or(rank.Eq(1), rank.Eq(2))},
			},
			"values and a negation": {
				from().Where(cond.Or(group.Eq("a"), group.Eq("b")), cond.Not(cond.Or(rank.Eq(1), rank.Eq(2)))),
				[]cond.Cond[Item]{cond.Or(group.Eq("a"), group.Eq("b")), cond.Not(cond.Or(rank.Eq(1), rank.Eq(2)))},
			},
			"values and a range stay a disjunction": {
				from().Where(cond.Or(rank.Eq(1), rank.Gt(7))),
				[]cond.Cond[Item]{cond.Or(rank.Eq(1), rank.Gt(7))},
			},
			// The same path encoded otherwise is another field, whose values are not the
			// values of this one.
			"values of one path in two encodings stay a disjunction": {
				from().Where(cond.Or(rank.Eq(1), rank32.Eq(2))),
				[]cond.Cond[Item]{cond.Or(rank.Eq(1), rank32.Eq(2))},
			},
		} {
			t.Run(name, func(t *testing.T) {
				s := spec{conds: test.where, memo: &memo{}}
				want := f.expect(t, s)
				require.NotEmpty(t, want)
				for _, q := range []*query.Query[int64, Item]{test.q, test.q.FullScan()} {
					require.ElementsMatch(t, want, f.keys(t, db, q))
				}
			})
		}
	})
}

// rank32 is the field Rank encoded as a 32-bit integer, unlike rank.
var rank32 = field.Of("Rank", func(it *Item) int32 { return int32(it.Rank) })

// store of records with a key that is not a number, and one that is.
type Rec struct{ A, B, C int }

var (
	recA = field.Path[Rec, int]("A")
	recB = field.Path[Rec, int]("B")
	recC = field.Path[Rec, int]("C")
)

func defineRecs[K comparable](prefix string) *extstore.Store[K, Rec] {
	recs := extstore.New[K, Rec](prefix)
	index.On(recs, "by_a", recA)
	index.On(recs, "by_b", recB)
	index.On(recs, "by_c", recC)
	return recs
}

// recKeys are the keys of the records, which are many and not all of the size of a number.
func recKeys[K comparable](key func(i int) K) []K {
	keys := make([]K, 160)
	for i := range keys {
		keys[i] = key(i)
	}
	return keys
}

func recOf(i int) *Rec { return &Rec{A: i % 7, B: i % 11, C: i % 13} }

// TestCombinedPlansOfKeysThatAreNotNumbers checks that plans that combine indexes give the
// records that a scan gives when the primary keys are strings, which are more than 8 bytes, and
// arrays of bytes, and all the keys have the same hash.
func TestCombinedPlansOfKeysThatAreNotNumbers(t *testing.T) {
	t.Run("strings", func(t *testing.T) {
		testCombinedKeys(t, defineRecs[string]("recs"),
			recKeys(func(i int) string { return fmt.Sprintf("record-%05d", i) }), true)
	})
	t.Run("arrays of 16 bytes", func(t *testing.T) {
		testCombinedKeys(t, defineRecs[[16]byte]("recs"),
			recKeys(func(i int) (k [16]byte) {
				k[0], k[7], k[15] = byte(i>>8), byte(i), byte(i*7)
				return k
			}), true)
	})
	t.Run("numbers", func(t *testing.T) {
		testCombinedKeys(t, defineRecs[int64]("recs"), recKeys(func(i int) int64 { return int64(i) + 1 }), false)
	})
}

func testCombinedKeys[K comparable](t *testing.T, recs *extstore.Store[K, Rec], keys []K, hashed bool) {
	// Few hashes, so that keys that are not in a set seem to be.
	defer keyhash.Set(func(raw []byte) uint64 { return uint64(len(raw)+int(raw[len(raw)-1])) % 3 })()

	testkv.Each(t, func(t *testing.T, db kv.DB) {
		require.NoError(t, recs.Check(db))
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			for i, k := range keys {
				if err := recs.Instantiate(tx).Set(k, recOf(i)); err != nil {
					return err
				}
			}
			return nil
		}))

		run := func(q *query.Query[K, Rec]) []K {
			var got []K
			require.NoError(t, db.View(func(tx kv.Tx) error {
				it := q.Run(tx)
				defer it.Close()
				for ; it.Valid(); it.Next() {
					k, err := it.Key()
					require.NoError(t, err)
					r, err := it.Value()
					require.NoError(t, err)
					require.Equal(t, *recOf(slices.Index(keys, k)), *r)
					got = append(got, k)
				}
				return it.Err()
			}))
			return got
		}
		want := func(match func(r *Rec) bool) []K {
			var want []K
			for i, k := range keys {
				if match(recOf(i)) {
					want = append(want, k)
				}
			}
			return want
		}

		for name, test := range map[string]struct {
			conds []cond.Cond[Rec]
			match func(r *Rec) bool
			shape string
		}{
			"and": {
				[]cond.Cond[Rec]{recA.Eq(3), recB.Eq(4)},
				func(r *Rec) bool { return r.A == 3 && r.B == 4 }, "Intersect",
			},
			"and of three": {
				[]cond.Cond[Rec]{recA.In(1, 2), recB.Lt(5), recC.Gte(3)},
				func(r *Rec) bool { return (r.A == 1 || r.A == 2) && r.B < 5 && r.C >= 3 }, "",
			},
			"or": {
				[]cond.Cond[Rec]{cond.Or(recA.Eq(3), recB.Eq(4))},
				func(r *Rec) bool { return r.A == 3 || r.B == 4 }, "Union",
			},
			"or of three, and": {
				[]cond.Cond[Rec]{cond.Or(recA.Eq(3), recB.Eq(4), recC.Eq(5)), recC.Lt(9)},
				func(r *Rec) bool { return (r.A == 3 || r.B == 4 || r.C == 5) && r.C < 9 }, "Union",
			},
			"or, of ranges that overlap": {
				[]cond.Cond[Rec]{cond.Or(recA.Lt(5), recA.Gt(3), recB.Gt(5))},
				func(r *Rec) bool { return r.A < 5 || r.A > 3 || r.B > 5 }, "",
			},
			"ors": {
				[]cond.Cond[Rec]{cond.Or(recA.Eq(1), recB.Eq(2)), cond.Or(recB.Eq(3), recC.Eq(4))},
				func(r *Rec) bool { return (r.A == 1 || r.B == 2) && (r.B == 3 || r.C == 4) }, "Union",
			},
		} {
			t.Run(name, func(t *testing.T) {
				q := query.From(recs).Where(test.conds...)
				expected := want(test.match)
				require.NotEmpty(t, expected)

				text, err := q.Explain()
				require.NoError(t, err)
				if test.shape != "" {
					require.Contains(t, text, test.shape)
				}
				require.ElementsMatch(t, expected, run(q), text)

				// Every plan that reads several indexes, and a limit over each.
				plans, err := q.CombinedPlans()
				require.NoError(t, err)
				require.NotEmpty(t, plans)
				for _, p := range plans {
					text := p.Explain()
					if hashed {
						require.Contains(t, text, "(hashed)")
					} else {
						require.NotContains(t, text, "(hashed)")
					}

					var got []K
					require.NoError(t, db.View(func(tx kv.Tx) error {
						it := p.Run(tx)
						defer it.Close()
						for ; it.Valid(); it.Next() {
							k, err := it.Key()
							require.NoError(t, err)
							got = append(got, k)
						}
						return it.Err()
					}))
					require.ElementsMatch(t, expected, got, text)
				}

				// Paged, the records are distinct and satisfy the conditions.
				page := run(q.Offset(3).Limit(10))
				require.Len(t, page, min(10, max(0, len(expected)-3)))
				for _, k := range page {
					require.Contains(t, expected, k)
				}
				require.Len(t, slices.Compact(slices.Clone(sortedKeys(page, keys))), len(page))
			})
		}
	})
}

// sortedKeys returns keys in the order of the records.
func sortedKeys[K comparable](page, keys []K) []K {
	sorted := slices.Clone(page)
	slices.SortFunc(sorted, func(a, b K) int { return slices.Index(keys, a) - slices.Index(keys, b) })
	return sorted
}

// TestIntersectionsOfManyIndexes runs the intersections that are forced, of three and of four
// indexes, which the costs of the planner do not choose, and checks them against a scan.
func TestIntersectionsOfManyIndexes(t *testing.T) {
	testkv.Each(t, func(t *testing.T, db kv.DB) {
		f := newFixture(t, db)
		from := func() *query.Query[int64, Item] { return query.From(f.store) }

		for name, test := range map[string]struct {
			q      *query.Query[int64, Item]
			unions int
		}{
			"three": {from().Where(group.In("a", "b"), tags.Eq("t1"), note.Gte("n1")).
				UsingAll("by_group_rank", "by_tag_rank", "by_note"), 2},
			"three, in order": {from().Where(group.Eq("a"), tags.Eq("t1"), note.Gte("n1")).OrderBy(rank.Desc()).
				UsingAll("by_group_rank", "by_tag_rank", "by_note"), 2},
			"three, a page": {from().Where(group.Eq("a"), tags.Eq("t1"), note.Gte("n1")).OrderBy(rank.Desc()).Offset(2).Limit(5).
				UsingAll("by_group_rank", "by_tag_rank", "by_note"), 2},
			"four": {from().Where(group.In("a", "b", "c"), tags.Eq("t1"), note.Gte("n0"), total.Gte(3)).MaxKeys(1_000_000).
				UsingAll("by_group_rank", "by_tag_rank", "by_note", "by_total"), 3},
			"four, of values": {from().Where(group.In("a", "b", "c"), tags.In("t1", "t2"), note.In("n0", "n1"), total.In(3, 4, 5, 6)).OrderBy(code).
				UsingAll("by_group_rank", "by_tag_rank", "by_note", "by_total"), 3},
		} {
			t.Run(name, func(t *testing.T) {
				text, err := test.q.Explain()
				require.NoError(t, err)
				require.Contains(t, text, "Intersect")
				require.Equal(t, test.unions, strings.Count(text, "IndexKeys"), text)

				// The scan and the intersection give the records that satisfy the conditions, the
				// same page of them if they are ordered.
				want := f.keys(t, db, test.q.FullScan())
				require.NotEmpty(t, want, text)
				got := f.keys(t, db, test.q)
				require.Equal(t, len(want), len(got), text)
				if strings.Contains(text, "Sort") || strings.Contains(text, "in order of") {
					require.Equal(t, want, got, text)
				} else {
					require.ElementsMatch(t, want, got, text)
				}
			})
		}
	})
}
