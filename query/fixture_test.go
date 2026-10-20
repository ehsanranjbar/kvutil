package query_test

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/query/sql"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

// Item is a record with fields of every kind: ones that every record has, ones that a record may
// have none of or several, and ones that no index is defined on.
type Item struct {
	Code   string   // unique
	Group  string   // few values
	Rank   int      // few values
	Score  int      // few values
	Label  *string  // optional, and in a composite index after a field that every item has
	Extra  int      // no index
	Tags   []string // several values
	Note   *string  // optional
	Weight *int     // optional, no index
	Ready  bool
}

var (
	code   = field.Path[Item, string]("Code")
	group  = field.Path[Item, string]("Group")
	rank   = field.Path[Item, int]("Rank")
	score  = field.Path[Item, int]("Score")
	label  = field.Path[Item, string]("Label")
	extra  = field.Path[Item, int]("Extra")
	tags   = field.Path[Item, string]("Tags")
	note   = field.Path[Item, string]("Note")
	weight = field.Path[Item, int]("Weight")
	ready  = field.Path[Item, bool]("Ready")

	// Fields that are SQL expressions of an item: one value for every item, and several for the
	// items that have tags.
	total    = sql.Expr[Item, int]("Rank + Score")
	upperTag = sql.Expr[Item, string]("upper(Tags)")

	// Values that some of the indexes cover, computed from an item.
	codeUpper   = index.Cover("CodeUpper", func(it *Item) string { return strings.ToUpper(it.Code) })
	rankSquared = index.Cover("RankSquared", func(it *Item) int { return it.Rank * it.Rank })
	// With a codec of its own.
	labelLen = index.Cover("LabelLen", func(it *Item) int {
		if it.Label == nil {
			return -1
		}
		return len(*it.Label)
	}, codec.Formatted[int](codec.JSON))
)

const itemCount = 400

// fixture is a store of items with indexes, and the items that are in it.
type fixture struct {
	store *extstore.Store[int64, Item]
	items map[int64]*Item
}

// defineItems defines the store of items and its indexes. The indexes are in the order in which
// ties between them are resolved.
func defineItems() *extstore.Store[int64, Item] {
	items := extstore.New[int64, Item]("items")
	// The indexes hold fields that are not in their keys (Including), of one value for an item,
	// of none or one, and of several, and values that are computed from an item (Covering).
	index.Composite(items, "by_group_rank", field.List(group, rank.Desc()),
		index.Including(extra, weight, ready), index.Covering(codeUpper))
	index.Composite(items, "by_score_label", field.List(score, label),
		index.Including(note, tags), index.Covering(rankSquared))
	index.Unique(items, "by_code", code, index.Including(group, rank, score, label, weight))
	index.UniqueComposite(items, "by_group_code", field.List(group, code.Desc()),
		index.Including(rank, extra), index.Covering(rankSquared, labelLen))
	index.Composite(items, "by_tag_rank", field.List(tags, rank),
		index.Including(group, extra, note), index.Covering(codeUpper))
	index.On(items, "by_note", note, index.Including(group, extra, weight), index.Covering(rankSquared))
	// Indexes of some of the items, by a condition in Go and by one in SQL, which a query reads
	// if its conditions imply theirs.
	index.On(items, "ready_by_rank", rank, index.Where(ready.Eq(true)),
		index.Including(score, weight, tags), index.Covering(codeUpper))
	index.On(items, "ab_by_score", score, sql.Where[Item](`"Group" IN ('a', 'b') AND Rank >= 3`),
		index.Including(rank, group, extra))
	// Indexes of expressions, of one value for an item and of several.
	index.On(items, "by_total", total, index.Including(group, tags, weight), index.Covering(labelLen))
	index.On(items, "by_upper_tag", upperTag, index.Including(rank, group, extra), index.Covering(rankSquared))
	return items
}

// randomItem returns an item whose fields have a few values each, so that many items are equal
// in a field and a condition on it is satisfied by some of them.
func randomItem(rng *rand.Rand, id int64) *Item {
	pick := func(values ...string) string { return values[rng.IntN(len(values))] }

	it := &Item{
		Code:  fmt.Sprintf("i%04d", id),
		Group: pick("a", "b", "c", "d"),
		Rank:  rng.IntN(10),
		Score: rng.IntN(5),
		Extra: rng.IntN(6),
		Ready: rng.IntN(2) == 0,
	}
	if rng.IntN(5) > 0 {
		l := pick("l0", "l1", "l2", "l3", "l4")
		it.Label = &l
	}
	for _, tag := range []string{"t0", "t1", "t2", "t3"} {
		if rng.IntN(3) == 0 {
			it.Tags = append(it.Tags, tag)
		}
	}
	if rng.IntN(2) == 0 {
		n := pick("n0", "n1", "n2")
		it.Note = &n
	}
	if rng.IntN(2) == 0 {
		w := rng.IntN(5)
		it.Weight = &w
	}
	return it
}

// newFixture defines the store of items and fills a database with them.
func newFixture(t *testing.T, db kv.DB) *fixture {
	t.Helper()

	f := &fixture{store: defineItems(), items: map[int64]*Item{}}
	require.NoError(t, f.store.Check(db))

	rng := rand.New(rand.NewPCG(11, 12))
	require.NoError(t, db.Update(func(tx kv.Tx) error {
		store := f.store.Instantiate(tx)
		for id := int64(1); id <= itemCount; id++ {
			it := randomItem(rng, id)
			f.items[id] = it
			if err := store.Set(id, it); err != nil {
				return err
			}
		}
		return nil
	}))
	return f
}
