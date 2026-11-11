package field_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

func TestFieldReads(t *testing.T) {
	known := func(r cond.Reader, want ...string) {
		t.Helper()
		paths, ok := r.Reads()
		require.True(t, ok)
		if len(want) == 0 {
			require.Empty(t, paths)
			return
		}
		require.Equal(t, want, paths)
	}
	unknown := func(r cond.Reader) {
		t.Helper()
		paths, ok := r.Reads()
		require.False(t, ok)
		require.Nil(t, paths)
	}

	// A field of the struct reads its own path.
	byPath := field.Path[Book, int]("Year")
	known(byPath, "Year")
	known(field.Path[Book, string]("Author.Name"), "Author.Name")
	known(field.Path[Book, string]("Tags"), "Tags")
	known(field.Path[Book, string]("Subtitle"), "Subtitle")
	reflected, err := field.Reflect[Book]("Author.Born")
	require.NoError(t, err)
	known(reflected, "Author.Born")
	reflectedTags, err := field.Reflect[Book]("Tags")
	require.NoError(t, err)
	known(reflectedTags, "Tags")

	// A field of a function reads what is not known, until it is told.
	of := field.Of("Len", func(b *Book) int { return len(b.Title) })
	each := field.Each("Words", func(b *Book) []string { return b.Tags })
	computed := field.Computed("Short", func(b *Book) ([]int, error) { return nil, nil })
	computedEach := field.ComputedEach("Shorts", func(b *Book) ([]int, error) { return nil, nil })
	unknown(of)
	unknown(each)
	unknown(computed)
	unknown(computedEach)
	known(of.Reading("Title"), "Title")
	known(each.Reading("Tags"), "Tags")
	known(computed.Reading("Title", "Author.Name"), "Title", "Author.Name")
	known(computedEach.Reading())
	known(of.Reading())
	// Declaring does not change the field it is made of.
	unknown(of)

	// The paths that are given are copied, and so are the ones that are returned.
	given := []string{"Title"}
	declared := of.Reading(given...)
	given[0] = "Year"
	paths, _ := declared.Reads()
	require.Equal(t, []string{"Title"}, paths)
	paths[0] = "Year"
	known(declared, "Title")

	// What is derived from a field reads what the field does.
	known(byPath.Desc(), "Year")
	known(byPath.WithVersion("2"), "Year")
	known(byPath.Desc().WithVersion("2").Desc(), "Year")
	known(declared.Desc(), "Title")
	known(declared.WithVersion("1"), "Title")
	unknown(of.Desc())
	unknown(of.WithVersion("1"))
	var untyped field.Untyped[Book] = byPath
	known(untyped.(cond.Reader), "Year")
	untyped = of
	unknown(untyped.(cond.Reader))

	// The comparisons of a field read what it does.
	for name, f := range map[string]field.Field[Book, int]{
		"path":       byPath,
		"descending": byPath.Desc(),
		"version":    byPath.WithVersion("v"),
		"declared":   of.Reading("Year"),
		"nothing":    of.Reading(),
	} {
		want, ok := f.Reads()
		require.True(t, ok, name)
		comparisons := map[string]*cond.Comparison[Book]{
			"Eq":      f.Eq(1),
			"In":      f.In(1, 2),
			"In none": f.In(),
			"Gt":      f.Gt(1),
			"Gte":     f.Gte(1),
			"Lt":      f.Lt(1),
			"Lte":     f.Lte(1),
			"Between": f.Between(1, 2),
			"Range":   f.Range(kvutil.All[int]()),
		}
		anyOf := func(c *cond.Comparison[Book], err error) *cond.Comparison[Book] {
			require.NoError(t, err, name)
			return c
		}
		comparisons["EqAny"] = anyOf(f.EqAny(1))
		comparisons["InAny"] = anyOf(f.InAny(1, 2))
		comparisons["RangeAny"] = anyOf(f.RangeAny(kvutil.Between[any](1, 2)))
		for kind, c := range comparisons {
			got, ok := c.Reads()
			require.True(t, ok, name+" "+kind)
			require.Equal(t, len(want), len(got), name+" "+kind)
			if len(want) > 0 {
				require.Equal(t, want, got, name+" "+kind)
			}
		}
	}

	// A field that is not known has comparisons that are not.
	for kind, c := range map[string]*cond.Comparison[Book]{
		"Eq":      of.Eq(1),
		"In":      of.In(1, 2),
		"Gt":      of.Gt(1),
		"Between": of.Between(1, 2),
		"Range":   of.Range(kvutil.All[int]()),
		"Desc":    of.Desc().Eq(1),
	} {
		paths, ok := c.Reads()
		require.False(t, ok, kind)
		require.Nil(t, paths, kind)
	}
	for kind, c := range map[string]func() (*cond.Comparison[Book], error){
		"EqAny":    func() (*cond.Comparison[Book], error) { return of.EqAny(1) },
		"InAny":    func() (*cond.Comparison[Book], error) { return of.InAny(1, 2) },
		"RangeAny": func() (*cond.Comparison[Book], error) { return of.RangeAny(kvutil.From[any](1)) },
	} {
		cmp, err := c()
		require.NoError(t, err, kind)
		_, ok := cmp.Reads()
		require.False(t, ok, kind)
	}

	// The reads of the conditions of several fields come together.
	paths, ok := cond.Reads[Book](byPath.Gte(1), field.Path[Book, string]("Title").Eq("a"), byPath.Lt(5))
	require.True(t, ok)
	require.Equal(t, []string{"Title", "Year"}, paths)
	_, ok = cond.Reads[Book](byPath.Gte(1), of.Eq(1))
	require.False(t, ok)
}
