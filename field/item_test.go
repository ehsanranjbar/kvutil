package field_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
)

// yearsOf reads the values of an item of any type that is known by its values.
func yearsOf[F any](s field.Selectable[Book, F], b *Book) []F { return s.Values(b) }

func TestItems(t *testing.T) {
	// A field is an item of any type, and a selectable one for whoever knows the type.
	items := []field.Item[Book]{title, year, tags}
	require.Equal(t, "Title", items[0].Path())
	require.False(t, items[1].Multi())
	require.True(t, items[2].Multi())
	require.Equal(t, []any{1965}, items[1].AnyValues(sample))
	require.Equal(t, []any{"scifi", "classic"}, items[2].AnyValues(sample))

	// F is inferred from the item.
	require.Equal(t, []int{1965}, yearsOf(year, sample))
}

func TestReadKey(t *testing.T) {
	for _, f := range []field.Component[Book]{title, year, tags} {
		keys, err := f.Keys(sample)
		require.NoError(t, err)
		for _, key := range keys {
			// What follows a value is not a part of it.
			value, n, err := f.ReadKey(append(append([]byte{}, key...), 0x42, 0x42))
			require.NoError(t, err)
			require.Equal(t, len(key), n)
			require.Contains(t, f.(field.Untyped[Book]).AnyValues(sample), value)
		}
	}
	_, _, err := year.ReadKey([]byte{1})
	require.Error(t, err)
}

// TestMatchKeys checks that a comparison of the encoded values of a record is what Match says of
// the record.
func TestMatchKeys(t *testing.T) {
	type comparison = *cond.Comparison[Book]

	encoded, err := year.Keys(sample)
	require.NoError(t, err)
	many, err := tags.Keys(sample)
	require.NoError(t, err)

	for _, c := range []comparison{
		year.Eq(1965), year.Eq(1), year.In(1, 1965), year.Gt(1965), year.Gte(1965), year.Lt(1965), year.Between(1900, 1970),
	} {
		want, err := c.Match(sample)
		require.NoError(t, err)
		require.Equal(t, want, c.MatchKeys(encoded), c.String())
		require.False(t, c.MatchKeys(nil), "no value satisfies %s", c)
	}
	for _, c := range []comparison{
		tags.Eq("classic"), tags.Eq("none"), tags.In("a", "scifi"), tags.Gt("scifi"), tags.Lt("a"),
	} {
		want, err := c.Match(sample)
		require.NoError(t, err)
		require.Equal(t, want, c.MatchKeys(many), c.String())
	}
}
