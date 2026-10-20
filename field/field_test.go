package field_test

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/stretchr/testify/require"
)

type Author struct {
	Name string
	Born int
}

type Book struct {
	Title    string
	Year     int
	Tags     []string
	Subtitle *string
	Author   Author
	Editor   *Author
	hidden   string
}

var (
	title  = field.Of("Title", func(b *Book) string { return b.Title })
	year   = field.Of("Year", func(b *Book) int { return b.Year })
	tags   = field.Each("Tags", func(b *Book) []string { return b.Tags })
	parts  = field.List(title, year.Desc())
	sample = &Book{Title: "Dune", Year: 1965, Tags: []string{"scifi", "classic"}}
)

func TestField(t *testing.T) {
	require.Equal(t, "Title", title.Path())
	require.Equal(t, "string", title.CodecName())
	require.False(t, title.Multi())
	require.False(t, title.Descending())
	require.Equal(t, []string{"Dune"}, title.Values(sample))

	require.True(t, tags.Multi())
	require.Equal(t, []string{"scifi", "classic"}, tags.Values(sample))
	require.Empty(t, tags.Values(&Book{}))

	// A value is encoded in the form that marks where it ends.
	keys, err := title.Keys(sample)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("Dune\x00\x01")}, keys)
	keys, err = tags.Keys(sample)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("scifi\x00\x01"), []byte("classic\x00\x01")}, keys)

	// A descending field has its bytes inverted, and is another value than the field it is made of.
	desc := year.Desc()
	require.True(t, desc.Descending())
	require.False(t, year.Descending())
	keys, err = desc.Keys(sample)
	require.NoError(t, err)
	require.Equal(t, [][]byte{lex.Invert(lex.EncodeInt64(1965))}, keys)
	encoded, err := desc.Encode(1965)
	require.NoError(t, err)
	require.Equal(t, lex.EncodeInt64(1965), encoded)

	require.Len(t, parts, 2)
	require.Equal(t, "Year", parts[1].Path())
	require.True(t, parts[1].Descending())
}

func TestFieldCodec(t *testing.T) {
	// A type without a key codec is a mistake in the definition.
	require.PanicsWithValue(t,
		`field "Author": no key codec for type field_test.Author; give the field a key codec`,
		func() { field.Of("Author", func(b *Book) Author { return b.Author }) })
	require.Panics(t, func() { field.Each("Authors", func(b *Book) []Author { return nil }) })
	require.Panics(t, func() { field.Path[Book, Author]("Author") })

	// A key codec that is given is used.
	byteTitle := field.Of("Title", func(b *Book) string { return b.Title }, codec.StringKey[string]())
	require.Equal(t, "string", byteTitle.Codec().Name())
	require.Panics(t, func() {
		field.Of("Title", func(b *Book) string { return b.Title }, codec.StringKey[string](), codec.StringKey[string]())
	})
}

func TestPath(t *testing.T) {
	subtitle := "A novel"
	b := &Book{
		Title:    "Dune",
		Year:     1965,
		Tags:     []string{"scifi", "classic"},
		Subtitle: &subtitle,
		Author:   Author{Name: "Herbert", Born: 1920},
		Editor:   &Author{Name: "Ann"},
	}

	require.Equal(t, []string{"Dune"}, field.Path[Book, string]("Title").Values(b))
	require.Equal(t, []int{1965}, field.Path[Book, int]("Year").Values(b))
	require.Equal(t, []string{"Herbert"}, field.Path[Book, string]("Author.Name").Values(b))
	require.Equal(t, []int{1920}, field.Path[Book, int]("Author.Born").Values(b))

	// A pointer is the value that it points to, and a record with a nil pointer has no value.
	bySubtitle := field.Path[Book, string]("Subtitle")
	require.False(t, bySubtitle.Multi())
	require.Equal(t, []string{"A novel"}, bySubtitle.Values(b))
	require.Empty(t, bySubtitle.Values(&Book{}))

	// The same holds for a pointer on the way to the field.
	byEditor := field.Path[Book, string]("Editor.Name")
	require.Equal(t, []string{"Ann"}, byEditor.Values(b))
	require.Empty(t, byEditor.Values(&Book{}))

	// A slice is each of its elements.
	byTag := field.Path[Book, string]("Tags")
	require.True(t, byTag.Multi())
	require.Equal(t, []string{"scifi", "classic"}, byTag.Values(b))
	require.Empty(t, byTag.Values(&Book{}))

	// A path is the same field as the function that returns its value.
	keys, err := field.Path[Book, int]("Year").Keys(b)
	require.NoError(t, err)
	want, err := year.Keys(b)
	require.NoError(t, err)
	require.Equal(t, want, keys)
	require.Equal(t, year.Eq(1965).String(), field.Path[Book, int]("Year").Eq(1965).String())

	for name, define := range map[string]func(){
		"MissingField":   func() { field.Path[Book, string]("Missing") },
		"MissingNested":  func() { field.Path[Book, string]("Author.Missing") },
		"WrongType":      func() { field.Path[Book, int]("Title") },
		"WrongElement":   func() { field.Path[Book, int]("Tags") },
		"NotAStruct":     func() { field.Path[Book, string]("Title.Length") },
		"Unexported":     func() { field.Path[Book, string]("hidden") },
		"StructAsAValue": func() { field.Path[Book, Author]("Author") },
	} {
		require.Panics(t, define, name)
	}
}

func match(t *testing.T, cnd cond.Cond[Book], b *Book) bool {
	t.Helper()

	ok, err := cnd.Match(b)
	require.NoError(t, err)
	return ok
}

func TestComparisons(t *testing.T) {
	tests := []struct {
		cnd  cond.Cond[Book]
		want bool
		text string
	}{
		{year.Eq(1965), true, "Year = 1965"},
		{year.Eq(1966), false, "Year = 1966"},
		{year.In(1900, 1965), true, "Year in (1900, 1965)"},
		{year.In(1900, 1966), false, "Year in (1900, 1966)"},
		{year.In(), false, "Year in ()"},
		{year.Gt(1964), true, "Year > 1964"},
		{year.Gt(1965), false, "Year > 1965"},
		{year.Gte(1965), true, "Year >= 1965"},
		{year.Gte(1966), false, "Year >= 1966"},
		{year.Lt(1966), true, "Year < 1966"},
		{year.Lt(1965), false, "Year < 1965"},
		{year.Lte(1965), true, "Year <= 1965"},
		{year.Lte(1964), false, "Year <= 1964"},
		{year.Between(1960, 1965), true, "Year >= 1960 and Year <= 1965"},
		{year.Between(1966, 1970), false, "Year >= 1966 and Year <= 1970"},
		{year.Range(kvutil.After(1960).Until(1965)), false, "Year > 1960 and Year < 1965"},
		{year.Range(kvutil.All[int]()), true, "Year is anything"},
		// Numbers compare as numbers, whatever their signs are.
		{year.Gt(-5), true, "Year > -5"},
		{year.Lt(-5), false, "Year < -5"},

		{title.Eq("Dune"), true, `Title = "Dune"`},
		{title.Eq("Dun"), false, `Title = "Dun"`},
		{title.Gt("Dun"), true, `Title > "Dun"`},
		{title.Lt("Dune2"), true, `Title < "Dune2"`},
		{title.Between("A", "E"), true, `Title >= "A" and Title <= "E"`},

		// A record with several values satisfies a condition if one of them does.
		{tags.Eq("classic"), true, `Tags = "classic"`},
		{tags.Eq("fantasy"), false, `Tags = "fantasy"`},
		{tags.In("fantasy", "scifi"), true, `Tags in ("fantasy", "scifi")`},
		{tags.Gt("d"), true, `Tags > "d"`},
		{tags.Gt("t"), false, `Tags > "t"`},

		// A descending field compares as the field does.
		{year.Desc().Gt(1964), true, "Year > 1964"},
		{year.Desc().Lt(1964), false, "Year < 1964"},
	}
	for _, test := range tests {
		require.Equal(t, test.want, match(t, test.cnd, sample), test.text)
		require.Equal(t, test.text, test.cnd.String())
	}

	// A record with no value satisfies no comparison.
	require.False(t, match(t, tags.Eq("classic"), &Book{}))
	require.False(t, match(t, tags.Range(kvutil.All[string]()), &Book{}))

	// What an index reads of a comparison.
	c := year.Between(1960, 1970)
	require.Equal(t, "Year", c.Path)
	require.Equal(t, "int", c.Codec)
	require.Equal(t, cond.OpRange, c.Op)
	require.Equal(t, &cond.Bound{Value: lex.EncodeInt64(1960)}, c.Low)
	require.Equal(t, &cond.Bound{Value: lex.EncodeInt64(1970)}, c.High)
	require.NoError(t, c.Err())

	c = title.In("a", "b")
	require.Equal(t, cond.OpIn, c.Op)
	require.Equal(t, [][]byte{[]byte("a\x00\x01"), []byte("b\x00\x01")}, c.Values)
	require.Equal(t, cond.OpEq, title.Eq("a").Op)
	require.Nil(t, year.Gt(1).High)
	require.True(t, year.Gt(1).Low.Exclusive)
}

func TestCombinations(t *testing.T) {
	yes, no := year.Eq(1965), year.Eq(1900)
	long := cond.Func("long_title", func(b *Book) bool { return len(b.Title) > 10 })

	tests := []struct {
		cnd  cond.Cond[Book]
		want bool
		text string
	}{
		{cond.And(yes, title.Eq("Dune")), true, `(Year = 1965) and (Title = "Dune")`},
		{cond.And(yes, no), false, "(Year = 1965) and (Year = 1900)"},
		{cond.And[Book](), true, "true"},
		{cond.Or(no, yes), true, "(Year = 1900) or (Year = 1965)"},
		{cond.Or(no, no), false, "(Year = 1900) or (Year = 1900)"},
		{cond.Or[Book](), false, "false"},
		{cond.Not(no), true, "not (Year = 1900)"},
		{cond.Not(yes), false, "not (Year = 1965)"},
		{long, false, "long_title()"},
		{cond.Not(long), true, "not (long_title())"},
		{cond.And(yes, cond.Or(no, cond.Not(long))), true, "(Year = 1965) and ((Year = 1900) or (not (long_title())))"},
	}
	for _, test := range tests {
		require.Equal(t, test.want, match(t, test.cnd, sample), test.text)
		require.Equal(t, test.text, test.cnd.String())
	}
}

// TestComparisonsAgreeWithValues checks the comparisons, which compare encoded values, against
// the comparisons of the values themselves.
func TestComparisonsAgreeWithValues(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	titles := []string{"", "a", "a\x00", "a\x00b", "a\xff", "ab", "b"}

	for range 20000 {
		b := &Book{Title: titles[rng.IntN(len(titles))], Year: rng.IntN(7) - 3}
		y, s := rng.IntN(7)-3, titles[rng.IntN(len(titles))]

		require.Equal(t, b.Year == y, match(t, year.Eq(y), b))
		require.Equal(t, b.Year > y, match(t, year.Gt(y), b))
		require.Equal(t, b.Year >= y, match(t, year.Gte(y), b))
		require.Equal(t, b.Year < y, match(t, year.Lt(y), b))
		require.Equal(t, b.Year <= y, match(t, year.Lte(y), b))

		require.Equal(t, b.Title == s, match(t, title.Eq(s), b), "%q = %q", b.Title, s)
		require.Equal(t, b.Title > s, match(t, title.Gt(s), b), "%q > %q", b.Title, s)
		require.Equal(t, b.Title >= s, match(t, title.Gte(s), b), "%q >= %q", b.Title, s)
		require.Equal(t, b.Title < s, match(t, title.Lt(s), b), "%q < %q", b.Title, s)
		require.Equal(t, b.Title <= s, match(t, title.Lte(s), b), "%q <= %q", b.Title, s)
	}
}

// TestComparisonsKnowIfTheFieldHasSeveralValues checks what a planner needs to know to merge the
// comparisons of a field: two of them are one only if a record has a value at most.
func TestComparisonsKnowIfTheFieldHasSeveralValues(t *testing.T) {
	require.False(t, year.Gt(1).Multi)
	require.False(t, year.Eq(1).Multi)
	require.False(t, title.In("a", "b").Multi)
	require.True(t, tags.Gt("a").Multi)
	require.True(t, tags.Eq("a").Multi)
	require.True(t, tags.In("a", "b").Multi)
}

// TestAlways checks which fields every record has exactly one value for.
var errNegative = errors.New("negative year")

func TestComputed(t *testing.T) {
	// A field that has no value for a record, one that fails, and one that has a value.
	age := field.Computed("Age", func(b *Book) ([]int, error) {
		switch {
		case b.Year < 0:
			return nil, errNegative
		case b.Year == 0:
			return nil, nil
		}
		return []int{2000 - b.Year}, nil
	})
	require.Equal(t, "Age", age.Path())
	require.Equal(t, "int", age.CodecName())
	require.False(t, age.Multi())
	require.False(t, age.Always(), "a computed field may have no value")
	require.Empty(t, age.Version())

	require.Equal(t, []int{35}, age.Values(sample))
	require.Equal(t, []any{35}, age.AnyValues(sample))
	keys, err := age.Keys(sample)
	require.NoError(t, err)
	want, err := field.Of("Age", func(*Book) int { return 35 }).Keys(sample)
	require.NoError(t, err)
	require.Equal(t, want, keys)

	none := &Book{}
	require.Empty(t, age.Values(none))
	keys, err = age.Keys(none)
	require.NoError(t, err)
	require.Empty(t, keys)
	c := age.Eq(35)
	ok, err := c.Match(none)
	require.NoError(t, err)
	require.False(t, ok)

	// The error comes out of the keys, which an index writes with, and of a comparison, and the
	// field has no values then.
	failing := &Book{Year: -1}
	_, err = age.Keys(failing)
	require.ErrorIs(t, err, errNegative)
	_, err = age.Desc().Keys(failing)
	require.ErrorIs(t, err, errNegative)
	require.Empty(t, age.Values(failing))
	require.Empty(t, age.AnyValues(failing))
	for _, c := range []*cond.Comparison[Book]{age.Eq(1), age.In(1, 2), age.Gt(1), age.Between(1, 5)} {
		ok, err := c.Match(failing)
		require.ErrorIs(t, err, errNegative, c.String())
		require.False(t, ok)
	}
	ok, err = cond.Not(age.Eq(1)).Match(failing)
	require.ErrorIs(t, err, errNegative)
	require.False(t, ok)
	c2, err := age.EqAny(35)
	require.NoError(t, err)
	ok, err = c2.Match(sample)
	require.NoError(t, err)
	require.True(t, ok)

	// More than one value is not what a field of one value has.
	two := field.Computed("Two", func(*Book) ([]int, error) { return []int{1, 2}, nil })
	_, err = two.Keys(sample)
	require.Error(t, err)
	require.Empty(t, two.Values(sample))

	// A key codec can be given, and a type that has none needs one.
	given := field.Computed("Given", func(*Book) ([]string, error) { return []string{"a"}, nil }, codec.StringKey[string]())
	require.Equal(t, "string", given.Codec().Name())
	require.Panics(t, func() {
		field.Computed("Bad", func(*Book) ([]Author, error) { return nil, nil })
	})
	require.Panics(t, func() {
		field.ComputedEach("Bad", func(*Book) ([]Author, error) { return nil, nil })
	})
}

func TestComputedEach(t *testing.T) {
	upper := field.ComputedEach("Upper", func(b *Book) ([]string, error) {
		if b.Title == "boom" {
			return nil, errNegative
		}
		values := make([]string, len(b.Tags))
		for i, tag := range b.Tags {
			values[i] = tag + "!"
		}
		return values, nil
	})
	require.True(t, upper.Multi())
	require.False(t, upper.Always())
	require.Equal(t, []string{"scifi!", "classic!"}, upper.Values(sample))
	keys, err := upper.Keys(sample)
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.Empty(t, upper.Values(&Book{}))
	_, err = upper.Keys(&Book{Title: "boom"})
	require.ErrorIs(t, err, errNegative)
	ok, err := upper.Eq("scifi!").Match(sample)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, upper.Eq("x").Multi)
}

func TestFieldVersion(t *testing.T) {
	f := field.Computed("F", func(*Book) ([]int, error) { return []int{1}, nil })
	v := f.WithVersion("3")
	require.Equal(t, "3", v.Version())
	require.Empty(t, f.Version(), "a field is not changed by its version")
	require.Equal(t, "F", v.Path())
	require.Equal(t, f.Values(sample), v.Values(sample))
	// Any field can have one.
	require.Equal(t, "7", title.WithVersion("7").Version())
	require.Equal(t, "7", title.WithVersion("7").Desc().Version())
}

func TestAlways(t *testing.T) {
	require.True(t, title.Always())
	require.True(t, title.Desc().Always())
	require.False(t, tags.Always())

	// A path with no pointer on it.
	require.True(t, field.Path[Book, string]("Title").Always())
	require.True(t, field.Path[Book, int]("Author.Born").Always())
	// A pointer at the end, a pointer on the way, and a slice.
	require.False(t, field.Path[Book, string]("Subtitle").Always())
	require.False(t, field.Path[Book, string]("Editor.Name").Always())
	require.False(t, field.Path[Book, string]("Tags").Always())

	for path, want := range map[string]bool{
		"Title":       true,
		"Author.Name": true,
		"Subtitle":    false,
		"Editor.Name": false,
		"Editor":      false,
		"Tags":        false,
	} {
		f, err := field.Reflect[Book](path)
		if path == "Editor" {
			// A struct has no key codec.
			require.Error(t, err)
			continue
		}
		require.NoError(t, err, path)
		require.Equal(t, want, f.Always(), path)
	}
}
