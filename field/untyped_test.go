package field_test

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type (
	Genre  string
	Rating uint8
)

// Edition has a field of every kind that a field can be found in by reflection.
type Edition struct {
	ID       uuid.UUID
	Name     string
	Genre    Genre
	Genres   []Genre
	Pages    int32
	Rating   Rating
	Price    float64
	Weight   float32
	InPrint  bool
	Released time.Time
	Reprint  *time.Time
	Digest   [4]byte
	Raw      []byte
	Book     *Book
	Sizes    []int
	Extra    map[string]string
	Editions []Book
}

var edition = &Edition{
	ID:       uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
	Name:     "First",
	Genre:    "scifi",
	Genres:   []Genre{"scifi", "classic"},
	Pages:    412,
	Rating:   9,
	Price:    9.5,
	Weight:   0.25,
	InPrint:  true,
	Released: time.Date(1965, 8, 1, 0, 0, 0, 0, time.UTC),
	Digest:   [4]byte{0xde, 0xad, 0xbe, 0xef},
	Raw:      []byte("raw"),
	Book:     &Book{Title: "Dune", Year: 1965, Tags: []string{"scifi"}},
	Sizes:    []int{1, 2},
}

func TestAccessor(t *testing.T) {
	get, typ, err := field.Accessor[Edition]("Book.Author.Name")
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[string](), typ)

	value, ok := get(&Edition{Book: &Book{Author: Author{Name: "Herbert"}}})
	require.True(t, ok)
	require.Equal(t, "Herbert", value.String())

	// Behind a pointer that is nil there is no field.
	_, ok = get(&Edition{})
	require.False(t, ok)

	// A field of any type can be read, with or without a key codec.
	get, typ, err = field.Accessor[Edition]("Extra")
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[map[string]string](), typ)
	value, ok = get(&Edition{Extra: map[string]string{"a": "b"}})
	require.True(t, ok)
	require.Equal(t, map[string]string{"a": "b"}, value.Interface())

	for _, path := range []string{"Missing", "Book.Missing", "Name.Length", "Book.hidden", ""} {
		_, _, err := field.Accessor[Edition](path)
		require.Error(t, err, path)
	}
}

func TestReflect(t *testing.T) {
	for path, want := range map[string]struct {
		typ    reflect.Type
		codec  string
		multi  bool
		values []any
	}{
		"ID":               {reflect.TypeFor[uuid.UUID](), "bytes[16]", false, []any{edition.ID}},
		"Name":             {reflect.TypeFor[string](), "string", false, []any{"First"}},
		"Genre":            {reflect.TypeFor[Genre](), "string", false, []any{Genre("scifi")}},
		"Genres":           {reflect.TypeFor[Genre](), "string", true, []any{Genre("scifi"), Genre("classic")}},
		"Pages":            {reflect.TypeFor[int32](), "int32", false, []any{int32(412)}},
		"Rating":           {reflect.TypeFor[Rating](), "uint8", false, []any{Rating(9)}},
		"Price":            {reflect.TypeFor[float64](), "float64", false, []any{9.5}},
		"Weight":           {reflect.TypeFor[float32](), "float32", false, []any{float32(0.25)}},
		"InPrint":          {reflect.TypeFor[bool](), "bool", false, []any{true}},
		"Released":         {reflect.TypeFor[time.Time](), "time", false, []any{edition.Released}},
		"Reprint":          {reflect.TypeFor[time.Time](), "time", false, []any{}},
		"Digest":           {reflect.TypeFor[[4]byte](), "bytes[4]", false, []any{[4]byte{0xde, 0xad, 0xbe, 0xef}}},
		"Raw":              {reflect.TypeFor[[]byte](), "bytes", false, []any{[]byte("raw")}},
		"Sizes":            {reflect.TypeFor[int](), "int", true, []any{1, 2}},
		"Book.Year":        {reflect.TypeFor[int](), "int", false, []any{1965}},
		"Book.Tags":        {reflect.TypeFor[string](), "string", true, []any{"scifi"}},
		"Book.Subtitle":    {reflect.TypeFor[string](), "string", false, []any{}},
		"Book.Editor.Name": {reflect.TypeFor[string](), "string", false, []any{}},
	} {
		f, err := field.Reflect[Edition](path)
		require.NoError(t, err, path)
		require.Equal(t, path, f.Path())
		require.Equal(t, want.typ, f.Type(), path)
		require.Equal(t, want.codec, f.CodecName(), path)
		require.Equal(t, want.multi, f.Multi(), path)
		require.Equal(t, want.values, f.AnyValues(edition), path)

		keys, err := f.Keys(edition)
		require.NoError(t, err, path)
		require.Len(t, keys, len(want.values), path)
	}

	for _, path := range []string{"Missing", "Book", "Extra", "Editions", "Book.Author", "Book.hidden"} {
		_, err := field.Reflect[Edition](path)
		require.Error(t, err, path)
	}

	t.Run("SameAsPath", func(t *testing.T) {
		// A field that is found by reflection has the keys of the field of its type, and the
		// comparisons of the two are the same to an index.
		typed := field.Path[Edition, Genre]("Genres")
		reflected, err := field.Reflect[Edition]("Genres")
		require.NoError(t, err)

		want, err := typed.Keys(edition)
		require.NoError(t, err)
		got, err := reflected.Keys(edition)
		require.NoError(t, err)
		require.Equal(t, want, got)

		a, b := typed.Eq("classic"), reflected.Eq(Genre("classic"))
		require.NoError(t, b.Err())
		require.Equal(t, a.Path, b.Path)
		require.Equal(t, a.Codec, b.Codec)
		require.Equal(t, a.Values, b.Values)
		require.Equal(t, a.String(), b.String())

		// A value of another type than the one of the field is refused.
		require.Error(t, reflected.Eq("classic").Err())
	})

	t.Run("TypeOfATypedField", func(t *testing.T) {
		require.Equal(t, reflect.TypeFor[int](), year.Type())
		require.Equal(t, reflect.TypeFor[string](), tags.Type())
		require.Equal(t, []any{"scifi", "classic"}, tags.AnyValues(sample))
		require.Equal(t, []any{1965}, year.Desc().AnyValues(sample))
	})
}

func TestConvert(t *testing.T) {
	when := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	id := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

	for _, test := range []struct {
		value any
		want  any
	}{
		// As it is.
		{"a", "a"},
		{7, 7},
		{Genre("a"), Genre("a")},
		{when, when},

		// Between a type and the types that are defined on it.
		{"a", Genre("a")},
		{Genre("a"), "a"},
		{int64(9), Rating(9)},
		{Rating(9), 9},

		// Numbers.
		{int64(1965), 1965},
		{int64(1965), int32(1965)},
		{int64(-128), int8(-128)},
		{uint64(math.MaxUint64), uint64(math.MaxUint64)},
		{uint8(200), int16(200)},
		{1965.0, 1965},
		{-3.0, int8(-3)},
		{float32(2), uint(2)},
		{int64(3), 3.0},
		{int64(3), float32(3)},
		{uint16(3), 3.0},
		{2.5, float32(2.5)},
		{float32(2.5), 2.5},

		// Numbers and booleans as text.
		{"1965", 1965},
		{"-7", int8(-7)},
		{"1e3", 1000},
		{"255", uint8(255)},
		{"2.5", 2.5},
		{"-1e-3", -0.001},
		{"true", true},
		{"false", false},
		{true, true},

		// Times.
		{"2020-01-02T03:04:05Z", when},
		{"2020-01-02T05:04:05+02:00", when},
		{"2020-01-02T03:04:05", when},
		{"2020-01-02 03:04:05", when},
		{"2020-01-02 03:04:05.5", when.Add(time.Second / 2)},
		{"2020-01-02", time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)},

		// Bytes.
		{"raw", []byte("raw")},
		{[]byte("raw"), []byte("raw")},
		{"deadbeef", [4]byte{0xde, 0xad, 0xbe, 0xef}},

		// A type that reads itself from text.
		{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", id},
		{id, id},
	} {
		got, err := field.Convert(test.value, reflect.TypeOf(test.want))
		require.NoError(t, err, "%#v as %T", test.value, test.want)
		require.IsType(t, test.want, got)
		if want, ok := test.want.(time.Time); ok {
			require.True(t, want.Equal(got.(time.Time)), "%#v: got %v", test.value, got)
			continue
		}
		require.Equal(t, test.want, got, "%#v as %T", test.value, test.want)
	}

	for _, test := range []struct {
		value any
		to    any
	}{
		{nil, ""},
		{nil, 0},

		// A string is not made of something else, and nothing but a string is made of text.
		{7, ""},
		{true, ""},
		{when, ""},
		{"abc", 0},
		{"", 0},
		{"abc", 1.5},
		{"NaN", 1.5},
		{"yes", true},
		{"1", true},
		{1, true},

		// A number that the type does not have.
		{1965.5, 0},
		{"1965.5", 0},
		{int64(300), int8(0)},
		{int64(-129), int8(0)},
		{"300", uint8(0)},
		{int64(-1), uint(0)},
		{-1.0, uint8(0)},
		{"-1", uint64(0)},
		{uint64(math.MaxUint64), int64(0)},
		{1e19, int64(0)},
		{9.3e18, int64(0)},
		{-9.3e18, int64(0)},
		{2e19, uint64(0)},
		{math.Inf(1), 0},
		{math.NaN(), 0},
		{1e300, float32(0)},
		{true, 0},
		{true, 1.5},

		// Times, bytes and the rest.
		{"yesterday", when},
		{"2020-13-45", when},
		{1577934245, when},
		{"dead", [4]byte{}},
		{"deadbeef00", [4]byte{}},
		{"not hex!", [4]byte{}},
		{7, [4]byte{}},
		{7, []byte{}},
		{"a", []string{}},
		{"not-a-uuid", id},
		{7, id},
		{"a", Author{}},
		{"a", map[string]int{}},
	} {
		got, err := field.Convert(test.value, reflect.TypeOf(test.to))
		require.Error(t, err, "%#v as %T gave %#v", test.value, test.to, got)
	}
}

func TestUntyped(t *testing.T) {
	// The same checks for a field of a type and for a field that is found by reflection.
	reflected, err := field.Reflect[Book]("Year")
	require.NoError(t, err)
	reflectedTags, err := field.Reflect[Book]("Tags")
	require.NoError(t, err)

	for name, fields := range map[string]struct{ year, tags field.Untyped[Book] }{
		"Typed":      {year, tags},
		"Descending": {year.Desc(), tags.Desc()},
		"Reflected":  {reflected, reflectedTags},
	} {
		t.Run(name, func(t *testing.T) {
			same := func(want *cond.Comparison[Book], got *cond.Comparison[Book], err error) {
				t.Helper()

				require.NoError(t, err)
				require.NoError(t, got.Err())
				require.Equal(t, want.Path, got.Path)
				require.Equal(t, want.Codec, got.Codec)
				require.Equal(t, want.Op, got.Op)
				require.Equal(t, want.Values, got.Values)
				require.Equal(t, want.Low, got.Low)
				require.Equal(t, want.High, got.High)
				require.Equal(t, want.String(), got.String())
				for _, b := range []*Book{sample, {Year: 1970}, {Year: 2000, Tags: []string{"new"}}, {}} {
					require.Equal(t, match(t, want, b), match(t, got, b))
				}
			}

			// The values are converted to the type of the field, whatever they come as.
			for _, value := range []any{1965, int64(1965), 1965.0, "1965", uint16(1965)} {
				got, err := fields.year.EqAny(value)
				same(year.Eq(1965), got, err)
			}
			got, err := fields.year.InAny(int64(1965), "1970", 2000.0)
			same(year.In(1965, 1970, 2000), got, err)
			got, err = fields.tags.EqAny("scifi")
			same(tags.Eq("scifi"), got, err)
			got, err = fields.tags.InAny()
			same(tags.In(), got, err)

			got, err = fields.year.RangeAny(kvutil.From[any](int64(1965)))
			same(year.Gte(1965), got, err)
			got, err = fields.year.RangeAny(kvutil.After[any]("1965"))
			same(year.Gt(1965), got, err)
			got, err = fields.year.RangeAny(kvutil.Until[any](1970.0))
			same(year.Lt(1970), got, err)
			got, err = fields.year.RangeAny(kvutil.Through[any](1970))
			same(year.Lte(1970), got, err)
			got, err = fields.year.RangeAny(kvutil.Between[any](int64(1960), "1970"))
			same(year.Between(1960, 1970), got, err)
			got, err = fields.year.RangeAny(kvutil.After[any](1960).Until(1970))
			same(year.Range(kvutil.After(1960).Until(1970)), got, err)
			got, err = fields.year.RangeAny(kvutil.All[any]())
			same(year.Range(kvutil.All[int]()), got, err)

			// A value that the type of the field does not have is an error, not a condition
			// that nothing satisfies.
			_, err = fields.year.EqAny("abc")
			require.ErrorContains(t, err, "Year")
			_, err = fields.year.EqAny(1965.5)
			require.Error(t, err)
			_, err = fields.year.InAny(1965, "abc")
			require.Error(t, err)
			_, err = fields.year.RangeAny(kvutil.From[any]("abc"))
			require.Error(t, err)
			_, err = fields.year.RangeAny(kvutil.Until[any](true))
			require.Error(t, err)
			_, err = fields.tags.EqAny(7)
			require.Error(t, err)
			_, err = fields.tags.EqAny(nil)
			require.Error(t, err)
		})
	}
}
