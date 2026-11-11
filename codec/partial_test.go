package codec_test

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type partialInner struct {
	X int
	Y string
}

type partialRecord struct {
	Renamed string  `msgpack:"n" json:"n"`
	Omit    string  `msgpack:",omitempty" json:",omitempty"`
	Alias   int     `msgpack:"alias:old"`
	Ptr     *string `msgpack:",omitempty" json:",omitempty"`
	NilPtr  *string `msgpack:",omitempty" json:",omitempty"`
	Tags    []string
	Floats  []float32
	Inner   partialInner
	When    time.Time
	Skipped string `msgpack:"-" json:"-"`
	Count   int
	OK      bool

	hidden int
}

func newPartialRecord() partialRecord {
	s := "pointed"
	return partialRecord{
		Renamed: "renamed",
		Omit:    "omit",
		Alias:   7,
		Ptr:     &s,
		Tags:    []string{"a", "b"},
		Floats:  []float32{1.5, 2.5},
		Inner:   partialInner{X: 3, Y: "y"},
		When:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
		Skipped: "skipped",
		Count:   42,
		OK:      true,
		hidden:  1,
	}
}

func TestPartialMatchesFullOnNamedFields(t *testing.T) {
	subsets := [][]string{
		{"Count"},
		{"Renamed"},
		{"Omit", "Ptr", "NilPtr"},
		{"Tags", "Floats", "When"},
		{"Inner.X"},
		{"Inner.X", "Inner.Y", "OK"},
		{"Skipped", "Alias"},
		{"Count", "Count", "Renamed"},
		{"Renamed", "Omit", "Alias", "Ptr", "NilPtr", "Tags", "Floats", "Inner", "When", "Skipped", "Count", "OK"},
	}

	for _, format := range []codec.Format{codec.Msgpack, codec.JSON} {
		t.Run(format.Name(), func(t *testing.T) {
			c := codec.Formatted[partialRecord](format)
			bz, err := c.Encode(newPartialRecord())
			require.NoError(t, err)
			full, err := c.Decode(bz)
			require.NoError(t, err)

			for _, fields := range subsets {
				t.Run(strings.Join(fields, ","), func(t *testing.T) {
					decode, ok := codec.PartialOf(c, fields)
					require.True(t, ok)

					var got partialRecord
					require.NoError(t, decode(bz, &got))

					want := partialRecord{}
					named := map[string]bool{}
					for _, f := range fields {
						first, _, _ := strings.Cut(f, ".")
						named[first] = true
					}
					if named["Renamed"] {
						want.Renamed = full.Renamed
					}
					if named["Omit"] {
						want.Omit = full.Omit
					}
					if named["Alias"] {
						want.Alias = full.Alias
					}
					if named["Ptr"] {
						want.Ptr = full.Ptr
					}
					if named["NilPtr"] {
						want.NilPtr = full.NilPtr
					}
					if named["Tags"] {
						want.Tags = full.Tags
					}
					if named["Floats"] {
						want.Floats = full.Floats
					}
					if named["Inner"] {
						want.Inner = full.Inner
					}
					if named["When"] {
						want.When = full.When
					}
					if named["Skipped"] {
						want.Skipped = full.Skipped
					}
					if named["Count"] {
						want.Count = full.Count
					}
					if named["OK"] {
						want.OK = full.OK
					}
					require.Equal(t, want, got)
				})
			}

			// The values that were encoded are what the full decode gives for the named fields.
			require.Equal(t, 42, full.Count)
			require.Equal(t, "pointed", *full.Ptr)
			require.Empty(t, full.Skipped)
			if format.Name() == "msgpack" {
				require.Equal(t, 7, full.Alias)
			}
		})
	}
}

type embeddedBase struct{ A int }

type partialEmbedded struct {
	embeddedBase
	B int
}

type partialArray struct {
	_msgpack struct{} `msgpack:",as_array"`
	A        int
	B        int
}

type customMsgpack struct {
	A int
	B int
}

func (c *customMsgpack) DecodeMsgpack(d *msgpack.Decoder) error {
	return errors.New("custom")
}

type customJSON struct {
	A int
	B int
}

func (c *customJSON) UnmarshalJSON([]byte) error { return errors.New("custom") }

type caseFold struct {
	Foo int
	FOO int
}

type tagFold struct {
	A int `json:"x"`
	B int `json:"X"`
}

type plainTwo struct {
	A int
	b int
}

type customFormat struct{}

func (customFormat) Name() string                  { return "custom" }
func (customFormat) Marshal(v any) ([]byte, error) { return json.Marshal(v) }
func (customFormat) Unmarshal(d []byte, v any) error {
	return json.Unmarshal(d, v)
}

func TestPartialBailsOut(t *testing.T) {
	cases := []struct {
		name   string
		ok     bool
		result func() bool
	}{
		{"msgpack struct", true, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](codec.Msgpack), []string{"A"})
			return ok
		}},
		{"json struct", true, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](codec.JSON), []string{"A"})
			return ok
		}},
		{"non-struct map", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[map[string]any](codec.Msgpack), []string{"A"})
			return ok
		}},
		{"non-struct int", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[int](codec.JSON), []string{"A"})
			return ok
		}},
		{"pointer to struct", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[*plainTwo](codec.Msgpack), []string{"A"})
			return ok
		}},
		{"embedded msgpack", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[partialEmbedded](codec.Msgpack), []string{"B"})
			return ok
		}},
		{"embedded json", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[partialEmbedded](codec.JSON), []string{"B"})
			return ok
		}},
		{"_msgpack field", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[partialArray](codec.Msgpack), []string{"A"})
			return ok
		}},
		{"_msgpack field is nothing to json", true, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[partialArray](codec.JSON), []string{"A"})
			return ok
		}},
		{"custom msgpack decoder", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[customMsgpack](codec.Msgpack), []string{"A"})
			return ok
		}},
		{"json unmarshaler", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[customJSON](codec.JSON), []string{"A"})
			return ok
		}},
		{"json unmarshaler is nothing to msgpack", true, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[customJSON](codec.Msgpack), []string{"A"})
			return ok
		}},
		{"case fold json", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[caseFold](codec.JSON), []string{"Foo"})
			return ok
		}},
		{"case fold tags json", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[tagFold](codec.JSON), []string{"A"})
			return ok
		}},
		{"case fold msgpack", true, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[caseFold](codec.Msgpack), []string{"Foo"})
			return ok
		}},
		{"unknown path", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](codec.Msgpack), []string{"Nope"})
			return ok
		}},
		{"one unknown path of two", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](codec.JSON), []string{"A", "Nope.X"})
			return ok
		}},
		{"unexported path", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](codec.Msgpack), []string{"b"})
			return ok
		}},
		{"empty path", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](codec.Msgpack), []string{""})
			return ok
		}},
		{"custom format", false, func() bool {
			_, ok := codec.PartialOf(codec.Formatted[plainTwo](customFormat{}), []string{"A"})
			return ok
		}},
		{"without partial msgpack", false, func() bool {
			_, ok := codec.PartialOf(codec.WithoutPartial(codec.Formatted[plainTwo](codec.Msgpack)), []string{"A"})
			return ok
		}},
		{"without partial binary", false, func() bool {
			_, ok := codec.PartialOf(codec.WithoutPartial(codec.Binary[partialBinary]()), []string{"A"})
			return ok
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.ok, tc.result())
		})
	}
}

// partialBinary encodes the two numbers and decodes the first alone when it is asked to.
type partialBinary struct {
	A, B   uint32
	Fields []string
}

func (p partialBinary) MarshalBinary() ([]byte, error) {
	bz := make([]byte, 8)
	binary.BigEndian.PutUint32(bz, p.A)
	binary.BigEndian.PutUint32(bz[4:], p.B)
	return bz, nil
}

func (p *partialBinary) UnmarshalBinary(bz []byte) error {
	p.A = binary.BigEndian.Uint32(bz)
	p.B = binary.BigEndian.Uint32(bz[4:])
	return nil
}

func (p *partialBinary) UnmarshalPartial(bz []byte, fields []string) error {
	p.Fields = fields
	p.A = binary.BigEndian.Uint32(bz)
	return nil
}

func TestPartialBinary(t *testing.T) {
	bz, err := partialBinary{A: 1, B: 2}.MarshalBinary()
	require.NoError(t, err)

	for name, c := range map[string]codec.ValueCodec[partialBinary]{
		"Binary":   codec.Binary[partialBinary](),
		"ValueFor": codec.ValueFor[partialBinary](nil),
	} {
		t.Run(name, func(t *testing.T) {
			fields := []string{"A", "B"}
			decode, ok := codec.PartialOf(c, fields)
			require.True(t, ok)

			// The slice of the caller can change after the decoder is made.
			fields[0] = "changed"

			var got partialBinary
			require.NoError(t, decode(bz, &got))
			require.Equal(t, partialBinary{A: 1, Fields: []string{"A", "B"}}, got)
		})
	}

	t.Run("not partial", func(t *testing.T) {
		_, ok := codec.PartialOf(codec.Binary[selfValue](), []string{"Name"})
		require.False(t, ok)
		_, ok = codec.PartialOf(codec.ValueFor[selfValue](nil), []string{"Name"})
		require.False(t, ok)
	})
}

type aliasRecord struct {
	Name string
	Data []byte
	N    int
}

func TestPartialDoesNotAliasInput(t *testing.T) {
	for _, format := range []codec.Format{codec.Msgpack, codec.JSON} {
		t.Run(format.Name(), func(t *testing.T) {
			c := codec.Formatted[aliasRecord](format)
			bz, err := c.Encode(aliasRecord{Name: "name", Data: []byte("data"), N: 5})
			require.NoError(t, err)

			decode, ok := codec.PartialOf(c, []string{"Name", "Data"})
			require.True(t, ok)

			var got aliasRecord
			require.NoError(t, decode(bz, &got))
			clear(bz)

			require.Equal(t, aliasRecord{Name: "name", Data: []byte("data")}, got)
		})
	}
}

func TestPartialConcurrent(t *testing.T) {
	for _, format := range []codec.Format{codec.Msgpack, codec.JSON} {
		t.Run(format.Name(), func(t *testing.T) {
			c := codec.Formatted[aliasRecord](format)
			decode, ok := codec.PartialOf(c, []string{"Name", "N"})
			require.True(t, ok)

			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for g := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 500 {
						want := aliasRecord{Name: fmt.Sprintf("g%d-%d", g, i), N: g*1000 + i}
						bz, err := c.Encode(aliasRecord{Name: want.Name, N: want.N, Data: []byte("x")})
						if err != nil {
							errs <- err
							return
						}
						var got aliasRecord
						if err := decode(bz, &got); err != nil {
							errs <- err
							return
						}
						if got.Name != want.Name || got.N != want.N || got.Data != nil {
							errs <- fmt.Errorf("got %+v, want %+v", got, want)
							return
						}
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
		})
	}
}

func TestPartialEmptyFields(t *testing.T) {
	c := codec.Formatted[aliasRecord](codec.Msgpack)
	bz, err := c.Encode(aliasRecord{Name: "name", N: 5})
	require.NoError(t, err)

	for _, fields := range [][]string{nil, {}} {
		decode, ok := codec.PartialOf(c, fields)
		require.True(t, ok)
		var got aliasRecord
		require.NoError(t, decode(bz, &got))
		require.Equal(t, aliasRecord{}, got)
	}

	_, ok := codec.PartialOf(codec.WithoutPartial(c), nil)
	require.False(t, ok)

	// A binary type that cannot decode partially has nothing to decode either.
	decode, ok := codec.PartialOf(codec.Binary[selfValue](), nil)
	require.True(t, ok)
	var sv selfValue
	require.NoError(t, decode([]byte("self:x"), &sv))
	require.Equal(t, selfValue{}, sv)
}

func TestWithoutPartialKeepsName(t *testing.T) {
	c := codec.Formatted[plainValue](codec.JSON)
	w := codec.WithoutPartial(c)
	require.Equal(t, c.Name(), w.Name())

	want := plainValue{Name: "foo", Count: 3, Tags: []string{"a"}}
	bz, err := w.Encode(want)
	require.NoError(t, err)
	wantBz, err := c.Encode(want)
	require.NoError(t, err)
	require.Equal(t, wantBz, bz)

	got, err := w.Decode(bz)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

type capRecord struct {
	F0, F1, F2, F3, F4, F5, F6, F7 int
}

func TestPartialCap(t *testing.T) {
	c := codec.Formatted[capRecord](codec.Msgpack)
	names := []string{"F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7"}

	var first []string
	oks := 0
	for mask := 1; mask < 1<<len(names); mask++ {
		var fields []string
		for i, n := range names {
			if mask&(1<<i) != 0 {
				fields = append(fields, n)
			}
		}
		if _, ok := codec.PartialOf(c, fields); ok {
			oks++
			if first == nil {
				first = fields
			}
		}
	}
	require.Equal(t, 64, oks)

	// What is made stays available, in either format of the type that has no shadows yet.
	_, ok := codec.PartialOf(c, first)
	require.True(t, ok)
	_, ok = codec.PartialOf(codec.Formatted[capRecord](codec.JSON), names[:2])
	require.False(t, ok)
}

type benchBook struct {
	Title   string
	Author  string
	Year    int64
	Score   int64
	Tags    []string
	Payload []byte
}

type benchDoc struct {
	Lang      string
	Year      int
	Embedding []float32
}

func benchPartial[V any](b *testing.B, v V, fieldSets map[string][]string) {
	c := codec.Formatted[V](codec.Msgpack)
	bz, err := c.Encode(v)
	require.NoError(b, err)

	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := c.Decode(bz); err != nil {
				b.Fatal(err)
			}
		}
	})
	for name, fields := range fieldSets {
		decode, ok := codec.PartialOf(c, fields)
		require.True(b, ok)
		b.Run("partial-"+name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var out V
				if err := decode(bz, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPartialDecode(b *testing.B) {
	b.Run("book", func(b *testing.B) {
		benchPartial(b, benchBook{
			Title: "A title of a book", Author: "An author", Year: 1999, Score: 7,
			Tags: []string{"a", "b", "c"}, Payload: make([]byte, 4096),
		}, map[string][]string{"Year": {"Year"}, "Title": {"Title"}})
	})
	b.Run("doc", func(b *testing.B) {
		benchPartial(b, benchDoc{Lang: "en", Year: 2020, Embedding: make([]float32, 384)},
			map[string][]string{"Year": {"Year"}, "Lang": {"Lang"}})
	})
}
