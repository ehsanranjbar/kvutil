package codec_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/stretchr/testify/require"
)

type namedBool bool

// sameAsKeyFor checks that the codec of the type of a value that is found by reflection is
// the codec that KeyFor returns for the type, for the given values.
func sameAsKeyFor[K any](t *testing.T, values ...K) {
	t.Helper()

	typed, err := codec.KeyFor[K]()
	require.NoError(t, err)
	untyped, err := codec.KeyForType(reflect.TypeFor[K]())
	require.NoError(t, err)
	require.Equal(t, typed.Name(), untyped.Name())

	for _, value := range values {
		want, err := typed.Encode(value)
		require.NoError(t, err)
		got, err := untyped.Encode(value)
		require.NoError(t, err)
		require.Equal(t, want, got)

		decoded, err := untyped.Decode(got)
		require.NoError(t, err)
		require.IsType(t, value, decoded)
		require.Equal(t, value, decoded)

		want, err = typed.Append([]byte{0xaa}, value)
		require.NoError(t, err)
		got, err = untyped.Append([]byte{0xaa}, value)
		require.NoError(t, err)
		require.Equal(t, want, got)

		// What follows the key is left alone.
		read, n, err := untyped.Read(append(got[1:], 0xbb, 0xcc))
		require.NoError(t, err)
		require.Equal(t, len(got)-1, n)
		require.IsType(t, value, read)
		require.Equal(t, value, read)
	}
}

func TestKeyForType(t *testing.T) {
	sameAsKeyFor(t, "", "a", "a\x00b")
	sameAsKeyFor(t, []byte{}, []byte{0, 1, 0xff})
	sameAsKeyFor(t, 0, -1, 1<<40)
	sameAsKeyFor[int8](t, -128, 0, 127)
	sameAsKeyFor[int16](t, -1, 300)
	sameAsKeyFor[int32](t, -1, 70000)
	sameAsKeyFor[int64](t, -1<<62, 1<<62)
	sameAsKeyFor[uint](t, 0, 1<<63)
	sameAsKeyFor[uint8](t, 0, 255)
	sameAsKeyFor[uint16](t, 65535)
	sameAsKeyFor[uint32](t, 1<<31)
	sameAsKeyFor[uint64](t, 1<<63)
	sameAsKeyFor[float32](t, -1.5, 0, 2.25)
	sameAsKeyFor(t, -1.5, 0, 1e300)
	sameAsKeyFor(t, false, true)
	sameAsKeyFor(t, time.Date(2024, 2, 29, 12, 30, 0, 5, time.UTC), time.Unix(-5, 0).UTC())
	sameAsKeyFor(t, [16]byte{1, 2, 3}, [16]byte{})

	// The named types have the codecs of the types that they are defined on.
	sameAsKeyFor[namedString](t, "", "named")
	sameAsKeyFor[namedInt](t, -7, 7)
	sameAsKeyFor[namedUint](t, 0, 65535)
	sameAsKeyFor[namedFloat](t, -0.5, 0.5)
	sameAsKeyFor[namedBool](t, true)
	sameAsKeyFor(t, namedBytes{}, namedBytes("named"))
	sameAsKeyFor(t, namedArray{1, 2, 3, 4})

	// Pairs, of pairs too.
	sameAsKeyFor(t, codec.NewPair("a", int64(1)), codec.NewPair("", int64(-1)))
	sameAsKeyFor(t, codec.NewPair(namedInt(1), codec.NewPair("b", true)))

	t.Run("WrongType", func(t *testing.T) {
		for _, test := range []struct {
			of    reflect.Type
			value any
		}{
			{reflect.TypeFor[int](), int64(1)},
			{reflect.TypeFor[int](), "1"},
			{reflect.TypeFor[string](), namedString("a")},
			{reflect.TypeFor[namedString](), "a"},
			{reflect.TypeFor[[4]byte](), namedArray{}},
			{reflect.TypeFor[[4]byte](), [5]byte{}},
			{reflect.TypeFor[time.Time](), nil},
			{reflect.TypeFor[codec.Pair[string, int]](), codec.NewPair("a", "b")},
		} {
			c, err := codec.KeyForType(test.of)
			require.NoError(t, err)

			_, err = c.Encode(test.value)
			require.Error(t, err, "%s with %T", test.of, test.value)
			_, err = c.Append(nil, test.value)
			require.Error(t, err, "%s with %T", test.of, test.value)
		}
	})

	t.Run("NoCodec", func(t *testing.T) {
		for _, of := range []reflect.Type{
			reflect.TypeFor[struct{ A int }](),
			reflect.TypeFor[[]string](),
			reflect.TypeFor[map[string]int](),
			reflect.TypeFor[*int](),
			reflect.TypeFor[[2]int](),
			reflect.TypeFor[codec.Pair[string, []int]](),
		} {
			_, err := codec.KeyForType(of)
			require.Error(t, err, of.String())
		}
	})

	t.Run("ShortKeys", func(t *testing.T) {
		c, err := codec.KeyForType(reflect.TypeFor[namedArray]())
		require.NoError(t, err)
		_, err = c.Decode([]byte{1, 2, 3})
		require.Error(t, err)
		_, err = c.Decode([]byte{1, 2, 3, 4, 5})
		require.Error(t, err)
		_, _, err = c.Read([]byte{1, 2, 3})
		require.Error(t, err)

		c, err = codec.KeyForType(reflect.TypeFor[namedInt]())
		require.NoError(t, err)
		_, err = c.Decode([]byte{1})
		require.Error(t, err)
		_, _, err = c.Read([]byte{1})
		require.Error(t, err)
	})
}
