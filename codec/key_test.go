package codec_test

import (
	"bytes"
	"cmp"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// testKeyCodec checks that random keys survive both encodings of the codec and that the
// encodings sort as the keys do.
func testKeyCodec[K any](
	t *testing.T,
	c codec.KeyCodec[K],
	random func(*rand.Rand) K,
	compare func(a, b K) int,
) {
	t.Helper()

	rng := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		a, b := random(rng), random(rng)
		want := compare(a, b)

		ea, err := c.Encode(a)
		require.NoError(t, err)
		eb, err := c.Encode(b)
		require.NoError(t, err)
		require.Equal(t, want, bytes.Compare(ea, eb), "Encode: %v and %v", a, b)

		got, err := c.Decode(ea)
		require.NoError(t, err)
		require.Zero(t, compare(a, got), "Decode: %v became %v", a, got)

		// The other form, followed by the bytes of another component.
		suffix := []byte{0x00, 0xff, byte(rng.IntN(256))}
		aa, err := c.Append([]byte("x"), a)
		require.NoError(t, err)
		ab, err := c.Append([]byte("x"), b)
		require.NoError(t, err)
		if want != 0 {
			require.Equal(t, want, bytes.Compare(append(aa, 0xff), append(ab, 0x00)), "Append: %v and %v", a, b)
		} else {
			require.Equal(t, aa, ab, "Append: %v and %v", a, b)
		}

		got, n, err := c.Read(append(aa[1:], suffix...))
		require.NoError(t, err)
		require.Equal(t, len(aa)-1, n)
		require.Zero(t, compare(a, got), "Read: %v became %v", a, got)
	}
}

func randomInt[K interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}](rng *rand.Rand) K {
	// Small numbers of either sign as often as the ones that use every byte.
	switch rng.IntN(3) {
	case 0:
		return K(rng.IntN(5)) - 2
	case 1:
		return K(rng.Uint64() >> uint(rng.IntN(64)))
	default:
		return K(rng.Uint64())
	}
}

func randomBytes(rng *rand.Rand) []byte {
	b := make([]byte, rng.IntN(4))
	for i := range b {
		b[i] = []byte{0x00, 0x01, 'a', 0xfe, 0xff}[rng.IntN(5)]
	}
	return b
}

func keyFor[K any](t *testing.T) codec.KeyCodec[K] {
	t.Helper()

	c, err := codec.KeyFor[K]()
	require.NoError(t, err)
	return c
}

type (
	namedInt    int32
	namedUint   uint16
	namedString string
	namedBytes  []byte
	namedArray  [4]byte
)

func TestKeyFor(t *testing.T) {
	t.Run("Integers", func(t *testing.T) {
		testKeyCodec(t, keyFor[int](t), randomInt[int], cmp.Compare[int])
		testKeyCodec(t, keyFor[int8](t), randomInt[int8], cmp.Compare[int8])
		testKeyCodec(t, keyFor[int16](t), randomInt[int16], cmp.Compare[int16])
		testKeyCodec(t, keyFor[int32](t), randomInt[int32], cmp.Compare[int32])
		testKeyCodec(t, keyFor[int64](t), randomInt[int64], cmp.Compare[int64])
		testKeyCodec(t, keyFor[uint](t), randomInt[uint], cmp.Compare[uint])
		testKeyCodec(t, keyFor[uint8](t), randomInt[uint8], cmp.Compare[uint8])
		testKeyCodec(t, keyFor[uint16](t), randomInt[uint16], cmp.Compare[uint16])
		testKeyCodec(t, keyFor[uint32](t), randomInt[uint32], cmp.Compare[uint32])
		testKeyCodec(t, keyFor[uint64](t), randomInt[uint64], cmp.Compare[uint64])
	})

	t.Run("IntegerLayout", func(t *testing.T) {
		for _, test := range []struct {
			got  func() ([]byte, error)
			want []byte
		}{
			{func() ([]byte, error) { return keyFor[int64](t).Encode(math.MinInt64) }, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
			{func() ([]byte, error) { return keyFor[int64](t).Encode(-1) }, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
			{func() ([]byte, error) { return keyFor[int64](t).Encode(1) }, []byte{0x80, 0, 0, 0, 0, 0, 0, 1}},
			{func() ([]byte, error) { return keyFor[int](t).Encode(1) }, []byte{0x80, 0, 0, 0, 0, 0, 0, 1}},
			{func() ([]byte, error) { return keyFor[uint](t).Encode(1) }, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
			{func() ([]byte, error) { return keyFor[int8](t).Encode(-1) }, []byte{0x7f}},
			{func() ([]byte, error) { return keyFor[int16](t).Encode(1) }, []byte{0x80, 0x01}},
			{func() ([]byte, error) { return keyFor[uint32](t).Encode(math.MaxUint32) }, []byte{0xff, 0xff, 0xff, 0xff}},
		} {
			got, err := test.got()
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		}
	})

	t.Run("StringAndBytes", func(t *testing.T) {
		testKeyCodec(t, keyFor[string](t),
			func(rng *rand.Rand) string { return string(randomBytes(rng)) },
			cmp.Compare[string])
		testKeyCodec(t, keyFor[[]byte](t), randomBytes, bytes.Compare)

		// A string that is the whole key is stored as it is.
		bz, err := keyFor[string](t).Encode("a\x00b")
		require.NoError(t, err)
		require.Equal(t, []byte("a\x00b"), bz)
	})

	t.Run("BytesAreCopied", func(t *testing.T) {
		c := keyFor[[]byte](t)
		key := []byte("abc")

		bz, err := c.Encode(key)
		require.NoError(t, err)
		bz[0] = 'x'
		require.Equal(t, []byte("abc"), key)

		decoded, err := c.Decode(key)
		require.NoError(t, err)
		decoded[0] = 'x'
		require.Equal(t, []byte("abc"), key)
	})

	t.Run("Time", func(t *testing.T) {
		c := keyFor[time.Time](t)
		testKeyCodec(t, c,
			func(rng *rand.Rand) time.Time {
				// Around the epoch, on both sides of it.
				return time.Unix(rng.Int64N(7)-3, rng.Int64N(3)*500_000_000)
			},
			func(a, b time.Time) int { return a.Compare(b) })

		for _, want := range []time.Time{
			{},
			time.Date(1969, 12, 31, 23, 59, 59, 999_999_999, time.UTC),
			time.Date(2026, 10, 10, 12, 0, 0, 1, time.FixedZone("x", 3600)),
			time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		} {
			bz, err := c.Encode(want)
			require.NoError(t, err)
			got, err := c.Decode(bz)
			require.NoError(t, err)
			require.True(t, want.Equal(got), "%v became %v", want, got)
			require.Equal(t, time.UTC, got.Location())
		}
	})

	t.Run("Array", func(t *testing.T) {
		testKeyCodec(t, keyFor[uuid.UUID](t),
			func(rng *rand.Rand) uuid.UUID {
				var id uuid.UUID
				id[rng.IntN(16)] = byte(rng.IntN(3))
				return id
			},
			func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })

		id := uuid.MustParse("00010203-0405-0607-0809-0a0b0c0d0e0f")
		bz, err := keyFor[uuid.UUID](t).Encode(id)
		require.NoError(t, err)
		require.Equal(t, id[:], bz)
	})

	t.Run("NamedTypes", func(t *testing.T) {
		testKeyCodec(t, keyFor[namedInt](t), randomInt[namedInt], cmp.Compare[namedInt])
		testKeyCodec(t, keyFor[namedUint](t), randomInt[namedUint], cmp.Compare[namedUint])
		testKeyCodec(t, keyFor[namedString](t),
			func(rng *rand.Rand) namedString { return namedString(randomBytes(rng)) },
			cmp.Compare[namedString])
		testKeyCodec(t, keyFor[namedBytes](t),
			func(rng *rand.Rand) namedBytes { return randomBytes(rng) },
			func(a, b namedBytes) int { return bytes.Compare(a, b) })
		testKeyCodec(t, keyFor[namedArray](t),
			func(rng *rand.Rand) namedArray {
				var a namedArray
				a[rng.IntN(len(a))] = byte(rng.IntN(3))
				return a
			},
			func(a, b namedArray) int { return bytes.Compare(a[:], b[:]) })

		// A named type is encoded as the type that it is defined on.
		named, err := keyFor[namedInt](t).Encode(-5)
		require.NoError(t, err)
		plain, err := keyFor[int32](t).Encode(-5)
		require.NoError(t, err)
		require.Equal(t, plain, named)
		require.Equal(t, keyFor[int32](t).Name(), keyFor[namedInt](t).Name())
	})

	t.Run("InvalidLength", func(t *testing.T) {
		_, err := keyFor[int32](t).Decode([]byte{0x01})
		require.Error(t, err)
		_, err = keyFor[int32](t).Decode([]byte{0x01, 0x02, 0x03, 0x04, 0x05})
		require.Error(t, err)
		_, _, err = keyFor[int32](t).Read([]byte{0x01})
		require.Error(t, err)

		_, err = keyFor[time.Time](t).Decode([]byte{0x01})
		require.Error(t, err)
		_, err = keyFor[uuid.UUID](t).Decode([]byte{0x01})
		require.Error(t, err)
		_, _, err = keyFor[string](t).Read([]byte("unterminated"))
		require.Error(t, err)
	})

	t.Run("Unsupported", func(t *testing.T) {
		_, err := codec.KeyFor[struct{}]()
		require.Error(t, err)
		_, err = codec.KeyFor[float64]()
		require.Error(t, err)
		_, err = codec.KeyFor[[]string]()
		require.Error(t, err)
		_, err = codec.KeyFor[map[string]int]()
		require.Error(t, err)
		_, err = codec.KeyFor[codec.Pair[string, float64]]()
		require.Error(t, err)
	})
}

func TestPairKey(t *testing.T) {
	comparePair := func(a, b codec.Pair[string, int64]) int {
		return cmp.Or(cmp.Compare(a.First, b.First), cmp.Compare(a.Second, b.Second))
	}
	randomPair := func(rng *rand.Rand) codec.Pair[string, int64] {
		return codec.NewPair(string(randomBytes(rng)), rng.Int64N(5)-2)
	}

	t.Run("Derived", func(t *testing.T) {
		c := keyFor[codec.Pair[string, int64]](t)
		require.Equal(t, "pair(string,int64)", c.Name())
		testKeyCodec(t, c, randomPair, comparePair)
	})

	t.Run("Explicit", func(t *testing.T) {
		c := codec.PairKey(codec.StringKey[string](), codec.IntKey[int64]())
		testKeyCodec(t, c, randomPair, comparePair)
	})

	t.Run("VariableLengthLast", func(t *testing.T) {
		c := keyFor[codec.Pair[int64, string]](t)
		testKeyCodec(t, c,
			func(rng *rand.Rand) codec.Pair[int64, string] {
				return codec.NewPair(rng.Int64N(5)-2, string(randomBytes(rng)))
			},
			func(a, b codec.Pair[int64, string]) int {
				return cmp.Or(cmp.Compare(a.First, b.First), cmp.Compare(a.Second, b.Second))
			})

		// The last component of a whole key is stored as it is.
		bz, err := c.Encode(codec.NewPair(int64(1), "ab"))
		require.NoError(t, err)
		require.Equal(t, []byte{0x80, 0, 0, 0, 0, 0, 0, 1, 'a', 'b'}, bz)
	})

	t.Run("Nested", func(t *testing.T) {
		type triple = codec.Pair[string, codec.Pair[string, uint8]]

		c := keyFor[triple](t)
		require.Equal(t, "pair(string,pair(string,uint8))", c.Name())
		testKeyCodec(t, c,
			func(rng *rand.Rand) triple {
				return codec.NewPair(string(randomBytes(rng)), codec.NewPair(string(randomBytes(rng)), uint8(rng.IntN(3))))
			},
			func(a, b triple) int {
				return cmp.Or(
					cmp.Compare(a.First, b.First),
					cmp.Compare(a.Second.First, b.Second.First),
					cmp.Compare(a.Second.Second, b.Second.Second),
				)
			})
	})
}
