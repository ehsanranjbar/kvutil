package codec_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func testRoundTrip[T any](t *testing.T, ordered ...T) {
	t.Helper()

	c, err := codec.CodecFor[T]()
	require.NoError(t, err)

	var prev []byte
	for i, v := range ordered {
		bz, err := c.Encode(v)
		require.NoError(t, err)

		got, err := c.Decode(bz)
		require.NoError(t, err)
		require.Equal(t, v, got)

		if i > 0 {
			require.True(t, bytes.Compare(prev, bz) < 0, "%v must sort before %v", ordered[i-1], v)
		}
		prev = bz
	}
}

func TestCodecFor(t *testing.T) {
	t.Run("Integers", func(t *testing.T) {
		testRoundTrip[int](t, math.MinInt, -1, 0, 1, math.MaxInt)
		testRoundTrip[int8](t, math.MinInt8, -1, 0, 1, math.MaxInt8)
		testRoundTrip[int16](t, math.MinInt16, -1, 0, 1, math.MaxInt16)
		testRoundTrip[int32](t, math.MinInt32, -1, 0, 1, math.MaxInt32)
		testRoundTrip[int64](t, math.MinInt64, -1, 0, 1, math.MaxInt64)
		testRoundTrip[uint](t, 0, 1, math.MaxUint)
		testRoundTrip[uint8](t, 0, 1, math.MaxUint8)
		testRoundTrip[uint16](t, 0, 1, math.MaxUint16)
		testRoundTrip[uint32](t, 0, 1, math.MaxUint32)
		testRoundTrip[uint64](t, 0, 1, math.MaxUint64)
	})

	t.Run("StringAndBytes", func(t *testing.T) {
		testRoundTrip(t, "a", "ab", "b")
		testRoundTrip(t, []byte("a"), []byte("ab"), []byte("b"))
	})

	t.Run("BinaryMarshaler", func(t *testing.T) {
		testRoundTrip(t, uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.MustParse("00000000-0000-0000-0000-000000000002"))
	})

	t.Run("InvalidLength", func(t *testing.T) {
		c, err := codec.CodecFor[int32]()
		require.NoError(t, err)

		_, err = c.Decode([]byte{0x01})
		require.Error(t, err)
	})

	t.Run("Unsupported", func(t *testing.T) {
		type named int64

		_, err := codec.CodecFor[named]()
		require.Error(t, err)

		_, err = codec.CodecFor[struct{}]()
		require.Error(t, err)

		_, err = codec.CodecFor[float64]()
		require.Error(t, err)
	})
}
