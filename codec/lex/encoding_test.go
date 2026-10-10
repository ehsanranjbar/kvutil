package lex_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/stretchr/testify/require"
)

func TestEncodeInt32(t *testing.T) {
	tests := []struct {
		input    int32
		expected []byte
	}{
		{0, []byte{0x80, 0x00, 0x00, 0x00}},
		{1, []byte{0x80, 0x00, 0x00, 0x01}},
		{-1, []byte{0x7f, 0xff, 0xff, 0xff}},
		{math.MaxInt32, []byte{0xff, 0xff, 0xff, 0xff}},
		{math.MinInt32, []byte{0x00, 0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		result := lex.EncodeInt32(test.input)
		require.Equal(t, test.expected, result, "EncodeInt32(%d)", test.input)
	}
}

func TestDecodeInt32(t *testing.T) {
	tests := []struct {
		input    []byte
		expected int32
	}{
		{[]byte{0x80, 0x00, 0x00, 0x00}, 0},
		{[]byte{0x80, 0x00, 0x00, 0x01}, 1},
		{[]byte{0x7f, 0xff, 0xff, 0xff}, -1},
		{[]byte{0xff, 0xff, 0xff, 0xff}, math.MaxInt32},
		{[]byte{0x00, 0x00, 0x00, 0x00}, math.MinInt32},
	}

	for _, test := range tests {
		result := lex.DecodeInt32(test.input)
		require.Equal(t, test.expected, result, "DecodeInt32(%v)", test.input)
	}
}

func TestEncodeInt64(t *testing.T) {
	tests := []struct {
		input    int64
		expected []byte
	}{
		{0, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{-1, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.MaxInt64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.MinInt64, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for _, test := range tests {
		result := lex.EncodeInt64(test.input)
		require.Equal(t, test.expected, result, "EncodeInt64(%d)", test.input)
	}
}

func TestDecodeInt64(t *testing.T) {
	tests := []struct {
		input    []byte
		expected int64
	}{
		{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0},
		{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, 1},
		{[]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, -1},
		{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, math.MaxInt64},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, math.MinInt64},
	}

	for _, test := range tests {
		result := lex.DecodeInt64(test.input)
		require.Equal(t, test.expected, result, "DecodeInt64(%v)", test.input)
	}
}

func TestEncodeUint64(t *testing.T) {
	tests := []struct {
		input    uint64
		expected []byte
	}{
		{0, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{1, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{math.MaxUint64, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
	}

	for _, test := range tests {
		result := lex.EncodeUint64(test.input)
		require.Equal(t, test.expected, result, "EncodeUint64(%d)", test.input)
	}
}

func TestDecodeUint64(t *testing.T) {
	tests := []struct {
		input    []byte
		expected uint64
	}{
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0},
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, 1},
		{[]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, math.MaxUint64},
	}

	for _, test := range tests {
		result := lex.DecodeUint64(test.input)
		require.Equal(t, test.expected, result, "DecodeUint64(%v)", test.input)
	}
}

func TestEncodeFloat32(t *testing.T) {
	tests := []struct {
		input    float32
		expected []byte
	}{
		{float32(math.NaN()), []byte{0x00, 0x00, 0x00, 0x00}},
		{float32(math.Inf(-1)), []byte{0x00, 0x7f, 0xff, 0xff}},
		{-1, []byte{0x40, 0x7f, 0xff, 0xff}},
		{-math.SmallestNonzeroFloat32, []byte{0x7f, 0xff, 0xff, 0xfe}},
		{0, []byte{0x80, 0x0, 0x0, 0x0}},
		{math.SmallestNonzeroFloat32, []byte{0x80, 0x00, 0x00, 0x01}},
		{1, []byte{0xbf, 0x80, 0x00, 0x00}},
		{math.MaxFloat32, []byte{0xff, 0x7f, 0xff, 0xff}},
		{float32(math.Inf(1)), []byte{0xff, 0x80, 0x00, 0x00}},
	}

	for i, test := range tests {
		result := lex.EncodeFloat32(test.input)
		require.Equal(t, test.expected, result, "EncodeFloat32(%f)", test.input)

		if i < len(tests)-1 {
			require.True(t, bytes.Compare(result, tests[i+1].expected) < 0, "EncodeFloat32(%f) < EncodeFloat32(%f)", test.input, tests[i+1].input)
		}
	}
}

func TestDecodeFloat32(t *testing.T) {
	tests := []struct {
		input    []byte
		expected float32
	}{
		{[]byte{0x00, 0x00, 0x00, 0x00}, float32(math.NaN())},
		{[]byte{0x00, 0x7f, 0xff, 0xff}, float32(math.Inf(-1))},
		{[]byte{0x40, 0x7f, 0xff, 0xff}, -1},
		{[]byte{0x7f, 0xff, 0xff, 0xfe}, -math.SmallestNonzeroFloat32},
		{[]byte{0x80, 0x0, 0x0, 0x0}, 0},
		{[]byte{0x80, 0x00, 0x00, 0x01}, math.SmallestNonzeroFloat32},
		{[]byte{0xbf, 0x80, 0x00, 0x00}, 1},
		{[]byte{0xff, 0x7f, 0xff, 0xff}, math.MaxFloat32},
		{[]byte{0xff, 0x80, 0x00, 0x00}, float32(math.Inf(1))},
	}

	for _, test := range tests {
		result := lex.DecodeFloat32(test.input)
		if math.IsNaN(float64(test.expected)) {
			require.True(t, math.IsNaN(float64(result)), "DecodeFloat32(%v)", test.input)
			continue
		}
		require.Equal(t, test.expected, result, "DecodeFloat32(%v)", test.input)
	}
}

func TestEncodeFloat64(t *testing.T) {
	tests := []struct {
		input    float64
		expected []byte
	}{
		{math.NaN(), []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.Inf(-1), []byte{0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{-1, []byte{0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{-math.SmallestNonzeroFloat64, []byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}},
		{0, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.SmallestNonzeroFloat64, []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}},
		{1, []byte{0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
		{math.MaxFloat64, []byte{0xff, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}},
		{math.Inf(1), []byte{0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
	}

	for i, test := range tests {
		result := lex.EncodeFloat64(test.input)
		require.Equal(t, test.expected, result, "EncodeFloat64(%f)", test.input)

		if i < len(tests)-1 {
			require.True(t, bytes.Compare(result, tests[i+1].expected) < 0, "EncodeFloat64(%f) < EncodeFloat64(%f)", test.input, tests[i+1].input)
		}
	}
}

func TestDecodeFloat64(t *testing.T) {
	tests := []struct {
		input    []byte
		expected float64
	}{
		{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, math.NaN()},
		{[]byte{0x00, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, math.Inf(-1)},
		{[]byte{0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, -1},
		{[]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}, -math.SmallestNonzeroFloat64},
		{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0},
		{[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, math.SmallestNonzeroFloat64},
		{[]byte{0xbf, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 1},
		{[]byte{0xff, 0xef, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, math.MaxFloat64},
		{[]byte{0xff, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, math.Inf(1)},
	}

	for _, test := range tests {
		result := lex.DecodeFloat64(test.input)
		if math.IsNaN(test.expected) {
			require.True(t, math.IsNaN(result), "DecodeFloat64(%v)", test.input)
			continue
		}
		require.Equal(t, test.expected, result, "DecodeFloat64(%v)", test.input)
	}
}

func TestEncodeFloatNegativeZero(t *testing.T) {
	negZero32 := float32(math.Copysign(0, -1))
	require.Equal(t, lex.EncodeFloat32(0), lex.EncodeFloat32(negZero32))
	require.Equal(t, float32(0), lex.DecodeFloat32(lex.EncodeFloat32(negZero32)))

	negZero64 := math.Copysign(0, -1)
	require.Equal(t, lex.EncodeFloat64(0), lex.EncodeFloat64(negZero64))
	require.Equal(t, float64(0), lex.DecodeFloat64(lex.EncodeFloat64(negZero64)))
}

func TestInt16RoundTrip(t *testing.T) {
	ordered := []int16{math.MinInt16, -1, 0, 1, math.MaxInt16}
	for i, v := range ordered {
		bz := lex.EncodeInt16(v)
		require.Equal(t, v, lex.DecodeInt16(bz), "DecodeInt16(EncodeInt16(%d))", v)

		if i < len(ordered)-1 {
			require.True(t, bytes.Compare(bz, lex.EncodeInt16(ordered[i+1])) < 0, "EncodeInt16(%d) < EncodeInt16(%d)", v, ordered[i+1])
		}
	}

	for _, v := range []uint16{0, 1, math.MaxUint16} {
		require.Equal(t, v, lex.DecodeUint16(lex.EncodeUint16(v)), "DecodeUint16(EncodeUint16(%d))", v)
	}
}
