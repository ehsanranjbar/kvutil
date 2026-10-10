package lex_test

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/stretchr/testify/require"
)

func TestAppendTerminated(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{nil, []byte{0x00, 0x01}},
		{[]byte("a"), []byte{'a', 0x00, 0x01}},
		{[]byte{0x00}, []byte{0x00, 0xff, 0x00, 0x01}},
		{[]byte{'a', 0x00, 'b'}, []byte{'a', 0x00, 0xff, 'b', 0x00, 0x01}},
		{[]byte{0x00, 0x00}, []byte{0x00, 0xff, 0x00, 0xff, 0x00, 0x01}},
		{[]byte{0x01, 0xff}, []byte{0x01, 0xff, 0x00, 0x01}},
	}
	for _, test := range tests {
		result := lex.AppendTerminated(nil, test.input)
		require.Equal(t, test.expected, result, "AppendTerminated(%v)", test.input)

		value, n, err := lex.ReadTerminated(append(result, 0xff, 0x00))
		require.NoError(t, err)
		require.Equal(t, len(result), n)
		require.True(t, bytes.Equal(test.input, value), "ReadTerminated(%v)", result)
	}

	// The destination is appended to.
	require.Equal(t, []byte{'x', 'a', 0x00, 0x01}, lex.AppendTerminated([]byte("x"), []byte("a")))
}

func TestReadTerminatedInvalid(t *testing.T) {
	for _, input := range [][]byte{
		nil,
		[]byte("a"),
		{'a', 0x00},
		{'a', 0x00, 0xff},
		{'a', 0x00, 0xff, 'b'},
	} {
		_, _, err := lex.ReadTerminated(input)
		require.ErrorIs(t, err, lex.ErrUnterminated, "ReadTerminated(%v)", input)
	}

	_, _, err := lex.ReadTerminated([]byte{'a', 0x00, 0x02})
	require.Error(t, err)
}

func TestTerminatedOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	random := func() []byte {
		b := make([]byte, rng.IntN(4))
		for i := range b {
			b[i] = []byte{0x00, 0x01, 0x02, 0xfe, 0xff}[rng.IntN(5)]
		}
		return b
	}

	for range 20000 {
		a, b := random(), random()
		ea, eb := lex.AppendTerminated(nil, a), lex.AppendTerminated(nil, b)
		require.Equal(t, bytes.Compare(a, b), bytes.Compare(ea, eb), "%v and %v", a, b)

		// The order holds when other components follow the values.
		suffixA, suffixB := random(), random()
		if !bytes.Equal(a, b) {
			require.Equal(t, bytes.Compare(a, b), bytes.Compare(append(ea, suffixA...), append(eb, suffixB...)), "%v and %v", a, b)
		}

		value, n, err := lex.ReadTerminated(append(ea, suffixA...))
		require.NoError(t, err)
		require.Equal(t, len(ea), n)
		require.True(t, bytes.Equal(a, value))
	}
}
