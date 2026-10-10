package badgerkv

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangePrefix(t *testing.T) {
	tests := []struct {
		start, end string
		want       string
	}{
		{"", "", ""},
		{"a", "", ""},
		{"", "b", ""},
		{"a", "b", "a"},    // the range of the prefix a
		{"ab", "ac", "ab"}, // the range of the prefix ab
		{"ab\x05", "ac", "ab"},
		{"a", "c", ""},
		{"ab", "ab\x00", "ab"}, // only the key ab
		{"aa", "ab\x00", "a"},
		{"a\xff", "b", "a"}, // correct, though a\xff would be tighter
	}
	for _, test := range tests {
		got := rangePrefix([]byte(test.start), []byte(test.end))
		require.Equal(t, test.want, string(got), "[%q, %q)", test.start, test.end)
	}

	// Every key of a range must have the prefix of the range.
	rng := rand.New(rand.NewPCG(1, 2))
	randomKey := func() []byte {
		key := make([]byte, 1+rng.IntN(3))
		for i := range key {
			key[i] = []byte{0x00, 0x01, 0xfe, 0xff}[rng.IntN(4)]
		}
		return key
	}
	for range 20000 {
		start, end, key := randomKey(), randomKey(), randomKey()
		if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
			continue
		}
		prefix := rangePrefix(start, end)
		require.True(t, bytes.HasPrefix(key, prefix), "key %q of [%q, %q) has no prefix %q", key, start, end, prefix)
	}
}
