package kv_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/stretchr/testify/require"
)

func TestPrefixEnd(t *testing.T) {
	tests := []struct {
		prefix []byte
		want   []byte
	}{
		{nil, nil},
		{[]byte{}, nil},
		{[]byte("a"), []byte("b")},
		{[]byte("ab"), []byte("ac")},
		{[]byte{0x01, 0xff}, []byte{0x02}},
		{[]byte{0x01, 0xff, 0xff}, []byte{0x02}},
		{[]byte{0xff}, nil},
		{[]byte{0xff, 0xff}, nil},
	}
	for _, test := range tests {
		require.Equal(t, test.want, kv.PrefixEnd(test.prefix), "PrefixEnd(%v)", test.prefix)
	}

	// The prefix itself is not modified.
	prefix := []byte("ab")
	kv.PrefixEnd(prefix)
	require.Equal(t, []byte("ab"), prefix)
}
