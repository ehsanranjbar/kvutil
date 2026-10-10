package lex_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/stretchr/testify/require"
)

func TestInverse(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x00}, []byte{0xff}},
		{[]byte{0xff}, []byte{0x00}},
		{[]byte{0x55, 0xaa}, []byte{0xaa, 0x55}},
		{[]byte{0x00, 0xff, 0x7f, 0x80}, []byte{0xff, 0x00, 0x80, 0x7f}},
	}

	for _, test := range tests {
		result := lex.Invert(test.input)
		require.Equal(t, test.expected, result, "Inverse(%v)", test.input)
	}
}

func TestIncrement(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0x01}, []byte{0x02}},
		{[]byte{0xff}, []byte{0xff, 0x01}},
		{[]byte{0x00, 0xff}, []byte{0x01, 0x00}},
		{[]byte{0xff, 0xff}, []byte{0xff, 0xff, 0x01}},
	}

	for _, test := range tests {
		result := lex.Increment(test.input)
		require.Equal(t, test.expected, result, "Increment(%v)", test.input)
	}
}
