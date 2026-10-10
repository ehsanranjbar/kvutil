package expr_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/stretchr/testify/require"
)

func TestRangeString(t *testing.T) {
	tests := []struct {
		partition expr.Range[[]byte]
		expected  string
	}{
		{
			partition: expr.NewRange(expr.NewBound([]byte{0x01}, false), expr.NewBound([]byte{0x02}, false)),
			expected:  "[0x01, 0x02]",
		},
		{
			partition: expr.NewRange(expr.NewBound[[]byte]([]byte{0x01}, true), expr.NewBound[[]byte]([]byte{0x02}, true)),
			expected:  "(0x01, 0x02)",
		},
		{
			partition: expr.NewRange[[]byte](nil, nil),
			expected:  "[0x00, ∞)",
		},
	}

	for _, test := range tests {
		result := test.partition.String()
		require.Equal(t, test.expected, result)
	}
}
