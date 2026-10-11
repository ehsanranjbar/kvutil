package kvutil_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil"
	"github.com/stretchr/testify/require"
)

func TestRange(t *testing.T) {
	type side struct {
		value     int
		exclusive bool
		ok        bool
	}
	tests := []struct {
		r         kvutil.Range[int]
		low, high side
		text      string
	}{
		{kvutil.All[int](), side{}, side{}, "(-∞, ∞)"},
		{kvutil.Range[int]{}, side{}, side{}, "(-∞, ∞)"},
		{kvutil.From(10), side{10, false, true}, side{}, "[10, ∞)"},
		{kvutil.After(10), side{10, true, true}, side{}, "(10, ∞)"},
		{kvutil.Until(20), side{}, side{20, true, true}, "(-∞, 20)"},
		{kvutil.Through(20), side{}, side{20, false, true}, "(-∞, 20]"},
		{kvutil.From(10).Until(20), side{10, false, true}, side{20, true, true}, "[10, 20)"},
		{kvutil.After(10).Through(20), side{10, true, true}, side{20, false, true}, "(10, 20]"},
		{kvutil.Until(20).From(10), side{10, false, true}, side{20, true, true}, "[10, 20)"},
		{kvutil.Through(20).After(10), side{10, true, true}, side{20, false, true}, "(10, 20]"},
		{kvutil.Between(10, 20), side{10, false, true}, side{20, false, true}, "[10, 20]"},
		{kvutil.Only(10), side{10, false, true}, side{10, false, true}, "[10, 10]"},
		// A side that is set again replaces the one before.
		{kvutil.From(1).After(10), side{10, true, true}, side{}, "(10, ∞)"},
	}
	for _, test := range tests {
		value, exclusive, ok := test.r.Low()
		require.Equal(t, test.low, side{value, exclusive, ok}, test.text)
		value, exclusive, ok = test.r.High()
		require.Equal(t, test.high, side{value, exclusive, ok}, test.text)
		require.Equal(t, test.text, test.r.String())
	}

	// A range is a value: making another one from it leaves it as it is.
	base := kvutil.From("a")
	_ = base.Until("z")
	_, _, ok := base.High()
	require.False(t, ok)
}
