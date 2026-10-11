package iters_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/legacy/iters"
	"github.com/stretchr/testify/require"
)

func TestCollect(t *testing.T) {
	var (
		items = []int{1, 2, 3}
		it    = iters.Slice(items)
	)

	collected, err := iters.Collect(it)
	require.NoError(t, err)
	require.Equal(t, items, collected)
}

func TestCollectKeys(t *testing.T) {
	var (
		items = []int{1, 2, 3}
		it    = iters.Enumerate[byte](iters.Slice(items))
	)

	collected, err := iters.CollectKeys(it)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 2}, collected)
}
