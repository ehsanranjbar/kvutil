package iters_test

import (
	"encoding/binary"
	"testing"

	"github.com/ehsanranjbar/kvutil/legacy/iters"
	"github.com/stretchr/testify/assert"
)

func TestRewindSeek(t *testing.T) {
	it := iters.RewindSeek(iters.Slice([]int{1, 2, 3}), binary.BigEndian.AppendUint64(nil, 1))
	defer it.Close()

	actual, err := iters.Collect(it)
	assert.NoError(t, err)
	assert.Equal(t, []int{2, 3}, actual)
}
