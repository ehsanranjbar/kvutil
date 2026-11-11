package index_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ehsanranjbar/kvutil/cond"
	"github.com/ehsanranjbar/kvutil/index"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

func TestCoverReads(t *testing.T) {
	// The function of a cover is opaque, so what it reads is not known.
	var cover cond.Reader = chipLen
	paths, ok := cover.Reads()
	require.False(t, ok)
	require.Nil(t, paths)

	// Reading declares it, on the cover that it is called on.
	declared := index.Cover("ChipLen2", func(p *pet) int { return len(p.Chip) }).Reading("Chip")
	paths, ok = declared.Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Chip"}, paths)

	nothing := index.Cover("Zero", func(*pet) int { return 0 }).Reading()
	paths, ok = nothing.Reads()
	require.True(t, ok)
	require.Empty(t, paths)

	// The paths are copied.
	given := []string{"Name", "Age"}
	two := index.Cover("Two", func(p *pet) int { return p.Age }).Reading(given...)
	given[0] = "Chip"
	paths, _ = two.Reads()
	require.Equal(t, []string{"Name", "Age"}, paths)
	paths[0] = "Chip"
	paths, _ = two.Reads()
	require.Equal(t, []string{"Name", "Age"}, paths)

	// A cover that is given to an index keeps what it reads.
	store := extstore.New[int64, pet]("pets")
	index.On(store, "by_owner", ownerField, index.Covering(declared))
	paths, ok = declared.Reads()
	require.True(t, ok)
	require.Equal(t, []string{"Chip"}, paths)
}
