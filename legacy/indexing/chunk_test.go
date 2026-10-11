package indexing_test

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/expr"
	"github.com/ehsanranjbar/kvutil/legacy/indexing"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	refstore "github.com/ehsanranjbar/kvutil/legacy/store/ref"
	"github.com/ehsanranjbar/kvutil/legacy/testutil"
	"github.com/stretchr/testify/require"
)

func TestLookupChunks(t *testing.T) {
	store := refstore.New(nil)

	txn := testutil.PrepareTxn(t, true)
	ins := store.Instantiate(txn).(*refstore.Instance)

	var n uint64 = 0
	for i := 'A'; i <= 'Z'; i++ {
		ins.Set(binary.BigEndian.AppendUint64(nil, n), refstore.NewRefEntry([]byte{byte(i)}))
		n++

		for j := 'A'; j <= 'Z'; j++ {
			ins.Set(binary.BigEndian.AppendUint64(nil, n), refstore.NewRefEntry([]byte{byte(i), byte(j)}))
			n++
		}
	}

	tests := []struct {
		name           string
		parts          []indexing.Chunk
		expectedCount  int
		expectedRanges []expr.Range[uint64]
	}{
		{
			name: "[0x00, ∞)",
			parts: []indexing.Chunk{
				indexing.NewChunk(nil, nil),
			},
			expectedCount: 702,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](0, false), expr.NewBound[uint64](702, false)),
			},
		},
		{
			name: "[A, B)",
			parts: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound([]byte{'A'}, false),
					expr.NewBound([]byte{'B'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](0, false), expr.NewBound[uint64](26, false)),
			},
		},
		{
			name: "(A, B]",
			parts: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound([]byte{'A'}, true),
					expr.NewBound([]byte{'B'}, false),
				),
			},
			expectedCount: 27,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](1, false), expr.NewBound[uint64](27, false)),
			},
		},
		{
			name: "[X, Y)",
			parts: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound([]byte{'X'}, false),
					expr.NewBound([]byte{'Y'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](621, false), expr.NewBound[uint64](647, false)),
			},
		},
		{
			name: "[A, B), [X, Y)",
			parts: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound([]byte{'A'}, false),
					expr.NewBound([]byte{'B'}, true),
				),
				indexing.NewChunk(
					expr.NewBound([]byte{'X'}, false),
					expr.NewBound([]byte{'Y'}, true),
				),
			},
			expectedCount: 54,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](0, false), expr.NewBound[uint64](26, false)),
				expr.NewRange(expr.NewBound[uint64](621, false), expr.NewBound[uint64](647, false)),
			},
		},
		{
			name: "(∞, B)",
			parts: []indexing.Chunk{
				indexing.NewChunk(
					nil,
					expr.NewBound([]byte{'B'}, true),
				),
			},
			expectedCount: 27,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](0, false), expr.NewBound[uint64](26, false)),
			},
		},
		{
			name: "[Y, ∞)",
			parts: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound([]byte{'Y'}, false),
					nil,
				),
			},
			expectedCount: 54,
			expectedRanges: []expr.Range[uint64]{
				expr.NewRange(expr.NewBound[uint64](648, false), expr.NewBound[uint64](701, false)),
			},
		},
	}

	opts := []badger.IteratorOptions{
		badger.DefaultIteratorOptions,
		{
			PrefetchValues: true,
			PrefetchSize:   100,
			Reverse:        true,
			AllVersions:    false,
		},
	}

	for _, test := range tests {
		for _, opt := range opts {
			t.Run(test.name, func(t *testing.T) {
				it := indexing.LookupChunks(ins, iters.Slice(test.parts), opt)
				defer it.Close()

				actual, err := iters.Collect(it)
				require.NoError(t, err)
				require.Len(t, actual, test.expectedCount)
				require.True(t, areWithinChunks(actual, test.expectedRanges), "collected keys are not within expected ranges")

				// Checking multi-chunk sorting is a bit difficult, so we skip it for now.
				if len(test.expectedRanges) < 2 {
					cmpFunc := bytes.Compare
					if opt.Reverse {
						cmpFunc = func(a, b []byte) int {
							return -bytes.Compare(a, b)
						}
					}
					require.True(t, slices.IsSortedFunc(actual, cmpFunc), "collected keys are not sorted")
				}
			})
		}
	}
}

func areWithinChunks(items [][]byte, ranges []expr.Range[uint64]) bool {
	for _, item := range items {
		if !isWithinChunks(item, ranges) {
			return false
		}
	}

	return true
}

func isWithinChunks(item []byte, ranges []expr.Range[uint64]) bool {
	for _, r := range ranges {
		if isWithin(r, binary.BigEndian.Uint64(item)) {
			return true
		}
	}

	return false
}

func isWithin(r expr.Range[uint64], value uint64) bool {
	if !r.Low().IsEmpty() {
		if r.Low().Exclusive() {
			if value <= r.Low().Value() {
				return false
			}
		} else {
			if value < r.Low().Value() {
				return false
			}
		}
	}

	if !r.High().IsEmpty() {
		if r.High().Exclusive() {
			if value >= r.High().Value() {
				return false
			}
		} else {
			if value > r.High().Value() {
				return false
			}
		}
	}

	return true
}
