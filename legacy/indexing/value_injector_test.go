package indexing_test

import (
	"encoding/json"
	"testing"

	"github.com/ehsanranjbar/kvutil/legacy"
	"github.com/ehsanranjbar/kvutil/legacy/indexing"
	"github.com/ehsanranjbar/kvutil/legacy/testutil/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestValueInjector_Index(t *testing.T) {
	tests := []struct {
		name     string
		paths    []string
		input    *struct{}
		set      bool
		expected []byte
	}{
		{
			name:     "Single path",
			paths:    []string{"A"},
			input:    &struct{}{},
			set:      true,
			expected: []byte(`{"A":"test"}`),
		},
		{
			name:     "Multiple paths",
			paths:    []string{"A", "B.C"},
			input:    &struct{}{},
			set:      true,
			expected: []byte(`{"A":"test","B.C":123}`),
		},
		{
			name:     "Nil",
			paths:    []string{"A"},
			input:    nil,
			set:      true,
			expected: []byte(`{}`),
		},
		{
			name:     "Not set",
			paths:    []string{"A"},
			input:    &struct{}{},
			set:      false,
			expected: []byte(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := mocks.NewMockIndexer[struct{}](t)
			idx.
				EXPECT().
				Index(mock.Anything, mock.Anything).
				Return([]legacy.RawKVPair{
					{Key: []byte("key1")},
					{Key: []byte("key2")},
				}, nil).
				Maybe()
			pe := mocks.NewMockPathExtractor[struct{}](t)
			pe.
				EXPECT().
				ExtractPath(mock.Anything, "A").
				Return("test", nil).
				Maybe()
			pe.
				EXPECT().
				ExtractPath(mock.Anything, "B.C").
				Return(123, nil).
				Maybe()
			vr := indexing.NewMapValueRetriever(
				pe,
				json.Marshal,
				tt.paths...,
			)
			vi := indexing.NewValueInjector[struct{}](idx, vr)

			got, err := vi.Index(tt.input, tt.set)
			require.NoError(t, err)
			for _, kv := range got {
				require.Equal(t, string(tt.expected), string(kv.Value))
			}
		})
	}
}
