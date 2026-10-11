package concat_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/ehsanranjbar/kvutil/codec/be"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/legacy"
	"github.com/ehsanranjbar/kvutil/legacy/expr"
	"github.com/ehsanranjbar/kvutil/legacy/indexing"
	"github.com/ehsanranjbar/kvutil/legacy/indexing/concat"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	"github.com/ehsanranjbar/kvutil/legacy/schema"
	"github.com/stretchr/testify/require"
)

type Foo struct {
	Str1     string
	Str2     string
	Int      int
	Float    float64
	Struct   Bar
	Pointer  *Bar
	Bytes    []byte
	Array    [3]int
	StrSlice []string
}

type Bar struct {
	Test int
}

func TestIndexer_Index(t *testing.T) {
	tests := []struct {
		name       string
		components []concat.Component
		input      *Foo
		want       []legacy.RawKVPair
		wantErr    bool
	}{
		{
			name:       "Single component",
			components: []concat.Component{concat.NewComponent("Str1")},
			input:      &Foo{Str1: "Alice"},
			want: []legacy.RawKVPair{
				{
					Key: be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize),
				},
			},
		},
		{
			name:       "Multiple components",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int").WithSize(8)},
			input:      &Foo{Str2: "Male", Int: 30},
			want: []legacy.RawKVPair{
				{
					Key: append(be.PadOrTruncRight([]byte("Male"), concat.DefaultMaxComponentSize), lex.EncodeInt64(30)...),
				},
			},
		},
		{
			name:       "Nested struct",
			components: []concat.Component{concat.NewComponent("Struct.Test").WithSize(8)},
			input:      &Foo{Struct: Bar{Test: 42}},
			want:       []legacy.RawKVPair{{Key: lex.EncodeInt64(42)}},
		},
		{
			name:       "Pointer to struct",
			components: []concat.Component{concat.NewComponent("Pointer.Test").WithSize(8)},
			input:      &Foo{Pointer: &Bar{Test: 42}},
			want:       []legacy.RawKVPair{{Key: lex.EncodeInt64(42)}},
		},
		{
			name:       "Bytes",
			components: []concat.Component{concat.NewComponent("Bytes")},
			input:      &Foo{Bytes: []byte{1, 2, 3}},
			want: []legacy.RawKVPair{
				{Key: be.PadOrTruncRight([]byte{1, 2, 3}, concat.DefaultMaxComponentSize)},
			},
		},
		{
			name:       "Array",
			components: []concat.Component{concat.NewComponent("Array").WithSize(8)},
			input:      &Foo{Array: [3]int{1, 2, 3}},
			want: []legacy.RawKVPair{
				{Key: lex.EncodeInt64(1)},
				{Key: lex.EncodeInt64(2)},
				{Key: lex.EncodeInt64(3)},
			},
		},
		{
			name:       "Slice",
			components: []concat.Component{concat.NewComponent("StrSlice")},
			input:      &Foo{StrSlice: []string{"Alice", "Bob"}},
			want: []legacy.RawKVPair{
				{Key: be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)},
				{Key: be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)},
			},
		},
		{
			name:       "Array x Slice",
			components: []concat.Component{concat.NewComponent("Array").WithSize(8), concat.NewComponent("StrSlice")},
			input:      &Foo{Array: [3]int{1, 2, 3}, StrSlice: []string{"Alice", "Bob"}},
			want: []legacy.RawKVPair{
				{Key: append(lex.EncodeInt64(1), be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(1), be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(2), be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(2), be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(3), be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize)...)},
				{Key: append(lex.EncodeInt64(3), be.PadOrTruncRight([]byte("Bob"), concat.DefaultMaxComponentSize)...)},
			},
		},
		{
			name:       "Descending order",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8).Desc()},
			input:      &Foo{Int: 30},
			want:       []legacy.RawKVPair{{Key: lex.Invert(lex.EncodeInt64(30))}},
		},
		{
			name:       "Custom size",
			components: []concat.Component{concat.NewComponent("Str1").WithSize(10)},
			input:      &Foo{Str1: "Alice"},
			want: []legacy.RawKVPair{
				{Key: be.PadOrTruncRight([]byte("Alice"), 10)},
			},
		},
		{
			name:       "Non-existing field",
			components: []concat.Component{concat.NewComponent("NonExisting")},
			input:      &Foo{},
			wantErr:    true,
		},
		{
			name:       "Nil input",
			components: []concat.Component{concat.NewComponent("Str1")},
			input:      nil,
			want:       nil,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := concat.New(
				schema.NewReflectPathExtractor[Foo](false),
				&lex.Encoder{},
				tt.components...,
			)
			require.NoError(t, err)

			got, err := indexer.Index(tt.input, true)
			if tt.wantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			require.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestIndexer_Lookup(t *testing.T) {
	tests := []struct {
		name       string
		components []concat.Component
		args       []any
		want       []indexing.Chunk
		wantErr    bool
	}{
		{
			name:       "Single component equal",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{expr.NewAssigned("Str1", expr.NewExact[any]("Alice"))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
					expr.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
				),
			},
		},
		{
			name:       "Multiple components equal",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int").WithSize(8)},
			args: []any{
				expr.NewAssigned("Str2", expr.NewExact[any]("Male")),
				expr.NewAssigned("Int", expr.NewExact[any](int(30))),
			},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(append(be.PadOrTruncRight([]byte("Male"), concat.DefaultMaxComponentSize), lex.EncodeInt64(30)...), false),
					expr.NewBound(append(be.PadOrTruncRight([]byte("Male"), concat.DefaultMaxComponentSize), lex.EncodeInt64(30)...), false),
				),
			},
		},
		{
			name:       "Range",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8)},
			args: []any{expr.NewAssigned(
				"Int",
				expr.NewRange(
					expr.NewBound[any](int(0), false),
					expr.NewBound[any](int(30), false),
				),
			)},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(lex.EncodeInt64(0), false),
					expr.NewBound(lex.EncodeInt64(30), false),
				),
			},
		},
		{
			name: "Range with open bound",
			components: []concat.Component{
				concat.NewComponent("Int").WithSize(8),
				concat.NewComponent("Float").WithSize(8),
			},
			args: []any{
				expr.NewAssigned("Int", expr.NewRange(expr.NewBound[any](5, true), nil)),
			},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(append(lex.EncodeInt64(6), bytes.Repeat([]byte{0}, 8)...), false),
					expr.NewBound(append(lex.EncodeInt64(math.MaxInt64), bytes.Repeat([]byte{0xff}, 8)...), false),
				),
			},
		},
		{
			name:       "In",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8)},
			args:       []any{expr.NewAssigned("Int", expr.NewSet[any](int(10), int(20), int(30)))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(lex.EncodeInt64(10), false),
					expr.NewBound(lex.EncodeInt64(10), false),
				),
				indexing.NewChunk(
					expr.NewBound(lex.EncodeInt64(20), false),
					expr.NewBound(lex.EncodeInt64(20), false),
				),
				indexing.NewChunk(
					expr.NewBound(lex.EncodeInt64(30), false),
					expr.NewBound(lex.EncodeInt64(30), false),
				),
			},
		},
		{
			name:       "Omitted component",
			components: []concat.Component{concat.NewComponent("Str1"), concat.NewComponent("Str2")},
			args:       []any{expr.NewAssigned("Str1", expr.NewExact[any]("Alice"))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(append(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), make([]byte, concat.DefaultMaxComponentSize)...), false),
					expr.NewBound(append(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), bytes.Repeat([]byte{0xff}, concat.DefaultMaxComponentSize)...), false),
				),
			},
		},
		{
			name:       "Nested struct equal",
			components: []concat.Component{concat.NewComponent("Struct.Test").WithSize(8)},
			args:       []any{expr.NewAssigned("Struct.Test", expr.NewExact[any](int64(42)))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(lex.EncodeInt64(42), false),
					expr.NewBound(lex.EncodeInt64(42), false),
				),
			},
		},
		{
			name:       "Pointer to struct equal",
			components: []concat.Component{concat.NewComponent("Pointer.Test").WithSize(8)},
			args:       []any{expr.NewAssigned("Pointer.Test", expr.NewExact[any](int64(42)))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(lex.EncodeInt64(42), false),
					expr.NewBound(lex.EncodeInt64(42), false),
				),
			},
		},
		{
			name:       "Descending order equal",
			components: []concat.Component{concat.NewComponent("Int").WithSize(8).Desc()},
			args:       []any{expr.NewAssigned("Int", expr.NewExact[any](int64(30)))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(lex.Invert(lex.EncodeInt64(30)), false),
					expr.NewBound(lex.Invert(lex.EncodeInt64(30)), false),
				),
			},
		},
		{
			name:       "Slice equal",
			components: []concat.Component{concat.NewComponent("StrSlice")},
			args:       []any{expr.NewAssigned("StrSlice", expr.NewExact[any]("Alice"))},
			want: []indexing.Chunk{
				indexing.NewChunk(
					expr.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
					expr.NewBound(be.PadOrTruncRight([]byte("Alice"), concat.DefaultMaxComponentSize), false),
				),
			},
		},
		{
			name:       "Non-existing field",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{expr.NewAssigned("NonExisting", expr.NewExact[any]("Value"))},
			wantErr:    true,
		},
		{
			name:       "Too many arguments",
			components: []concat.Component{concat.NewComponent("Str1")},
			args: []any{
				expr.NewAssigned("Str1", expr.NewExact[any]("Alice")),
				expr.NewAssigned("Str2", expr.NewExact[any]("Bob")),
			},
			wantErr: true,
		},
		{
			name:       "Duplicate path",
			components: []concat.Component{concat.NewComponent("Str1"), concat.NewComponent("Str2")},
			args:       []any{expr.NewAssigned("Str1", expr.NewExact[any]("Alice")), expr.NewAssigned("Str1", expr.NewExact[any]("Bob"))},
			wantErr:    true,
		},
		{
			name:       "Invalid argument",
			components: []concat.Component{concat.NewComponent("Str1")},
			args:       []any{"InvalidArg"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := concat.New(
				schema.NewReflectPathExtractor[Foo](false),
				&lex.Encoder{},
				tt.components...,
			)
			require.NoError(t, err)

			got, err := indexer.Lookup(tt.args...)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			gotPartitions, err := iters.Collect(got)
			require.NoError(t, err)
			require.Equal(t, tt.want, gotPartitions)
		})
	}
}

func TestIndexer_SupportedExprs(t *testing.T) {
	tests := []struct {
		name       string
		components []concat.Component
		want       []string
	}{
		{
			name:       "Single component",
			components: []concat.Component{concat.NewComponent("Str1")},
			want:       []string{"queryable(Str1, '=,>,>=,<,<=')"},
		},
		{
			name:       "Multiple components",
			components: []concat.Component{concat.NewComponent("Str2"), concat.NewComponent("Int").WithSize(8)},
			want: []string{
				"queryable(Str2, '=,>,>=,<,<=')",
				"queryable(Str2, '=') and queryable(Int, '=,>,>=,<,<=')",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexer, err := concat.New(
				schema.NewReflectPathExtractor[Foo](false),
				&lex.Encoder{},
				tt.components...,
			)
			require.NoError(t, err)

			for _, l := range indexer.SupportedQueries() {
				require.Contains(t, tt.want, l)
			}
		})
	}
}
