package record_test

import (
	"testing"

	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/stretchr/testify/require"
)

func TestDoc(t *testing.T) {
	t.Run("MarshalWithId", func(t *testing.T) {
		doc := record.Doc[int64]{"_id": int64(1), "a": "b", "n": int64(2)}

		bz, err := doc.MarshalBinary()
		require.NoError(t, err)

		var got record.Doc[int64]
		err = got.UnmarshalBinary(bz)
		require.NoError(t, err)
		// The id is the key of the record, so it is not part of the serialized value.
		require.Equal(t, record.Doc[int64]{"a": "b", "n": int64(2)}, got)
	})

	t.Run("MarshalWithoutId", func(t *testing.T) {
		doc := record.Doc[int64]{"a": "b"}

		bz, err := doc.MarshalBinary()
		require.NoError(t, err)

		var got record.Doc[int64]
		err = got.UnmarshalBinary(bz)
		require.NoError(t, err)
		require.Equal(t, doc, got)
	})

	t.Run("GetIdWithoutId", func(t *testing.T) {
		doc := record.Doc[int64]{"a": "b"}
		require.Zero(t, doc.GetId())

		doc.SetId(7)
		require.Equal(t, int64(7), doc.GetId())
	})

	t.Run("NilDocument", func(t *testing.T) {
		var doc record.Doc[int64]
		require.Zero(t, doc.GetId())

		bz, err := doc.MarshalBinary()
		require.NoError(t, err)

		// What is read can have its id set.
		var got record.Doc[int64]
		require.NoError(t, got.UnmarshalBinary(bz))
		require.NotPanics(t, func() { got.SetId(1) })
		require.Equal(t, int64(1), got.GetId())
	})
}
