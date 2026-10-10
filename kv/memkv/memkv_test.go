package memkv_test

import (
	"testing"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/kvtest"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/stretchr/testify/require"
)

func TestConformance(t *testing.T) {
	kvtest.Run(t, func(*testing.T) kv.DB { return memkv.New() }, kvtest.Options{})
}

func TestNoCapabilities(t *testing.T) {
	db := memkv.New()

	err := db.Update(func(tx kv.Tx) error {
		return kv.SetWithTTL(tx, []byte("a"), []byte("1"), time.Hour)
	})
	require.ErrorIs(t, err, kv.ErrUnsupported)

	require.ErrorIs(t, kv.DropRange(db, nil, nil), kv.ErrUnsupported)

	_, err = kv.NewBulkWriter(db)
	require.ErrorIs(t, err, kv.ErrUnsupported)
}
