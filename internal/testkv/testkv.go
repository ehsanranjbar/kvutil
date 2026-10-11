// Package testkv provides the engines that the tests of the layers above kv run on.
package testkv

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
)

// NewMem returns a new in-memory DB.
func NewMem(testing.TB) kv.DB {
	return memkv.New()
}

// NewBadger returns a new badger DB that is kept in memory and is closed when the test ends.
func NewBadger(t testing.TB) kv.DB {
	t.Helper()

	db, err := badgerkv.Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Each runs the test on a new DB of every engine.
func Each(t *testing.T, test func(t *testing.T, db kv.DB)) {
	t.Run("Mem", func(t *testing.T) { test(t, NewMem(t)) })
	t.Run("Badger", func(t *testing.T) { test(t, NewBadger(t)) })
}
