package testutil

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// PrepareTxn creates an in-memory BadgerDB and a transaction for testing.
func PrepareTxn(t testing.TB, update bool) *badger.Txn {
	opt := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	txn := db.NewTransaction(update)
	t.Cleanup(func() {
		txn.Discard()
	})
	return txn
}

// Dump reads all key-value pairs from a transaction and returns them as a map.
func Dump(txn *badger.Txn) map[string]string {
	result := make(map[string]string)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := string(item.Key())
		v, err := item.ValueCopy(nil)
		if err != nil {
			panic(err)
		}
		result[k] = string(v)
	}
	return result
}
