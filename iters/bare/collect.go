package bare

import "github.com/ehsanranjbar/kvutil"

// CollectKeys collects all the keys from the iterator and returns them as a slice.
func CollectKeys(it kvutil.BadgerIterator) [][]byte {
	var keys [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	return keys
}
