package iters

import "github.com/ehsanranjbar/kvutil/legacy"

// Collect collects all the items from the iterator and returns them as a slice.
func Collect[K, V any](it legacy.Iterator[K, V]) ([]V, error) {
	var items []V
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := it.Value()
		if err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	return items, nil
}

// CollectKeys collects all the keys from the iterator and returns them as a slice.
func CollectKeys[K, V any](it legacy.Iterator[K, V]) ([]K, error) {
	var keys []K
	for it.Rewind(); it.Valid(); it.Next() {
		k, err := it.Key()
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, nil
}
