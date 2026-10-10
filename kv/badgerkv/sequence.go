package badgerkv

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/internal/constraints"
)

// SequenceID returns an id function that takes the ids of new records from a badger sequence.
func SequenceID[I constraints.Integer, T any](seq *badger.Sequence) func(*T) (I, error) {
	return func(*T) (I, error) {
		id, err := seq.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to generate id: %w", err)
		}
		// badger sequences start from 0, which is the id of a record that has none yet.
		return I(id + 1), nil
	}
}
