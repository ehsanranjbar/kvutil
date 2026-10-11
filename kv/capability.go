package kv

import "time"

// TTLWriter is an optional capability of a transaction to write keys that expire.
type TTLWriter interface {
	// SetWithTTL sets the value of the key and makes the key expire after the given duration.
	// Neither slice is retained.
	SetWithTTL(key, value []byte, ttl time.Duration) error
}

// RangeDropper is an optional capability of a DB to drop a range of keys quickly.
type RangeDropper interface {
	// DropRange deletes the keys in [start, end). An empty start or end leaves that side of
	// the range unbounded. It is not transactional and must not run alongside writers of the range.
	DropRange(start, end []byte) error
}

// BulkWriter writes without the size limit of a transaction.
// The writes are not atomic: they may become visible in parts.
type BulkWriter interface {
	// Set sets the value of the key. Neither slice is retained.
	Set(key, value []byte) error
	// Delete deletes the key. The slice is not retained.
	Delete(key []byte) error
	// Flush writes whatever is still pending. The writer must not be used afterwards.
	Flush() error
	// Cancel discards whatever is still pending. It is safe to call after Flush.
	Cancel()
}

// BulkOpener is an optional capability of a DB to create bulk writers.
type BulkOpener interface {
	// NewBulkWriter creates a new bulk writer.
	NewBulkWriter() BulkWriter
}

// Unwrapper is implemented by transactions that decorate another transaction.
type Unwrapper interface {
	// Unwrap returns the decorated transaction.
	Unwrap() Tx
}

// SetWithTTL sets a key that expires, or returns ErrUnsupported if the transaction cannot.
func SetWithTTL(tx Tx, key, value []byte, ttl time.Duration) error {
	if w, ok := tx.(TTLWriter); ok {
		return w.SetWithTTL(key, value, ttl)
	}
	return ErrUnsupported
}

// DropRange drops a range of keys, or returns ErrUnsupported if the DB cannot.
func DropRange(db DB, start, end []byte) error {
	if d, ok := db.(RangeDropper); ok {
		return d.DropRange(start, end)
	}
	return ErrUnsupported
}

// NewBulkWriter creates a bulk writer, or returns ErrUnsupported if the DB cannot.
func NewBulkWriter(db DB) (BulkWriter, error) {
	if b, ok := db.(BulkOpener); ok {
		return b.NewBulkWriter(), nil
	}
	return nil, ErrUnsupported
}
