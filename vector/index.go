package vector

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	"github.com/ehsanranjbar/kvutil/migrate"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
	"github.com/ehsanranjbar/kvutil/upkeep"
)

// The keys of an index, under the prefix of its extension. All the integers are 4 bytes,
// big-endian.
//
//	'p' <partition> <primary key>  the vector of a record, in the partition it is in.
//	'q' <partition> <primary key>  the code of the vector of a record in a partition, if the
//	                               index is quantized. The unplaced area has none.
//	'k' <primary key>              the partition that a record is in, if it is in one.
//	'c' <partition>                the centroid of a partition.
//	'n' <partition>                the number of vectors of a partition, as an upper bound.
//	's'                            the state: a nonce, and the next partition id.
//
// The partition 0 is the unplaced area, which is where a record is written. It is in no
// partition until the index is built again or an upkeep.Runner places it, and every search reads
// it in full.
//
// The count of a partition is written by the build and the runner only. A writer that
// removes a vector from a partition does not touch it, so it counts vectors that are gone until
// the partition is counted again. A partition that has no count has an unknown one.
const (
	vectorsKey   = 'p'
	codesKey     = 'q'
	placesKey    = 'k'
	centroidsKey = 'c'
	countsKey    = 'n'
	stateKey     = 's'
)

// unplaced is the partition of the vectors that are in none.
const unplaced uint32 = 0

// encoding identifies how a vector is stored and where, which the index records of itself.
const encoding = "f32le/p1"

// options are what the options of an index set.
type options struct {
	version     string
	partitioned bool
	size        int
	probes      int
	quantized   bool
}

// Option is an option of an index.
type Option func(*options)

// Versioned gives the index a version, which identifies what the function that returns the
// vectors computes, like the model that the embeddings come from. The index is built again
// (by migrate) when the version changes. Several versions are joined with ";".
func Versioned(version string) Option {
	return func(o *options) {
		if o.version == "" {
			o.version = version
		} else {
			o.version += ";" + version
		}
	}
}

// Partitioned makes the index keep its vectors in partitions of about size vectors at most,
// which a search reads the nearest ones of instead of all the vectors. size 0 is a default
// that depends on the dimensions; it panics if size is negative.
//
// The partitions are formed when the index is built (see Rebuild), or by an upkeep.Runner once
// the index has more vectors than a partition. The records that are written after that are found
// by every search, and the runner places them in the partitions, splits a partition that has
// grown and dissolves one that has few vectors left. A partition is split and dissolved in one
// transaction, so the vectors of a partition must fit in a transaction of the engine.
func Partitioned(size int) Option {
	if size < 0 {
		panic(fmt.Sprintf("vector: the size of a partition must not be negative, not %d", size))
	}
	return func(o *options) {
		o.partitioned, o.size = true, size
	}
}

// Probes sets how many partitions a search of the index reads when the search does not say.
// 0, the default, is the square root of the number of partitions, rounded up. It panics if n
// is negative.
func Probes(n int) Option {
	if n < 0 {
		panic(fmt.Sprintf("vector: the number of probes must not be negative, not %d", n))
	}
	return func(o *options) {
		o.probes = n
	}
}

// Quantized makes a partitioned index keep a code of 1 bit per dimension (rounded up to a power of
// two) of every vector in a partition, which a search reads instead of the vector, and re-ranks
// the best candidates by their vectors (see Search.Rescore). The codes are a fraction of the size
// of the vectors, so a search reads a fraction of the bytes, and the distances it returns are
// still exact. What is approximate is which records are found, as it is with partitions.
//
// The codes are written wherever a vector is placed in a partition, and are made again when the
// index is built, so an index that is made quantized or not is built again by migrate. It panics
// unless the index is also Partitioned, whatever the order of the options is.
func Quantized() Option {
	return func(o *options) {
		o.quantized = true
	}
}

// defaultPartitionSize returns the size of a partition that is used when none is given: the
// vectors of a partition are about a megabyte, within limits.
func defaultPartitionSize(dims int) int {
	return min(1024, max(64, (1<<20)/(4*dims)))
}

// Index is a vector index of a store. It is kept up to date as an extension of the store,
// and is instantiated for a transaction to search in.
type Index[K, V any] struct {
	name    string
	store   extstore.Extensible[K, V]
	dims    int
	metric  Metric
	get     func(*V) []float32
	version string
	// size is the size of a partition, which is 0 if the index is not partitioned, and probes is
	// the number of partitions that a search reads, if it is not 0.
	size   int
	probes int
	// rot is the rotation that the codes are of, which is nil if the index is not quantized.
	rot *rotation
	// upkeep makes the passes of the runners of a program over the index take turns, and
	// recounted is the last partition that one counted, where the next goes on from.
	upkeep    sync.Mutex
	recounted atomic.Uint32
	// prefix is the prefix of the extension, which the keys of the index are under. It is set when
	// the index is added to its store.
	prefix []byte
	// parts is the partitions as they were last read, which are read again if the state changed.
	parts atomic.Pointer[partitions]
}

// On defines a new vector index on a store. get returns the vector of a record; a record for
// which it returns no vector (nil or empty) is not in the index. It panics if dims is not
// positive, the metric is not a defined one, or get is nil.
func On[K, V any](
	store extstore.Extensible[K, V],
	name string,
	dims int,
	metric Metric,
	get func(*V) []float32,
	opts ...Option,
) *Index[K, V] {
	// These are mistakes in the definition, which is made in code when the program starts.
	if dims <= 0 {
		panic(fmt.Sprintf("vector: the index %q must have a positive number of dimensions, not %d", name, dims))
	}
	if !metric.defined() {
		panic(fmt.Sprintf("vector: the index %q has %v, which is not a defined metric", name, metric))
	}
	if get == nil {
		panic(fmt.Sprintf("vector: the index %q has no function to get the vector of a record", name))
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.quantized && !o.partitioned {
		panic(fmt.Sprintf("vector: the index %q is quantized, which needs it to be partitioned", name))
	}
	x := &Index[K, V]{
		name:    name,
		store:   store,
		dims:    dims,
		metric:  metric,
		get:     get,
		version: o.version,
		probes:  o.probes,
	}
	if o.partitioned {
		x.size = o.size
		if x.size == 0 {
			x.size = defaultPartitionSize(dims)
		}
	}
	if o.quantized {
		x.rot = newRotation(dims)
	}
	store.AddExtension(name, extension[K, V]{x: x})
	return x
}

// Name returns the name of the index.
func (x *Index[K, V]) Name() string {
	return x.name
}

// Dims returns the number of components of the vectors of the index.
func (x *Index[K, V]) Dims() int {
	return x.dims
}

// Metric returns how the index measures the distance between vectors.
func (x *Index[K, V]) Metric() Metric {
	return x.metric
}

// PartitionSize returns the size of the partitions of the index, or 0 if it is not partitioned.
func (x *Index[K, V]) PartitionSize() int {
	return x.size
}

// Quantized reports whether the index keeps codes of its vectors in partitions.
func (x *Index[K, V]) Quantized() bool {
	return x.rot != nil
}

// Prefix returns the prefix that the index keeps its entries under, which is given to it when it
// is added to its store. The slice must not be modified.
func (x *Index[K, V]) Prefix() []byte {
	return x.prefix
}

// Instantiate creates the instance of the index for a transaction.
func (x *Index[K, V]) Instantiate(tx kv.Tx) *Instance[K, V] {
	return &Instance[K, V]{
		x:    x,
		tx:   tx,
		data: prefix.New(tx, x.prefix),
	}
}

// attributes returns what the stored entries of the index depend on.
func (x *Index[K, V]) attributes() map[string]string {
	attrs := map[string]string{
		"dims":     strconv.Itoa(x.dims),
		"metric":   x.metric.String(),
		"encoding": encoding,
	}
	// Only an index that has a version has the attribute, so that the others are what they were.
	if x.version != "" {
		attrs["version"] = x.version
	}
	// The number of probes is no attribute, as it changes no stored data.
	if x.size > 0 {
		attrs["partitions"] = strconv.Itoa(x.size)
	}
	// The codes are stored data, so an index that turns them on or off is built again.
	if x.rot != nil {
		attrs["quantization"] = quantization
	}
	return attrs
}

// vectorKey returns the key of the vector of a record in a partition, which the keys of the
// records in the partition follow in the order of the store.
func vectorKey(pid uint32, rawKey []byte) []byte {
	k := make([]byte, 0, 5+len(rawKey))
	k = append(k, vectorsKey)
	k = binary.BigEndian.AppendUint32(k, pid)
	return append(k, rawKey...)
}

// codeKey returns the key of the code of the vector of a record in a partition. It is laid out as
// the key of the vector is, so splitVectorKey splits it too.
func codeKey(pid uint32, rawKey []byte) []byte {
	k := vectorKey(pid, rawKey)
	k[0] = codesKey
	return k
}

// placeKey returns the key of the partition that a record is in.
func placeKey(rawKey []byte) []byte {
	return append([]byte{placesKey}, rawKey...)
}

// centroidKey returns the key of the centroid of a partition.
func centroidKey(pid uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{centroidsKey}, pid)
}

// countKey returns the key of the number of vectors of a partition.
func countKey(pid uint32) []byte {
	return binary.BigEndian.AppendUint32([]byte{countsKey}, pid)
}

// splitVectorKey returns the primary key of a record and the partition of a vector from its key.
func splitVectorKey(key []byte) (rawKey []byte, pid uint32, err error) {
	if len(key) < 5 {
		return nil, 0, fmt.Errorf("%w: the key %x is too short", ErrCorrupt, key)
	}
	return key[5:], binary.BigEndian.Uint32(key[1:5]), nil
}

// readPlace returns the partition that a record is in, and whether it is in one.
func readPlace(data *prefix.Tx, rawKey []byte) (pid uint32, placed bool, err error) {
	b, err := data.Get(placeKey(rawKey))
	if errors.Is(err, kv.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(b) != 4 {
		return 0, false, fmt.Errorf("%w: the place of the key %x has %d bytes", ErrCorrupt, rawKey, len(b))
	}
	return binary.BigEndian.Uint32(b), true, nil
}

// extension is the extension that keeps an index up to date.
type extension[K, V any] struct {
	x *Index[K, V]
}

// Jobs implements the upkeep.Provider interface, which is how an upkeep.Runner finds the indexes
// of a store by going through its extensions. An index without partitions has nothing to keep.
func (m extension[K, V]) Jobs() []upkeep.Job {
	if m.x.size == 0 {
		return nil
	}
	return []upkeep.Job{job[K, V]{x: m.x}}
}

// Keep implements the extstore.Keeper interface.
func (m extension[K, V]) Keep(prefix []byte) migrate.Part {
	x := m.x
	x.prefix = bytes.Clone(prefix)
	return migrate.Part{
		Kind:       "vector",
		Attributes: x.attributes,
		// An index is derived from the records, so it is built for the ones that are there
		// when it is defined, and again when it or the records have changed.
		Rebuild: func(ctx context.Context, db kv.DB) error {
			return Rebuild(ctx, db, x)
		},
	}
}

// Instantiate implements the extstore.Extension interface.
func (m extension[K, V]) Instantiate(tx kv.Tx) extstore.ExtensionInstance[K, V] {
	return &maintenance[K, V]{
		x:    m.x,
		data: prefix.New(tx, m.x.prefix),
	}
}

type maintenance[K, V any] struct {
	x    *Index[K, V]
	data *prefix.Tx
}

// OnSet implements the extstore.ExtensionInstance interface.
//
// A vector is always written to the unplaced area, which every search reads, so that writers
// share no key and the record is found at once. Which partition it belongs to is for the build
// and the runner to say.
func (m *maintenance[K, V]) OnSet(_ K, rawKey []byte, old, new *V, _ ...any) error {
	hadVector := old != nil && len(m.x.get(old)) > 0

	vec := m.x.get(new)
	if len(vec) == 0 {
		// The record has no vector now, so the entry of the one it had is to go.
		if hadVector {
			return m.remove(rawKey)
		}
		return nil
	}

	// A vector that is not valid fails the write even if it is the one the record had, which
	// can only be if the function that returns the vectors has changed.
	stored, err := m.x.prepare(vec)
	if err != nil {
		return fmt.Errorf("failed to index the record: %w", err)
	}
	if old != nil && slices.Equal(m.x.get(old), vec) {
		return nil
	}

	// The vector that the record had is in a partition, if the index has them. Only then is
	// there something to read.
	if hadVector && m.x.size > 0 {
		if err := m.unplace(rawKey); err != nil {
			return err
		}
	}
	return m.data.Set(vectorKey(unplaced, rawKey), encode(stored))
}

// OnDelete implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V]) OnDelete(_ K, rawKey []byte, old *V) error {
	if len(m.x.get(old)) == 0 {
		return nil
	}
	return m.remove(rawKey)
}

// remove deletes the vector of a record, wherever it is.
func (m *maintenance[K, V]) remove(rawKey []byte) error {
	if m.x.size > 0 {
		if err := m.unplace(rawKey); err != nil {
			return err
		}
	}
	return m.data.Delete(vectorKey(unplaced, rawKey))
}

// unplace deletes the vector of a record from the partition it is in, if it is in one, and the
// code of the vector. The code is deleted without being read, as it is there if the index is
// quantized and the vector is in a partition.
func (m *maintenance[K, V]) unplace(rawKey []byte) error {
	pid, placed, err := readPlace(m.data, rawKey)
	if err != nil || !placed {
		return err
	}
	if err := m.data.Delete(vectorKey(pid, rawKey)); err != nil {
		return err
	}
	if m.x.rot != nil {
		if err := m.data.Delete(codeKey(pid, rawKey)); err != nil {
			return err
		}
	}
	return m.data.Delete(placeKey(rawKey))
}

// batchSize returns the number of records that Rebuild indexes in a transaction. It is bounded
// by the size of the vectors too, so that a transaction stays small whatever the dimensions.
func (x *Index[K, V]) batchSize() int {
	return min(500, max(16, (1<<20)/(4*x.dims)))
}
