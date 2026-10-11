// Package agg provides stored aggregations of the records of a store, like a count or a sum
// for each group of records, which can also be kept for each day, week or month.
//
// An aggregation is kept in two stages, so that the transactions that write records do not
// conflict with each other on it. A transaction that writes a record only notes the change,
// under a key of its own. A Collector, which runs in the background, turns what was noted
// into the stored states. Reading an aggregation takes what is not collected yet into account,
// so it is exact whether the collector has run or not.
//
// An aggregation is for reporting. A transaction that reads one and writes according to what
// it read is not protected from the records that other transactions add at the same time,
// so an aggregation cannot be used to enforce a limit.
package agg

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/ehsanranjbar/kvutil/catalog"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/prefix"
	extstore "github.com/ehsanranjbar/kvutil/store/ext"
)

var (
	// ErrAppendOnly is returned when a record that an append-only aggregation has counted is
	// changed or deleted.
	ErrAppendOnly = errors.New("agg: a record of an append-only aggregation cannot be changed or deleted")
	// ErrNoBucket is returned when an aggregation is read by a bucket that it does not have.
	ErrNoBucket = errors.New("agg: the aggregation does not have the bucket")
)

// Extract says what a record contributes to an aggregation: a value, in a group.
// A record that is not ok contributes nothing. The type of the groups must have a key codec:
// it can be a number, a string or a codec.Pair of them, among others. An aggregation that
// does not group its records has NoGroup as the type of its groups.
type Extract[V, G, X any] func(v *V) (group G, x X, ok bool)

// NoGroup is the type of the groups of an aggregation that has one state for all the records.
type NoGroup struct{}

// noGroupKey is the key codec of NoGroup, which takes no room in a key.
type noGroupKey struct{}

func (noGroupKey) Name() string                                 { return "none" }
func (noGroupKey) Encode(NoGroup) ([]byte, error)               { return nil, nil }
func (noGroupKey) Decode([]byte) (NoGroup, error)               { return NoGroup{}, nil }
func (noGroupKey) Append(dst []byte, _ NoGroup) ([]byte, error) { return dst, nil }
func (noGroupKey) Read([]byte) (NoGroup, int, error)            { return NoGroup{}, 0, nil }

// Point is the state of an aggregation for the span of a bucket that starts at a time.
type Point[S any] struct {
	Start time.Time
	State S
}

// Descriptor describes an aggregation to whoever plans queries on it.
type Descriptor struct {
	// Kind is how the aggregation is kept: "delta" or "recompute".
	Kind string
	// Function names what is computed, like "count" or "sum". It is empty for a custom function.
	Function string
	// Value is the path of the field that is aggregated.
	Value string
	// GroupBy are the paths of the fields that the records are grouped by.
	GroupBy []string
	// Time is the path of the field that says which span of a bucket a record is in.
	Time string
	// Buckets are the names of the buckets that the aggregation is kept for.
	Buckets []string
	// Merged is set if only the first bucket is stored, and the others are put together from it.
	Merged bool
	// Encoding identifies how the stored values are encoded.
	Encoding string
}

func (d Descriptor) attributes() map[string]string {
	return map[string]string{
		"kind":     d.Kind,
		"buckets":  strings.Join(d.Buckets, ","),
		"merged":   fmt.Sprint(d.Merged),
		"encoding": d.Encoding,
	}
}

type options struct {
	timeOf     any // func(*V) time.Time
	buckets    []Bucket
	merge      any // func(a, b S) S
	gCodec     any // codec.KeyCodec[G]
	xCodec     any // codec.ValueCodec[X]
	sCodec     any // codec.ValueCodec[S]
	appendOnly bool
	desc       Descriptor
}

// Option is an option of an aggregation.
type Option func(*options)

// ByTime keeps the aggregation for the spans of the given buckets, by the time that the
// function returns for a record. Without it an aggregation is kept for all of time only.
func ByTime[V any](timeOf func(*V) time.Time, buckets ...Bucket) Option {
	return func(o *options) {
		o.timeOf = timeOf
		o.buckets = buckets
	}
}

// WithMerge gives a recomputed aggregation the function that puts two states together.
// With it only the first of the buckets is stored, and the states of the other buckets are put
// together from its states when they are read, so the spans of the first bucket must be inside
// the spans of the others.
func WithMerge[S any](merge func(a, b S) S) Option {
	return func(o *options) {
		o.merge = merge
	}
}

// AppendOnly declares that the records of a delta aggregation are never changed or deleted,
// which lets it be without a function to remove a contribution. Changing or deleting a record
// that it has counted fails with ErrAppendOnly.
func AppendOnly() Option {
	return func(o *options) {
		o.appendOnly = true
	}
}

// WithGroupCodec sets the key codec of the groups, instead of the one that their type has by default.
func WithGroupCodec[G any](c codec.KeyCodec[G]) Option {
	return func(o *options) {
		o.gCodec = c
	}
}

// WithContributionCodec sets the codec of the values that the records contribute.
func WithContributionCodec[X any](c codec.ValueCodec[X]) Option {
	return func(o *options) {
		o.xCodec = c
	}
}

// WithStateCodec sets the codec of the states of the aggregation.
func WithStateCodec[S any](c codec.ValueCodec[S]) Option {
	return func(o *options) {
		o.sCodec = c
	}
}

// Describe names what the aggregation computes for its descriptor: the function, the path of
// the field that is aggregated, the path of the field that the time is taken from and the paths
// of the fields that the records are grouped by.
func Describe(function, value, timeField string, groupBy ...string) Option {
	return func(o *options) {
		o.desc.Function, o.desc.Value, o.desc.Time, o.desc.GroupBy = function, value, timeField, groupBy
	}
}

// option returns an option that was given as a value of any type as its own type.
// An option of another type is a mistake in the definition, which panics.
func option[T any](name, what string, v any) (t T) {
	if v == nil {
		return t
	}
	t, ok := v.(T)
	if !ok {
		panic(fmt.Sprintf("agg %q: %s is a %T, but must be a %s", name, what, v, reflect.TypeFor[T]()))
	}
	return t
}

// contribution is what a record contributes to an aggregation.
type contribution[X any] struct {
	group []byte // the encoded group
	time  time.Time
	x     X
	xbz   []byte
}

func (c *contribution[X]) equal(other *contribution[X]) bool {
	if c == nil || other == nil {
		return c == other
	}
	return bytes.Equal(c.group, other.group) && c.time.Equal(other.time) && bytes.Equal(c.xbz, other.xbz)
}

// The first byte of a key says what the key is of.
const (
	stateKeys        = 's'
	tempKeys         = 't'
	contributionKeys = 'i'
	markerKeys       = 'd'
	failedKeys       = 'e'
)

var timeCodec = codec.TimeKey()

// timeSize is the size of an encoded time.
const timeSize = 12

func appendTime(dst []byte, t time.Time) []byte {
	dst, _ = timeCodec.Append(dst, t)
	return dst
}

// core is what the two kinds of aggregations have in common. It has the groups as they are
// encoded in the keys: in the form of their key codec that marks where it ends.
type core[X, S any] struct {
	name      string
	ns        *catalog.Namespace
	buckets   []Bucket
	skipGroup func(bz []byte) (n int, err error) // the size of the group at the front of the bytes
	xCodec    codec.ValueCodec[X]
	sCodec    codec.ValueCodec[S]
}

func (c *core[X, S]) bucket(id byte) (Bucket, bool) {
	for _, b := range c.buckets {
		if b.ID() == id {
			return b, true
		}
	}
	return nil, false
}

// groupPrefix returns the beginning of the keys of a kind for a group.
func groupPrefix(kind byte, group []byte) []byte {
	return append([]byte{kind}, group...)
}

// bucketPrefix returns the beginning of the keys of a kind for a bucket and a group.
func bucketPrefix(kind byte, b Bucket, group []byte) []byte {
	return append([]byte{kind, b.ID()}, group...)
}

// timeRange returns the range of the keys that begin with the prefix and continue with a time
// in [lo, hi), or with any time if all is set.
func timeRange(prefix []byte, lo, hi time.Time, all bool) (start, end []byte) {
	if all {
		if end = kv.PrefixEnd(prefix); end == nil {
			// Nothing sorts after the prefix, so the keys go on to the end of the kind.
			end = []byte{prefix[0] + 1}
		}
		return prefix, end
	}
	return appendTime(bytes.Clone(prefix), lo), appendTime(bytes.Clone(prefix), hi)
}

// splitBucketKey splits a key of a state or of a marker.
func (c *core[X, S]) splitBucketKey(key []byte) (id byte, group []byte, start time.Time, err error) {
	if len(key) < 2 {
		return 0, nil, start, fmt.Errorf("agg %q: key of %d bytes", c.name, len(key))
	}
	n, err := c.skipGroup(key[2:])
	if err != nil {
		return 0, nil, start, err
	}
	start, err = timeCodec.Decode(key[2+n:])
	return key[1], key[2 : 2+n], start, err
}

// kind is how an aggregation is kept.
type kind[X, S any] interface {
	// change notes that a record contributes something else than it did. Either may be nil.
	change(tx *prefix.Tx, rawKey []byte, old, new *contribution[X]) error
	// series returns the states of the spans of a bucket that start in [lo, hi), or the state of
	// the only span if all is set. Only the spans that have records in them are returned.
	// What is not collected yet is left out if collectedOnly is set.
	series(tx *prefix.Tx, b Bucket, group []byte, lo, hi time.Time, all, collectedOnly bool) ([]Point[S], error)
	// collect turns a batch of what was noted after the cursor into stored states.
	// It returns the cursor to go on from, or nil when there is nothing after it.
	collect(db kv.DB, cursor []byte, batch int, report func(error)) (next []byte, err error)
}

// Agg is a stored aggregation of the records of a store. It is kept as an extension of the
// store, and is instantiated for a transaction to be read.
type Agg[K, V, G, X, S any] struct {
	*core[X, S]
	store   extstore.Extensible[K, V]
	extract Extract[V, G, X]
	gCodec  codec.KeyCodec[G]
	timeOf  func(*V) time.Time
	kind    kind[X, S]
	desc    Descriptor

	// What is set when the store gives the aggregation its namespace.
	opts options
}

// define creates an aggregation and adds it to the store. The kind is made by the function
// once the aggregation has its namespace and its codecs.
func define[K, V, G, X, S any](
	store extstore.Extensible[K, V],
	name, kindName string,
	extract Extract[V, G, X],
	opts []Option,
	newKind func(c *core[X, S], o options) kind[X, S],
) *Agg[K, V, G, X, S] {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if len(o.buckets) == 0 {
		o.buckets = []Bucket{AllTime}
	}

	ids := map[byte]string{}
	names := make([]string, 0, len(o.buckets))
	for _, b := range o.buckets {
		if other, ok := ids[b.ID()]; ok {
			panic(fmt.Sprintf("agg %q: the buckets %s and %s cannot be used together", name, other, b.Name()))
		}
		ids[b.ID()] = b.Name()
		names = append(names, b.Name())
	}

	gCodec := option[codec.KeyCodec[G]](name, "the group codec", o.gCodec)
	if gCodec == nil {
		if none, ok := any(noGroupKey{}).(codec.KeyCodec[G]); ok {
			gCodec = none
		} else {
			c, err := codec.KeyFor[G]()
			if err != nil {
				// A mistake in the definition, which is made in code when the program starts.
				panic(fmt.Sprintf("agg %q: the groups: %v; give the aggregation one with WithGroupCodec", name, err))
			}
			gCodec = c
		}
	}

	a := &Agg[K, V, G, X, S]{
		core: &core[X, S]{
			name:    name,
			buckets: o.buckets,
			skipGroup: func(bz []byte) (int, error) {
				_, n, err := gCodec.Read(bz)
				return n, err
			},
		},
		store:   store,
		extract: extract,
		gCodec:  gCodec,
		timeOf:  option[func(*V) time.Time](name, "the function of ByTime", o.timeOf),
		opts:    o,
	}
	a.desc = o.desc
	a.desc.Kind = kindName
	a.desc.Buckets = names
	a.desc.Merged = o.merge != nil

	store.AddExtension(name, maintainer[K, V, G, X, S]{a: a, newKind: newKind})
	return a
}

// Name returns the name of the aggregation.
func (a *Agg[K, V, G, X, S]) Name() string {
	return a.name
}

// Descriptor returns the descriptor of the aggregation.
func (a *Agg[K, V, G, X, S]) Descriptor() Descriptor {
	return a.desc
}

// Namespace returns the namespace that the aggregation keeps its data in.
func (a *Agg[K, V, G, X, S]) Namespace() *catalog.Namespace {
	return a.ns
}

// Buckets returns the buckets that the aggregation is kept for.
func (a *Agg[K, V, G, X, S]) Buckets() []Bucket {
	return slices.Clone(a.buckets)
}

// contributionOf returns what a record contributes, or nil if it contributes nothing.
func (a *Agg[K, V, G, X, S]) contributionOf(v *V) (*contribution[X], error) {
	if v == nil {
		return nil, nil
	}
	group, x, ok := a.extract(v)
	if !ok {
		return nil, nil
	}

	gbz, err := a.encodeGroup(group)
	if err != nil {
		return nil, err
	}
	xbz, err := a.xCodec.Encode(x)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the contribution: %w", err)
	}
	c := &contribution[X]{group: gbz, x: x, xbz: xbz}
	if a.timeOf != nil {
		c.time = a.timeOf(v)
	}
	return c, nil
}

// encodeGroup encodes a group as it is in the keys.
func (a *Agg[K, V, G, X, S]) encodeGroup(group G) ([]byte, error) {
	gbz, err := a.gCodec.Append(nil, group)
	if err != nil {
		return nil, fmt.Errorf("failed to encode the group: %w", err)
	}
	return gbz, nil
}

// collectName implements the collectable interface.
func (a *Agg[K, V, G, X, S]) collectName() string {
	return a.ns.Path()
}

// collectBatch implements the collectable interface.
func (a *Agg[K, V, G, X, S]) collectBatch(db kv.DB, cursor []byte, batch int, report func(error)) ([]byte, error) {
	return a.kind.collect(db, cursor, batch, report)
}

// maintainer is the extension that notes the changes of the records for an aggregation.
type maintainer[K, V, G, X, S any] struct {
	a       *Agg[K, V, G, X, S]
	newKind func(c *core[X, S], o options) kind[X, S]
}

// UseNamespace implements the extstore.NamespaceUser interface.
func (m maintainer[K, V, G, X, S]) UseNamespace(ns *catalog.Namespace) {
	a := m.a
	a.ns = ns

	format := ns.Schema().Format()
	if a.xCodec = option[codec.ValueCodec[X]](a.name, "the contribution codec", a.opts.xCodec); a.xCodec == nil {
		a.xCodec = codec.ValueFor[X](format)
	}
	if a.sCodec = option[codec.ValueCodec[S]](a.name, "the state codec", a.opts.sCodec); a.sCodec == nil {
		a.sCodec = codec.ValueFor[S](format)
	}
	a.desc.Encoding = a.gCodec.Name() + "," + a.xCodec.Name() + "," + a.sCodec.Name()
	a.kind = m.newKind(a.core, a.opts)

	ns.Define(catalog.Definition{
		Kind:       "agg",
		Keys:       true,
		KeyType:    reflect.TypeFor[K]().String(),
		ValueType:  reflect.TypeFor[V]().String(),
		Attributes: a.desc.attributes,
		Descriptor: a.desc,
		Object:     a,
	})
}

// Instantiate implements the extstore.Extension interface.
func (m maintainer[K, V, G, X, S]) Instantiate(tx kv.Tx) extstore.ExtensionInstance[K, V] {
	return &maintenance[K, V, G, X, S]{a: m.a, tx: m.a.ns.Open(tx)}
}

type maintenance[K, V, G, X, S any] struct {
	a  *Agg[K, V, G, X, S]
	tx *prefix.Tx
}

// OnSet implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V, G, X, S]) OnSet(_ K, rawKey []byte, old, new *V, _ ...any) error {
	return m.change(rawKey, old, new)
}

// OnDelete implements the extstore.ExtensionInstance interface.
func (m *maintenance[K, V, G, X, S]) OnDelete(_ K, rawKey []byte, old *V) error {
	return m.change(rawKey, old, nil)
}

func (m *maintenance[K, V, G, X, S]) change(rawKey []byte, old, new *V) error {
	oc, err := m.a.contributionOf(old)
	if err != nil {
		return err
	}
	nc, err := m.a.contributionOf(new)
	if err != nil {
		return err
	}

	// A record that contributes what it did before leaves the aggregation as it is.
	if oc.equal(nc) {
		return nil
	}
	return m.a.kind.change(m.tx, rawKey, oc, nc)
}

// Instantiate creates the instance of the aggregation for a transaction.
func (a *Agg[K, V, G, X, S]) Instantiate(tx kv.Tx) *Instance[K, V, G, X, S] {
	return &Instance[K, V, G, X, S]{a: a, tx: a.ns.Open(tx)}
}

// Instance is an Agg in a transaction, which it is read with.
type Instance[K, V, G, X, S any] struct {
	a  *Agg[K, V, G, X, S]
	tx *prefix.Tx
}

// Get returns the state of a group for all of time, and whether the group has any record.
// It is exact: it takes into account what the collector has not collected yet.
func (i *Instance[K, V, G, X, S]) Get(group G) (S, bool, error) {
	return i.At(AllTime, group, time.Time{})
}

// GetCollected is like Get but leaves out what the collector has not collected yet,
// which makes it cheaper and possibly out of date.
func (i *Instance[K, V, G, X, S]) GetCollected(group G) (S, bool, error) {
	return i.AtCollected(AllTime, group, time.Time{})
}

// At returns the state of a group for the span of a bucket that a time is in, and whether the
// group has any record in that span.
// It is exact: it takes into account what the collector has not collected yet.
func (i *Instance[K, V, G, X, S]) At(b Bucket, group G, t time.Time) (S, bool, error) {
	return i.at(b, group, t, false)
}

// AtCollected is like At but leaves out what the collector has not collected yet,
// which makes it cheaper and possibly out of date.
func (i *Instance[K, V, G, X, S]) AtCollected(b Bucket, group G, t time.Time) (S, bool, error) {
	return i.at(b, group, t, true)
}

func (i *Instance[K, V, G, X, S]) at(b Bucket, group G, t time.Time, collectedOnly bool) (state S, ok bool, err error) {
	if err := i.check(b); err != nil {
		return state, false, err
	}
	gbz, err := i.a.encodeGroup(group)
	if err != nil {
		return state, false, err
	}

	lo := b.Start(t)
	points, err := i.a.kind.series(i.tx, b, gbz, lo, b.Next(lo), b == AllTime, collectedOnly)
	if err != nil || len(points) == 0 {
		return state, false, err
	}
	return points[0].State, true, nil
}

// SeriesOption is an option of reading a series.
type SeriesOption func(*seriesOptions)

type seriesOptions struct {
	fillGaps bool
}

// FillGaps makes a series have a point with the zero state for every span that has no record.
func FillGaps() SeriesOption {
	return func(o *seriesOptions) {
		o.fillGaps = true
	}
}

// Series returns the states of a group for the spans of a bucket, from the span that the time
// from is in up to the spans that start before the time to, in the order of time.
// Only the spans that have records in them are returned, unless FillGaps is given.
// It is exact: it takes into account what the collector has not collected yet.
func (i *Instance[K, V, G, X, S]) Series(b Bucket, group G, from, to time.Time, opts ...SeriesOption) ([]Point[S], error) {
	return i.series(b, group, from, to, false, opts)
}

// SeriesCollected is like Series but leaves out what the collector has not collected yet,
// which makes it cheaper and possibly out of date.
func (i *Instance[K, V, G, X, S]) SeriesCollected(b Bucket, group G, from, to time.Time, opts ...SeriesOption) ([]Point[S], error) {
	return i.series(b, group, from, to, true, opts)
}

func (i *Instance[K, V, G, X, S]) series(
	b Bucket,
	group G,
	from, to time.Time,
	collectedOnly bool,
	opts []SeriesOption,
) ([]Point[S], error) {
	if err := i.check(b); err != nil {
		return nil, err
	}
	gbz, err := i.a.encodeGroup(group)
	if err != nil {
		return nil, err
	}
	var o seriesOptions
	for _, opt := range opts {
		opt(&o)
	}

	all := b == AllTime
	lo, hi := b.Start(from), b.Start(to)
	if hi.Before(to) {
		// A span that starts before the end is in the series as a whole.
		hi = b.Next(hi)
	}
	if !all && !lo.Before(hi) {
		return nil, nil
	}

	points, err := i.a.kind.series(i.tx, b, gbz, lo, hi, all, collectedOnly)
	if err != nil || !o.fillGaps || all {
		return points, err
	}

	filled := make([]Point[S], 0, len(points))
	for start := lo; start.Before(hi); start = b.Next(start) {
		if len(points) > 0 && points[0].Start.Equal(start) {
			filled = append(filled, points[0])
			points = points[1:]
			continue
		}
		filled = append(filled, Point[S]{Start: start})
	}
	return filled, nil
}

// check reports whether the aggregation has the bucket.
func (i *Instance[K, V, G, X, S]) check(b Bucket) error {
	own, ok := i.a.bucket(b.ID())
	if !ok || own.Name() != b.Name() {
		return fmt.Errorf("%w: %s", ErrNoBucket, b.Name())
	}
	return nil
}
