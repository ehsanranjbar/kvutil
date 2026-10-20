// Package keyfetch is the workload that asks whether a "bitmap heap scan" pays on Badger: given K
// primary keys in random order (as an index gives them), is it faster to read the records in
// that order, or to sort the keys first and read them in the order of the keys? It runs on kvutil
// only.
//
// The records are in raw Badger (8 byte keys, msgpack values; the ids are 0 to N-1), and K of them
// are read in four ways, in a read transaction of their own:
//
//	random-get  one Get for each key, in the order that the keys came in
//	sorted-get  the keys sorted first, then one Get for each (only the order changes)
//	seek        the keys sorted, then one iterator that Seeks to each in turn
//	scan        the keys sorted, then one iterator from the least to the greatest, which steps over
//	            the keys that are not wanted (run when the keys are 1% of the records or more: it
//	            reads all of them, wanted or not)
//
// Each way is timed from the unsorted ids to the last value read, so the sort is a part of the
// ways that sort. A set of keys is the same for every way (the trials are numbered), and the ways
// must read the same values, which they check. The iterators do not read values ahead: the keys
// that are skipped would have their values read.
//
// The cache is warm (the database was read in full before) or cold (the database is opened again
// before each trial, which empties the block cache of Badger; the page cache of the operating system
// stays). The variants are the sizes of the values, small (about 200 bytes) and large (4 KB), whatever
// -values says; large has at most 250,000 records. The results have the nanoseconds a record took
// (extra: ns_per_record_median, _min, _mean, _max), over the trials.
package keyfetch

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/kv"
)

func init() { harness.Register(workload{}) }

type workload struct{}

func (workload) Name() string  { return "key-order-fetch" }
func (workload) Group() string { return "query" }
func (workload) Description() string {
	return "K records by key in random order: Gets, or sorted keys read with an iterator (Seek, or a scan with skipping); kvutil only"
}

func (workload) Variants(sys harness.System) []harness.Variant {
	if _, ok := sys.(*kvsys.System); !ok {
		return nil
	}
	return []harness.Variant{&variant{name: "small", values: dataset.Small}, &variant{name: "large", values: dataset.Large}}
}

type variant struct {
	name   string
	values dataset.ValueSize
}

func (v *variant) Name() string { return v.name }

// The ways of reading the records.
var ways = []string{"random-get", "sorted-get", "seek", "scan"}

// records is the number of records of a variant.
func (v *variant) records(scale int) int64 {
	if v.values == dataset.Large {
		return int64(min(scale, 250_000))
	}
	return int64(scale)
}

// counts returns the numbers K of keys that are read from n records: 10 to 100,000, but never more
// than half of the records, and that half as well when it is more than all of those.
func counts(n int64) []int64 {
	var ks []int64
	for _, k := range []int64{10, 100, 1000, 10_000, 100_000} {
		if k <= n/2 {
			ks = append(ks, k)
		}
	}
	if half := min(n/2, 500_000); len(ks) == 0 || half > ks[len(ks)-1] {
		ks = append(ks, half)
	}
	return ks
}

// scans says whether the way scan is run for K keys of n: when they are a hundredth of the records.
func scans(k, n int64) bool { return k*100 >= n }

// pick returns k distinct ids of [0, n) for the trial, in a random order.
func pick(seed uint64, n, k int64, trial int) []int64 {
	r := harness.NewRand(seed^0xfe7c4, uint64(k)<<16|uint64(trial)+1)
	if k*4 > n {
		perm := r.Perm(int(n))
		ids := make([]int64, k)
		for i := range ids {
			ids[i] = int64(perm[i])
		}
		return ids
	}
	seen := make(map[int64]struct{}, k)
	ids := make([]int64, 0, k)
	for int64(len(ids)) < k {
		id := r.Int64N(n)
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids
}

// fetched is what a way read: the number of values, their bytes, and a checksum that depends on
// the values and on which key each came from.
type fetched struct {
	values int
	bytes  int64
	check  uint64
}

func (f *fetched) add(id int64, value []byte) {
	f.values++
	f.bytes += int64(len(value))
	// The first and the last bytes and the length are enough to tell values of different keys apart.
	h := uint64(len(value))*0x9e3779b97f4a7c15 ^ uint64(id)*0xbf58476d1ce4e5b9
	if len(value) > 0 {
		h ^= uint64(value[0])<<8 | uint64(value[len(value)-1])<<16
	}
	f.check += h
}

// fetch reads the records of the ids in a way, in a read transaction.
func fetch(db *kvsys.DB, way string, ids []int64) (fetched, error) {
	var f fetched
	err := db.View(func(tx kv.Tx) error {
		f = fetched{}
		switch way {
		case "random-get":
			for _, id := range ids {
				v, err := tx.Get(wl.Key8(id))
				if err != nil {
					return fmt.Errorf("get %d: %w", id, err)
				}
				f.add(id, v)
			}
			return nil
		case "sorted-get":
			sorted := slices.Clone(ids)
			slices.Sort(sorted)
			for _, id := range sorted {
				v, err := tx.Get(wl.Key8(id))
				if err != nil {
					return fmt.Errorf("get %d: %w", id, err)
				}
				f.add(id, v)
			}
			return nil
		case "seek":
			sorted := slices.Clone(ids)
			slices.Sort(sorted)
			it, err := tx.Iterator(nil, nil)
			if err != nil {
				return err
			}
			defer it.Close()
			for _, id := range sorted {
				key := wl.Key8(id)
				it.Seek(key)
				if !it.Valid() || !bytes.Equal(it.Key(), key) {
					return fmt.Errorf("seek %d: not found", id)
				}
				v, err := it.Value()
				if err != nil {
					return err
				}
				f.add(id, v)
			}
			return nil
		case "scan":
			sorted := slices.Clone(ids)
			slices.Sort(sorted)
			it, err := tx.Iterator(wl.Key8(sorted[0]), append(wl.Key8(sorted[len(sorted)-1]), 0))
			if err != nil {
				return err
			}
			defer it.Close()
			want := 0
			wanted := wl.Key8(sorted[0])
			for it.Valid() && want < len(sorted) {
				switch c := bytes.Compare(it.Key(), wanted); {
				case c < 0:
					it.Next()
				case c == 0:
					v, err := it.Value()
					if err != nil {
						return err
					}
					f.add(sorted[want], v)
					want++
					if want < len(sorted) {
						wanted = wl.Key8(sorted[want])
					}
					it.Next()
				default:
					return fmt.Errorf("scan: key %d is missing", sorted[want])
				}
			}
			if want != len(sorted) {
				return fmt.Errorf("scan: read %d of %d", want, len(sorted))
			}
			return nil
		}
		return fmt.Errorf("unknown way %q", way)
	})
	return f, err
}

// open creates the database of a variant and loads the records.
func (v *variant) open(sys *kvsys.System, spec dataset.Spec, n int64) (*kvsys.DB, error) {
	db, err := sys.OpenPrefetch("keyfetch-"+v.name, 0)
	if err != nil {
		return nil, err
	}
	spec.Values = v.values
	_, err = wl.BulkLoad(db, 0, n, 1, func() func(kv.BulkWriter, int64) error {
		o := &dataset.Order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			return w.Set(wl.Key8(id), wl.Msgpack(o))
		}
	})
	if err == nil {
		err = db.Sync()
	}
	if err != nil {
		db.Destroy()
		return nil, err
	}
	return db, nil
}

// warm reads every value of the database, so that its blocks are in the cache of Badger as far as
// it holds them.
func warm(db *kvsys.DB) error {
	return db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(nil, nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			if _, err := it.Value(); err != nil {
				return err
			}
		}
		return nil
	})
}

const (
	maxTrials     = 25
	minTrials     = 3
	maxColdTrials = 5
)

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	p := rc.Params
	n := v.records(p.Scale)
	spec := p.Dataset()

	db, err := v.open(rc.Sys.(*kvsys.System), spec, n)
	if err != nil {
		return err
	}
	defer db.Destroy()
	rc.SizeFn = func(context.Context) (harness.Sizes, error) {
		b, err := db.DiskBytes()
		return harness.Sizes{Data: b}, err
	}
	sample := spec
	sample.Values = v.values
	valueBytes := len(wl.Msgpack(sample.Gen(0, 0)))

	for _, cache := range []string{"warm", "cold"} {
		if cache == "warm" {
			if err := warm(db); err != nil {
				return err
			}
		}
		for _, k := range counts(n) {
			for _, way := range ways {
				if way == "scan" && !scans(k, n) {
					continue
				}
				budget := p.Duration
				maxT := maxTrials
				if cache == "cold" {
					maxT = maxColdTrials
				}

				var perRecord []float64
				var elapsed time.Duration
				start := time.Now()
				for trial := 0; trial < maxT && (trial < minTrials || elapsed < budget); trial++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					ids := pick(p.Seed, n, k, trial)
					if cache == "cold" {
						if err := db.Reopen(); err != nil {
							return err
						}
					}
					t0 := time.Now()
					f, err := fetch(db, way, ids)
					d := time.Since(t0)
					if err != nil {
						return fmt.Errorf("%s k=%d %s: %w", way, k, cache, err)
					}
					if int64(f.values) != k {
						return fmt.Errorf("%s k=%d %s: read %d values", way, k, cache, f.values)
					}
					elapsed += d
					perRecord = append(perRecord, float64(d.Nanoseconds())/float64(k))
				}
				sort.Float64s(perRecord)
				var sum float64
				for _, x := range perRecord {
					sum += x
				}
				err := rc.Report(ctx, harness.Case{
					Name: fmt.Sprintf("%s/k%d/%s", way, k, cache), Concurrency: 1,
					Params: map[string]any{"way": way, "k": k, "cache": cache, "records": n,
						"values": string(v.values), "value_bytes": valueBytes},
				}, start, time.Now(), metrics.Metrics{
					ElapsedSec: elapsed.Seconds(),
					Extra: map[string]float64{
						"trials":               float64(len(perRecord)),
						"ns_per_record_median": perRecord[len(perRecord)/2],
						"ns_per_record_min":    perRecord[0],
						"ns_per_record_mean":   sum / float64(len(perRecord)),
						"ns_per_record_max":    perRecord[len(perRecord)-1],
						"percent_of_the_table": float64(k) / float64(n) * 100,
					},
				})
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Verify has every way read the same sets of keys, and the database opened again in between, and
// says what they read. The ways of a size of the values must read the same.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for _, hv := range (workload{}).Variants(sys) {
		v := hv.(*variant)
		n := v.records(p.Scale)
		db, err := v.open(sys.(*kvsys.System), p.Dataset(), n)
		if err != nil {
			return nil, err
		}
		byWay := map[string]*harness.Output{}
		for _, way := range ways {
			byWay[way] = &harness.Output{Variant: v.name + "/" + way, Group: v.name}
		}
		err = func() error {
			defer db.Destroy()
			for i, k := range append(counts(n), 1, n) {
				ids := pick(p.Seed, n, k, i)
				if i%2 == 1 {
					if err := db.Reopen(); err != nil {
						return err
					}
				}
				for _, way := range ways {
					f, err := fetch(db, way, ids)
					if err != nil {
						return fmt.Errorf("%s k=%d: %w", way, k, err)
					}
					byWay[way].Lines = append(byWay[way].Lines,
						fmt.Sprintf("k=%d: %d values, %d bytes, check %016x", k, f.values, f.bytes, f.check))
				}
			}
			return nil
		}()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}
		for _, way := range ways {
			outs = append(outs, *byWay[way])
		}
	}
	return outs, nil
}
