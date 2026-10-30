// Package storage is the workload of what the data takes on disk: the bytes of a record and of
// each index, how much the disk grows when every record is written again and when half of them
// are deleted, how much is written to the disk for what the user writes (the write amplification),
// what gets the space back, and what expiring records cost.
//
// A variant is a way of running it:
//
//	footprint       N records, then each of four indexes built over them, then every record
//	                written again three times, half of them deleted, and the space reclaimed:
//	                kvutil RunValueLogGC and Flatten / PostgreSQL VACUUM and VACUUM FULL
//	footprint-vlog  kvutil only: the same with the values in the value log (Badger keeps values
//	                under 1 MiB in the tree, where the value log has nothing to collect), in files of
//	                16 MiB so that the files can be collected before the first gigabyte
//	expiry          records that expire (kvutil SetWithTTL, no index: a store with indexes cannot
//	                set a TTL) against a table with an expiry time that a job deletes from every
//	                second: the cost of writing them, and the space that comes back
//
// Every step ends with the writes made durable (kvutil closes and opens the database, which
// flushes its memtable; PostgreSQL checkpoints), and reports the size on disk and the bytes that
// the container wrote to block devices while it ran, where that can be read: from the cgroup of the
// harness' container (kvutil), from the Docker API (PostgreSQL's container), and not at all
// otherwise, which the result says (info.io_source).
package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/dockerapi"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/metrics"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
)

func init() { harness.Register(workload{}) }

// indexKinds are the indexes of the footprint, in the order that they are built.
var indexKinds = []string{"customer", "code", "composite", "tags"}

// overwrites is the number of times that every record is written again.
const overwrites = 3

// expiryTTL is the life of the expiring records. The job that deletes them runs every second.
var expiryTTL = 4 * time.Second

type workload struct{}

func (workload) Name() string  { return "storage" }
func (workload) Group() string { return "storage" }
func (workload) Description() string {
	return "bytes per record and per index, growth under overwrites and deletes, write amplification, VACUUM / VACUUM FULL against Flatten and value log GC, expiring records"
}

type variant struct {
	name string
	what string // footprint or expiry
	vlog bool
}

func (v *variant) Name() string { return v.name }

func (workload) Variants(sys harness.System) []harness.Variant {
	switch sys.(type) {
	case *kvsys.System:
		return []harness.Variant{
			&variant{name: "footprint", what: "footprint"},
			&variant{name: "footprint-vlog", what: "footprint", vlog: true},
			&variant{name: "expiry", what: "expiry"},
		}
	case *pgsys.System:
		return []harness.Variant{
			&variant{name: "footprint", what: "footprint"},
			&variant{name: "expiry", what: "expiry"},
		}
	}
	return nil
}

// reclaim is something that gives space back.
type reclaim struct {
	Name string
	Run  func(ctx context.Context) error
}

// driver is the database of a system.
type driver interface {
	Load(ctx context.Context, spec dataset.Spec, n int64) error
	AddIndex(ctx context.Context, kind string) error
	// Components are the sizes of the table and of each index: figures by name (table_bytes,
	// index_<kind>_bytes, and what else the system can say).
	Components(ctx context.Context) (map[string]float64, error)
	// Rewrite writes every record of [1, n] again in a version.
	Rewrite(ctx context.Context, spec dataset.Spec, n int64, version uint32) error
	// DeleteOdd deletes the records with odd ids of [1, n].
	DeleteOdd(ctx context.Context, n int64) error
	Count(ctx context.Context) (int64, error)
	Reclaims() []reclaim

	// ExpiryPrepare makes what the expiry needs; ExpiryInsert writes a record that expires after
	// the ttl, or one that does not if the ttl is 0. The job that deletes from the table runs
	// between ExpiryStartJob and the function it returns, which says what it did.
	ExpiryPrepare(ctx context.Context) error
	ExpiryInsert(ctx context.Context, o *dataset.Order, ttl time.Duration) error
	ExpiryStartJob(ctx context.Context, every time.Duration) (stop func() map[string]float64)
	// ExpiryLive counts the records that are there and the ones that have not expired.
	ExpiryLive(ctx context.Context) (plain, expiring int64, err error)

	// Flush makes what was written reach the disk, and the figures of the system true.
	Flush(ctx context.Context) error
	Sizes(ctx context.Context) (data, wal int64, err error)
	// Meter reads the bytes that the container of the database wrote to block devices.
	Meter() *dockerapi.Meter
	// Counters are the figures of the system about the bytes it has written, as numbers that
	// only grow (PostgreSQL's WAL and its I/O statistics).
	Counters(ctx context.Context) map[string]float64
	Close(ctx context.Context) error
}

func openDriver(ctx context.Context, sys harness.System, v *variant) (driver, error) {
	client := dockerapi.New(os.Getenv("KVB_DOCKER_SOCKET"))
	switch s := sys.(type) {
	case *kvsys.System:
		return openKV(ctx, s, v, dockerapi.OwnMeter(ctx, client, os.Getenv("KVB_SELF_CONTAINER")))
	case *pgsys.System:
		return openPG(ctx, s, v, dockerapi.ContainerMeter(ctx, client, os.Getenv("KVB_PG_CONTAINER")))
	}
	return nil, fmt.Errorf("storage does not run on %s", sys.Name())
}

// avgRecordBytes is the mean size of the record in a version, encoded: what the user writes.
func avgRecordBytes(spec dataset.Spec, version uint32) float64 {
	const sample = 200
	var total int
	for id := int64(1); id <= sample; id++ {
		total += len(wl.Msgpack(spec.Gen(id, version)))
	}
	return float64(total) / sample
}

type runner struct {
	rc    *harness.RunContext
	d     driver
	spec  dataset.Spec
	n     int64
	avg   float64
	infos map[string]string
}

func (v *variant) Run(ctx context.Context, rc *harness.RunContext) error {
	d, err := openDriver(ctx, rc.Sys, v)
	if err != nil {
		return err
	}
	defer d.Close(context.WithoutCancel(ctx))
	rc.SizeFn = func(ctx context.Context) (harness.Sizes, error) {
		data, wal, err := d.Sizes(ctx)
		return harness.Sizes{Data: data, WAL: wal}, err
	}
	spec := rc.Params.Dataset()
	r := &runner{rc: rc, d: d, spec: spec, n: int64(rc.Params.Scale), avg: avgRecordBytes(spec, 0),
		infos: map[string]string{"io_source": d.Meter().Source}}
	if v.what == "expiry" {
		return r.runExpiry(ctx)
	}
	return r.runFootprint(ctx)
}

// step runs fn, makes the writes durable, and reports the time, the size on disk before and
// after, the bytes written by the container, and the records and bytes the user wrote.
func (r *runner) step(ctx context.Context, name string, records int64, userBytes float64, params map[string]any, fn func(ctx context.Context) error) (map[string]float64, error) {
	if err := r.d.Flush(ctx); err != nil {
		return nil, err
	}
	dataBefore, walBefore, _ := r.d.Sizes(ctx)
	io0, ioErr0 := r.d.Meter().Read(ctx)
	c0 := r.d.Counters(ctx)

	start := time.Now()
	probe := metrics.StartProbe()
	if err := fn(ctx); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	res := probe.Stop()
	end := time.Now()
	if err := r.d.Flush(ctx); err != nil {
		return nil, err
	}

	dataAfter, walAfter, _ := r.d.Sizes(ctx)
	io1, ioErr1 := r.d.Meter().Read(ctx)
	extra := map[string]float64{
		"disk_bytes_before": float64(dataBefore), "disk_bytes_after": float64(dataAfter),
		"disk_growth_bytes": float64(dataAfter - dataBefore),
		"wal_bytes_before":  float64(walBefore), "wal_bytes_after": float64(walAfter),
		"user_bytes": userBytes, "duration_incl_flush_sec": time.Since(start).Seconds(),
	}
	if ioErr0 == nil && ioErr1 == nil {
		w := float64(io1 - io0)
		extra["container_write_bytes"] = w
		if userBytes > 0 {
			extra["write_amp"] = w / userBytes
		}
		extra["write_bytes_per_dataset_byte"] = w / (r.avg * float64(r.n))
	}
	for k, after := range r.d.Counters(ctx) {
		extra[k] = after - c0[k]
	}
	if wal, ok := extra["wal_bytes"]; ok && userBytes > 0 {
		extra["wal_per_user_byte"] = wal / userBytes
	}

	m := metrics.Metrics{ElapsedSec: end.Sub(start).Seconds(), Resources: res, Extra: extra}
	if records > 0 {
		m.Records = records
		m.RecordsPerSec = float64(records) / end.Sub(start).Seconds()
	}
	c := harness.Case{Name: name, Concurrency: 1, Params: params, Info: r.infos}
	return extra, r.rc.Report(ctx, c, start, end, m)
}

func (r *runner) runFootprint(ctx context.Context) error {
	n := r.n
	if _, err := r.step(ctx, "load", n, r.avg*float64(n), nil, func(ctx context.Context) error {
		return r.d.Load(ctx, r.spec, n)
	}); err != nil {
		return err
	}
	base, err := r.d.Components(ctx)
	if err != nil {
		return err
	}

	// Each index is built over the records and measured on its own.
	for _, kind := range indexKinds {
		kind := kind
		if _, err := r.step(ctx, "add-index-"+kind, n, 0, map[string]any{"index": kind}, func(ctx context.Context) error {
			return r.d.AddIndex(ctx, kind)
		}); err != nil {
			return err
		}
	}
	comp, err := r.d.Components(ctx)
	if err != nil {
		return err
	}
	comp["bytes_per_record"] = comp["total_bytes"] / float64(n)
	comp["table_bytes_per_record"] = comp["table_bytes"] / float64(n)
	comp["user_bytes_per_record"] = r.avg
	for _, kind := range indexKinds {
		comp["index_"+kind+"_bytes_per_record"] = comp["index_"+kind+"_bytes"] / float64(n)
	}
	comp["table_bytes_before_indexes"] = base["table_bytes"]
	start := time.Now()
	if err := r.rc.Report(ctx, harness.Case{Name: "footprint", Concurrency: 1, Info: r.infos}, start, start,
		metrics.Metrics{Records: n, Extra: comp}); err != nil {
		return err
	}
	if live, err := r.d.Count(ctx); err != nil || live != n {
		return fmt.Errorf("after the load there are %d records, not %d (%v)", live, n, err)
	}

	for round := uint32(1); round <= overwrites; round++ {
		avg := avgRecordBytes(r.spec, round)
		if _, err := r.step(ctx, fmt.Sprintf("overwrite-%d", round), n, avg*float64(n), map[string]any{"round": round}, func(ctx context.Context) error {
			return r.d.Rewrite(ctx, r.spec, n, round)
		}); err != nil {
			return err
		}
		if live, err := r.d.Count(ctx); err != nil || live != n {
			return fmt.Errorf("after overwrite %d there are %d records, not %d (%v)", round, live, n, err)
		}
	}

	if _, err := r.step(ctx, "delete-half", n/2, 0, nil, func(ctx context.Context) error { return r.d.DeleteOdd(ctx, n) }); err != nil {
		return err
	}
	want := n - (n+1)/2
	if live, err := r.d.Count(ctx); err != nil || live != want {
		return fmt.Errorf("after deleting half there are %d records, not %d (%v)", live, want, err)
	}

	return r.reclaimSteps(ctx, n)
}

// reclaimSteps runs what gives space back, one after the other, each as a case.
func (r *runner) reclaimSteps(ctx context.Context, n int64) error {
	for _, rec := range r.d.Reclaims() {
		extra, err := r.step(ctx, rec.Name, 0, 0, nil, rec.Run)
		if err != nil {
			return err
		}
		if extra != nil {
			r.rc.Logf("   %s: %d bytes on disk before, %d after", rec.Name, int64(extra["disk_bytes_before"]), int64(extra["disk_bytes_after"]))
		}
	}
	return nil
}

func (r *runner) runExpiry(ctx context.Context) error {
	d := r.d
	if err := d.ExpiryPrepare(ctx); err != nil {
		return err
	}
	var next atomic.Int64
	init := func(w *harness.Worker) { w.State = &dataset.Order{} }

	// measure runs inserts at a level of concurrency and says what the container wrote for them.
	measure := func(name string, c int, ttl time.Duration) error {
		if err := d.Flush(ctx); err != nil {
			return err
		}
		io0, e0 := d.Meter().Read(ctx)
		c0 := d.Counters(ctx)
		var ok atomic.Int64
		cs := harness.Case{Name: name, Concurrency: c, Init: init, Info: r.infos, Params: map[string]any{"ttl_sec": ttl.Seconds()}}
		start := time.Now()
		err := r.rc.Measure(ctx, cs, func(ctx context.Context, w *harness.Worker) error {
			id := next.Add(1)
			o := w.State.(*dataset.Order)
			r.spec.GenInto(o, id, 0)
			w.Begin()
			err := d.ExpiryInsert(ctx, o, ttl)
			if err == nil {
				ok.Add(1)
			}
			return err
		})
		if err != nil {
			return err
		}
		if err := d.Flush(ctx); err != nil {
			return err
		}
		extra := map[string]float64{"inserts": float64(ok.Load())}
		if io1, e1 := d.Meter().Read(ctx); e0 == nil && e1 == nil && ok.Load() > 0 {
			extra["container_write_bytes"] = float64(io1 - io0)
			extra["write_bytes_per_insert"] = float64(io1-io0) / float64(ok.Load())
			extra["write_amp"] = float64(io1-io0) / (r.avg * float64(ok.Load()))
		}
		for k, after := range d.Counters(ctx) {
			extra[k] = after - c0[k]
		}
		return r.rc.Report(ctx, harness.Case{Name: name + "-io", Concurrency: c, Info: r.infos}, start, time.Now(),
			metrics.Metrics{ElapsedSec: time.Since(start).Seconds(), Extra: extra})
	}

	for _, c := range harness.Levels(r.rc.Params.Concurrency) {
		if err := measure("insert-plain", c, 0); err != nil {
			return err
		}
	}
	stopJob := d.ExpiryStartJob(ctx, time.Second)
	for _, c := range harness.Levels(r.rc.Params.Concurrency) {
		if err := measure("insert-ttl", c, expiryTTL); err != nil {
			stopJob()
			return err
		}
	}
	lastWrite := time.Now()
	dataEnd, _, _ := d.Sizes(ctx)
	_, liveAtEnd, _ := d.ExpiryLive(ctx)

	// Everything written expires, and the job deletes it.
	wait := expiryTTL + 2500*time.Millisecond
	select {
	case <-time.After(time.Until(lastWrite.Add(wait))):
	case <-ctx.Done():
		stopJob()
		return ctx.Err()
	}
	job := stopJob()
	if err := d.Flush(ctx); err != nil {
		return err
	}
	plain, expiring, err := d.ExpiryLive(ctx)
	if err != nil {
		return err
	}
	dataAfter, _, _ := d.Sizes(ctx)
	extra := map[string]float64{
		"records_not_expired_at_the_end_of_the_writes": float64(liveAtEnd), "records_not_expired_after": float64(expiring),
		"records_that_do_not_expire":          float64(plain),
		"disk_bytes_at_the_end_of_the_writes": float64(dataEnd), "disk_bytes_after_expiry": float64(dataAfter),
		"ttl_sec": expiryTTL.Seconds(),
	}
	for k, x := range job {
		extra["job_"+k] = x
	}
	now := time.Now()
	if err := r.rc.Report(ctx, harness.Case{Name: "after-expiry", Concurrency: 1, Info: r.infos}, now, now, metrics.Metrics{Extra: extra}); err != nil {
		return err
	}
	if expiring != 0 {
		return fmt.Errorf("%d records that had expired are still there", expiring)
	}
	return r.reclaimSteps(ctx, 0)
}

// jobStats counts what a job that deletes does, from several calls.
type jobStats struct {
	mu             sync.Mutex
	runs, rows     int64
	total, longest time.Duration
	failures       int64
	firstFailure   string
}

func (j *jobStats) note(rows int64, d time.Duration, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.runs++
	if err != nil {
		j.failures++
		if j.firstFailure == "" {
			j.firstFailure = err.Error()
		}
		return
	}
	j.rows += rows
	j.total += d
	j.longest = max(j.longest, d)
}

func (j *jobStats) result() map[string]float64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return map[string]float64{
		"runs": float64(j.runs), "rows_deleted": float64(j.rows), "failures": float64(j.failures),
		"busy_ms_total": j.total.Seconds() * 1000, "longest_run_ms": j.longest.Seconds() * 1000,
	}
}

// Verify counts the records at every step on a small number of them: the records after the load,
// the rewrites and the deletion of half, and the records that expire.
func (workload) Verify(ctx context.Context, sys harness.System, p harness.Params) ([]harness.Output, error) {
	var outs []harness.Output
	for _, hv := range (workload{}).Variants(sys) {
		v := hv.(*variant)
		if v.vlog {
			continue
		}
		lines, err := v.verify(ctx, sys, p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", v.name, err)
		}
		outs = append(outs, harness.Output{Variant: v.name, Group: v.name, Lines: lines})
	}
	return outs, nil
}

func (v *variant) verify(ctx context.Context, sys harness.System, p harness.Params) ([]string, error) {
	d, err := openDriver(ctx, sys, v)
	if err != nil {
		return nil, err
	}
	defer d.Close(context.WithoutCancel(ctx))
	spec := p.Dataset()
	n := int64(p.Scale)
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	if v.what == "footprint" {
		if err := d.Load(ctx, spec, n); err != nil {
			return nil, err
		}
		for _, k := range indexKinds {
			if err := d.AddIndex(ctx, k); err != nil {
				return nil, err
			}
		}
		count := func(stage string) error {
			c, err := d.Count(ctx)
			add("%s: %d records", stage, c)
			return err
		}
		if err := count("after the load"); err != nil {
			return nil, err
		}
		for round := uint32(1); round <= 2; round++ {
			if err := d.Rewrite(ctx, spec, n, round); err != nil {
				return nil, err
			}
			if err := count(fmt.Sprintf("after the rewrite %d", round)); err != nil {
				return nil, err
			}
		}
		if err := d.DeleteOdd(ctx, n); err != nil {
			return nil, err
		}
		if err := count("after deleting half"); err != nil {
			return nil, err
		}
		for _, rec := range d.Reclaims() {
			if err := rec.Run(ctx); err != nil {
				return nil, fmt.Errorf("%s: %w", rec.Name, err)
			}
		}
		if err := count("after reclaiming"); err != nil {
			return nil, err
		}
		return lines, nil
	}

	if err := d.ExpiryPrepare(ctx); err != nil {
		return nil, err
	}
	stop := d.ExpiryStartJob(ctx, 500*time.Millisecond)
	const ttl = 2 * time.Second
	for id := int64(1); id <= n; id++ {
		ttlOf := ttl
		if id%2 == 0 {
			ttlOf = 0
		}
		if err := d.ExpiryInsert(ctx, spec.Gen(id, 0), ttlOf); err != nil {
			stop()
			return nil, err
		}
	}
	plain, expiring, err := d.ExpiryLive(ctx)
	if err != nil {
		stop()
		return nil, err
	}
	add("just written: %d records that do not expire, %d that do", plain, expiring)
	time.Sleep(ttl + 2*time.Second)
	stop()
	plain, expiring, err = d.ExpiryLive(ctx)
	if err != nil {
		return nil, err
	}
	add("after the ttl: %d records that do not expire, %d that do", plain, expiring)
	return lines, nil
}
