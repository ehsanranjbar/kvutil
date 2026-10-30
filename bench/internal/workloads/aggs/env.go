package aggs

import (
	"context"
	"fmt"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/tssys"
)

// env is a database with the events in it, and the aggregates of one variant of one system.
type env interface {
	// Load writes the events 1 to n. The aggregates are kept as the system keeps them: noted
	// for the collector (kvutil), in the same statements (the summary table of PostgreSQL),
	// or invalidated for the refresh (TimescaleDB, which needs CatchUp).
	Load(ctx context.Context, n int64) error
	// CatchUp makes the stored aggregates include everything that was written: kvutil's
	// Collect, a refresh of the whole range of every continuous aggregate. It is nothing
	// for the systems that keep them up to date in the write.
	CatchUp(ctx context.Context) error
	// Background starts or stops what keeps the stored aggregates up to date while writes go
	// on: kvutil's collector, the refresh policies of TimescaleDB. It is nothing for PostgreSQL.
	Background(ctx context.Context, on bool) error
	Insert(ctx context.Context, e *dataset.Event) error
	// InsertBatch writes the events in one transaction.
	InsertBatch(ctx context.Context, es []*dataset.Event) error
	// Update changes an event that is there. The old one says what it was, which the systems
	// that find a row by its time need.
	Update(ctx context.Context, old, new *dataset.Event) error
	Delete(ctx context.Context, old *dataset.Event) error
	// Read answers a query the way that the variant does: the exact way or the collected
	// one, the real-time or the materialized one, computed or from the summary.
	Read(ctx context.Context, q query) ([]row, error)
	// Truth answers a query exactly, from the events. kvutil has no scan of the events that
	// groups them, so its truth is its exact read.
	Truth(ctx context.Context, q query) ([]row, error)
	// Sizes is the size of the database on disk.
	Sizes(ctx context.Context) (harness.Sizes, error)
	// Storage is the size of the events and of their aggregates.
	Storage(ctx context.Context) (storage, error)
	// Stats are the counters of the background work, like the conflicts of the collector.
	Stats() map[string]float64
	Close(ctx context.Context) error
}

// storage is the size of the events and of the aggregates that are kept for them. It is on disk
// for PostgreSQL and TimescaleDB, and the sum of the lengths of the keys and values (before the
// compression of Badger) for kvutil: Logical says which.
type storage struct {
	Events, Aggs int64
	Logical      bool
}

// A variant is a way that a system keeps and reads the aggregates.
const (
	kvCollector    = "collector"    // kvutil, with the collector running; reads are exact
	kvExact        = "exact"        // kvutil, exact reads: pending notes are folded in
	kvCollected    = "collected"    // kvutil, reads of the collected state only
	pgComputed     = "computed"     // PostgreSQL: no aggregate, computed at read time
	pgTrigger      = "trigger"      // PostgreSQL: a summary table kept by a trigger
	tsRealtime     = "realtime"     // TimescaleDB: continuous aggregates read as real-time aggregates
	tsMaterialized = "materialized" // TimescaleDB: continuous aggregates read as materialized only
	tsCaggs        = "caggs"        // TimescaleDB: the continuous aggregates, for the workload that builds them
)

// variantSet says which variants a workload has on each system.
type variantSet struct {
	kv, pg, ts []string
}

func (v variantSet) of(sys harness.System) []string {
	switch sys.(type) {
	case *kvsys.System:
		return v.kv
	case *pgsys.System:
		return v.pg
	case *tssys.System:
		return v.ts
	}
	return nil
}

var (
	writeVariants = variantSet{
		kv: []string{kvCollector},
		pg: []string{pgComputed, pgTrigger},
		ts: []string{tsRealtime, tsMaterialized},
	}
	readVariants = variantSet{
		kv: []string{kvExact, kvCollected},
		pg: []string{pgComputed, pgTrigger},
		ts: []string{tsRealtime, tsMaterialized},
	}
	rebuildVariants = variantSet{
		kv: []string{kvCollector},
		pg: []string{pgTrigger},
		ts: []string{tsCaggs},
	}
)

// openEnv creates the database of a variant, with the events table (or store) and the definitions
// of the aggregates, and nothing in it yet. A plain one has no aggregate defined: it is the
// events alone, which the workload that builds the aggregates starts from.
func openEnv(ctx context.Context, sys harness.System, name string, s setup, schema string, plain bool) (env, error) {
	switch sys := sys.(type) {
	case *kvsys.System:
		return openKV(ctx, sys, name, s, schema, plain)
	case *pgsys.System:
		return openSQL(ctx, sys, nil, name, s, schema, plain)
	case *tssys.System:
		return openSQL(ctx, sys.System, sys, name, s, schema, plain)
	}
	return nil, fmt.Errorf("aggs: unknown system %T", sys)
}

// rebuilder is an env opened plain, which builds its aggregates over the events that are there.
type rebuilder interface {
	env
	// LoadPlain writes the events 1 to n, with no aggregate defined.
	LoadPlain(ctx context.Context, n int64) error
	// Build defines the aggregates and builds them; step runs one part of it that is timed.
	Build(ctx context.Context, step func(name string, fn func(context.Context) error) error) error
}

// zoner is an env that can read through a session with another time zone.
type zoner interface {
	SetZone(ctx context.Context, zone string) (restore func(), err error)
}

// settler is an env that has housekeeping to do before it is measured.
type settler interface {
	settle(ctx context.Context) error
}
