// Package upkeep runs the background work of the extensions of stores, like turning the notes of
// aggregations into stored states or keeping the partitions of vector indexes.
//
// The work is described as jobs. An extension that has some provides them (Provider), and a
// Runner, which a database should have one of, finds them among the extensions of the stores
// that are added to it and runs them: on demand with Drain, or in the background with Spawn.
// Everything the extensions do for their stores is right without it, but reading them takes longer
// the more there is that was not done.
//
// A job is done a bounded step at a time, each in a transaction of its own, so that it does not
// keep the others waiting and is not cut off in the middle of a change when the runner is shut
// down. A job that fails is left alone for a while, longer the more it fails, and the others go on.
package upkeep

import (
	"context"
	"time"

	"github.com/ehsanranjbar/kvutil/kv"
)

// DefaultInterval is the default time that a running runner waits between two rounds.
const DefaultInterval = time.Second

// Job is background work, done a bounded step at a time in transactions of its own.
//
// The steps of one job never run concurrently with each other within one Runner. A Job must be
// comparable, and an extension must return the same values from Jobs every time: the runner
// keeps the state of a job, like how many times it failed, by its value.
type Job interface {
	// Name names the job in errors and stats, e.g. `aggregation "sales"`.
	Name() string
	// Step goes on from pass.Cursor, which is nil at the start of a pass, and returns the cursor
	// to go on from, or nil when the pass is over. An error ends the pass.
	Step(ctx context.Context, db kv.DB, pass Pass) (next []byte, err error)
}

// Pass is what a step is told of the pass that it is a part of.
type Pass struct {
	// Cursor is what the previous step of the pass returned, or nil at its start.
	Cursor []byte
	// Drain is set when everything that is outstanding is to be done (Runner.Drain), and not
	// only one round's share of it.
	Drain bool
	// Batch is the size of a step that the runner was given, or 0 for the job's own default.
	Batch int
	// Report is told of what a step puts aside without failing, like a single note that cannot
	// be applied. It is never nil.
	Report func(error)
}

// Provider is implemented by the extensions that have background work, which is how a runner
// finds the jobs among the extensions of a store.
type Provider interface {
	// Jobs returns the jobs of the extension, the same values every time.
	Jobs() []Job
}

// Source is a store whose extensions may have jobs, like an ext store or a record store.
type Source interface {
	// Extensions returns the extensions of the store.
	Extensions() []any
}
