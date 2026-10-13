package agg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/schema"
)

const (
	// DefaultBatchSize is the default number of notes that a collector takes in one transaction.
	DefaultBatchSize = 1000
	// DefaultInterval is the default time that a running collector waits between two rounds.
	DefaultInterval = time.Second
)

// collectable is implemented by the aggregations, whatever their types are.
type collectable interface {
	collectName() string
	// collectBatch turns a batch of what was noted after the cursor into stored states.
	// It returns the cursor to go on from, or nil when there is nothing after it.
	collectBatch(db kv.DB, cursor []byte, batch int, report func(error)) (next []byte, err error)
}

// Collector turns what the transactions have noted for the aggregations of a schema into their
// stored states. A database should have one collector running. The aggregations are exact
// without it, but reading them takes longer the more there is that was not collected.
type Collector struct {
	db       kv.DB
	sch      *schema.Schema
	batch    int
	interval time.Duration
	onError  func(error)
	mu       sync.Mutex
}

// CollectorOption is an option of a Collector.
type CollectorOption func(*Collector)

// WithBatchSize sets the number of notes that the collector takes in one transaction.
func WithBatchSize(n int) CollectorOption {
	return func(c *Collector) {
		c.batch = n
	}
}

// WithInterval sets the time that a running collector waits between two rounds.
func WithInterval(d time.Duration) CollectorOption {
	return func(c *Collector) {
		c.interval = d
	}
}

// WithErrorHandler sets the function that is told of what a collector could not collect.
// What is wrong with a single note does not stop the collector: the note is put aside, or left
// for the next round, and the collector goes on.
func WithErrorHandler(f func(error)) CollectorOption {
	return func(c *Collector) {
		c.onError = f
	}
}

// NewCollector creates a collector for the aggregations that are defined in a schema.
func NewCollector(db kv.DB, sch *schema.Schema, opts ...CollectorOption) *Collector {
	c := &Collector{
		db:       db,
		sch:      sch,
		batch:    DefaultBatchSize,
		interval: DefaultInterval,
		onError:  func(error) {},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// aggs returns the aggregations that are defined in the schema right now.
func (c *Collector) aggs() []collectable {
	var aggs []collectable
	for _, def := range c.sch.Definitions() {
		if a, ok := def.Object.(collectable); ok {
			aggs = append(aggs, a)
		}
	}
	return aggs
}

// Collect collects everything that was noted for the aggregations before it was called,
// and returns once it is all in the stored states.
func (c *Collector) Collect(ctx context.Context) error {
	return c.collect(ctx, c.aggs())
}

func (c *Collector) collect(ctx context.Context, aggs []collectable) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// The aggregations take turns with one batch each, so that a busy one does not keep the
	// others waiting. A pass over an aggregation goes forward only, so that what keeps being
	// written at its start does not keep the pass from getting to its end.
	cursors := make([][]byte, len(aggs))
	done := make([]bool, len(aggs))
	var errs []error
	for remaining := len(aggs); remaining > 0; {
		for i, a := range aggs {
			if done[i] {
				continue
			}
			if err := ctx.Err(); err != nil {
				return errors.Join(append(errs, err)...)
			}

			next, err := a.collectBatch(c.db, cursors[i], c.batch, c.onError)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to collect %s: %w", a.collectName(), err))
			}
			if err != nil || next == nil {
				done[i] = true
				remaining--
			}
			cursors[i] = next
		}
	}
	return errors.Join(errs...)
}

// Run collects again and again, waiting for the interval between two rounds, until the context
// is done. It returns the error of the context.
func (c *Collector) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		if err := c.Collect(ctx); err != nil && ctx.Err() == nil {
			c.onError(err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Rebuild builds an aggregation again from the records of its store, which is how an
// aggregation that is added to a store with records in it gets to know of them.
// It must not run while records are written to the store.
func Rebuild[K, V, G, X, S any](ctx context.Context, db kv.DB, a *Aggregation[K, V, G, X, S], opts ...CollectorOption) error {
	c := NewCollector(db, a.ns.Schema(), opts...)

	// Everything that the aggregation has stored goes first.
	r := a.ns.Range()
	if err := kv.DeleteRange(ctx, db, r.Start, r.End); err != nil {
		return fmt.Errorf("failed to clear %s: %w", a.collectName(), err)
	}

	// The records are then noted as if they were new, a batch at a time.
	var last *K
	for more := true; more; {
		if err := ctx.Err(); err != nil {
			return err
		}

		// What a batch finds is only kept once its transaction is committed, as the function
		// may run more than once.
		var (
			batchLast *K
			batchSize int
		)
		err := db.Update(func(tx kv.Tx) error {
			batchLast, batchSize = last, 0

			var r kvutil.Range[K]
			if last != nil {
				r = r.After(*last)
			}
			it, err := a.store.Records(tx, r)
			if err != nil {
				return err
			}
			defer it.Close()

			m := &maintenance[K, V, G, X, S]{a: a, tx: a.ns.Open(tx)}
			for ; it.Valid() && batchSize < c.batch; it.Next() {
				batchSize++
				key, err := it.Key()
				if err != nil {
					return err
				}
				record, err := it.Value()
				if err != nil {
					return err
				}
				rawKey, err := a.store.KeyCodec().Encode(key)
				if err != nil {
					return err
				}
				if err := m.change(rawKey, nil, record); err != nil {
					return err
				}
				batchLast = &key
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to rebuild %s: %w", a.collectName(), err)
		}
		last, more = batchLast, batchSize == c.batch
	}

	return c.collect(ctx, []collectable{a})
}
