package badgerkv

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	// DefaultGCInterval is the default time between two garbage collections of the value log.
	DefaultGCInterval = 5 * time.Minute
	// DefaultGCDiscardRatio is the default part of a file of the value log that must be
	// garbage for the file to be rewritten.
	DefaultGCDiscardRatio = 0.5
)

// WithValueLogGC sets how the DB collects the garbage of the value log of badger, which
// badger does not do by itself: every interval, the files of which the given part at least
// is garbage are rewritten. A DB that Open opens does it with the defaults, and a DB that
// wraps a database that is already open only does it with this option, as whoever opened the
// database may do it already.
func WithValueLogGC(interval time.Duration, discardRatio float64) Option {
	return func(o *options) {
		o.gc, o.gcInterval, o.gcRatio = true, interval, discardRatio
	}
}

// WithoutValueLogGC keeps the DB from collecting the garbage of the value log, for a program
// that does it itself, with RunValueLogGC or with the database of badger.
func WithoutValueLogGC() Option {
	return func(o *options) {
		o.gc = false
	}
}

// maintenance is what a DB runs in the background, and what it releases when it is closed.
type maintenance struct {
	stop     chan struct{}
	stopped  sync.WaitGroup
	closing  sync.Once
	gcRounds atomic.Int64

	mu        sync.Mutex
	sequences []*badger.Sequence
}

// start runs the background work of the DB.
func (d *DB) start() {
	d.m.stop = make(chan struct{})
	// A database that is kept in memory has no value log.
	if !d.opts.gc || d.opts.gcInterval <= 0 || d.db.Opts().InMemory {
		return
	}

	d.m.stopped.Add(1)
	go func() {
		defer d.m.stopped.Done()

		ticker := time.NewTicker(d.opts.gcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-d.m.stop:
				return
			case <-ticker.C:
				// What goes wrong with a round is tried again in the next one.
				_ = d.RunValueLogGC()
			}
		}
	}()
}

// RunValueLogGC collects the garbage of the value log now: it rewrites the files that have
// enough garbage in them, until there is none left. It is what the DB does every interval.
// It returns an error if the garbage could not be collected, which is the case while another
// collection is running.
func (d *DB) RunValueLogGC() error {
	d.m.gcRounds.Add(1)
	for {
		err := d.db.RunValueLogGC(d.opts.gcRatio)
		switch {
		case err == nil:
			// A file was rewritten, and there may be another one.
		case errors.Is(err, badger.ErrNoRewrite):
			return nil
		default:
			return err
		}
	}
}

// Sequence returns a sequence of badger, which hands out numbers that grow, a bandwidth of
// them at a time, and that are never handed out twice. The DB releases the sequence when it is
// closed, which gives back the numbers that were not used. SequenceID makes the ids of the
// records of a store of one.
func (d *DB) Sequence(key []byte, bandwidth uint64) (*badger.Sequence, error) {
	seq, err := d.db.GetSequence(key, bandwidth)
	if err != nil {
		return nil, err
	}

	d.m.mu.Lock()
	defer d.m.mu.Unlock()
	d.m.sequences = append(d.m.sequences, seq)
	return seq, nil
}

// shutdown stops the background work of the DB and releases what it holds, once.
func (d *DB) shutdown() (err error) {
	d.m.closing.Do(func() {
		close(d.m.stop)
		d.m.stopped.Wait()

		d.m.mu.Lock()
		defer d.m.mu.Unlock()
		for _, seq := range d.m.sequences {
			err = errors.Join(err, seq.Release())
		}
		d.m.sequences = nil
	})
	return err
}
