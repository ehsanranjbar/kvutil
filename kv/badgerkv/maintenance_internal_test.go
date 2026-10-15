package badgerkv

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/stretchr/testify/require"
)

func onDisk(t *testing.T) badger.Options {
	return badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.ERROR)
}

// rounds returns the number of garbage collections that a DB has run a while after it was
// asked, so that a collection that was due has run.
func rounds(d *DB) int64 {
	time.Sleep(60 * time.Millisecond)
	return d.m.gcRounds.Load()
}

func TestValueLogGCRuns(t *testing.T) {
	// A DB that is opened collects the garbage, with the defaults unless it is told otherwise.
	db, err := Open(onDisk(t))
	require.NoError(t, err)
	require.True(t, db.opts.gc)
	require.Equal(t, DefaultGCInterval, db.opts.gcInterval)
	require.Equal(t, DefaultGCDiscardRatio, db.opts.gcRatio)
	require.Zero(t, rounds(db))
	require.NoError(t, db.Close())

	db, err = Open(onDisk(t), WithValueLogGC(5*time.Millisecond, 0.7))
	require.NoError(t, err)
	require.Equal(t, 0.7, db.opts.gcRatio)
	require.Eventually(t, func() bool { return db.m.gcRounds.Load() >= 3 }, 5*time.Second, time.Millisecond)

	// Closing it stops the collection before the database is closed, and can be done again.
	require.NoError(t, db.Close())
	stopped := db.m.gcRounds.Load()
	require.Equal(t, stopped, rounds(db))
	require.NoError(t, db.Close())

	// It does not when it is told not to, or when the database is kept in memory, which has
	// no value log.
	db, err = Open(onDisk(t), WithValueLogGC(5*time.Millisecond, 0.5), WithoutValueLogGC())
	require.NoError(t, err)
	require.Zero(t, rounds(db))
	require.NoError(t, db.Close())

	db, err = Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR),
		WithValueLogGC(5*time.Millisecond, 0.5))
	require.NoError(t, err)
	require.Zero(t, rounds(db))
	require.NoError(t, db.Close())

	// A database that someone else has opened is theirs to look after, unless they ask.
	base, err := badger.Open(onDisk(t))
	require.NoError(t, err)
	wrapped := Wrap(base)
	require.False(t, wrapped.opts.gc)
	require.Zero(t, rounds(wrapped))
	require.NoError(t, wrapped.Close())

	base, err = badger.Open(onDisk(t))
	require.NoError(t, err)
	wrapped = Wrap(base, WithValueLogGC(5*time.Millisecond, 0.5))
	require.Eventually(t, func() bool { return wrapped.m.gcRounds.Load() >= 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, wrapped.Close())
}

// TestRunValueLogGC checks a collection that is asked for: that it runs while the database is
// written to and read, and that a log without garbage to collect is not an error.
// What badger rewrites, and when, is its own business and is not checked here.
func TestRunValueLogGC(t *testing.T) {
	opt := onDisk(t).WithValueLogFileSize(1 << 20).WithValueThreshold(64)
	db, err := Open(opt, WithoutValueLogGC())
	require.NoError(t, err)
	defer db.Close()

	value := bytes.Repeat([]byte("v"), 64<<10)
	for i := range 40 {
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return tx.Set(fmt.Appendf(nil, "key%04d", i), value)
		}))
		if i%10 == 0 {
			require.NoError(t, db.RunValueLogGC())
		}
	}
	require.NoError(t, db.RunValueLogGC())
	require.Equal(t, int64(5), db.m.gcRounds.Load())

	require.NoError(t, db.View(func(tx kv.Tx) error {
		got, err := tx.Get([]byte("key0039"))
		require.NoError(t, err)
		require.Equal(t, value, got)
		return nil
	}))

	// A database that is kept in memory has no log to collect.
	mem, err := Open(badger.DefaultOptions("").WithInMemory(true).WithLoggingLevel(badger.ERROR))
	require.NoError(t, err)
	defer mem.Close()
	require.Error(t, mem.RunValueLogGC())
}

func TestSequencesAreReleased(t *testing.T) {
	opt := onDisk(t)
	take := func(count int) (numbers []uint64) {
		db, err := Open(opt)
		require.NoError(t, err)
		// A sequence leases a hundred numbers at a time.
		seq, err := db.Sequence([]byte("ids"), 100)
		require.NoError(t, err)
		for range count {
			n, err := seq.Next()
			require.NoError(t, err)
			numbers = append(numbers, n)
		}
		require.NoError(t, db.Close())
		return numbers
	}

	// The numbers that were leased and not used are given back when the DB is closed, so the
	// next program goes on from the last one that was used.
	require.Equal(t, []uint64{0, 1, 2}, take(3))
	require.Equal(t, []uint64{3, 4}, take(2))
	require.Equal(t, []uint64{5}, take(1))
}
