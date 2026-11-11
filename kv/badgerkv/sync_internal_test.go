package badgerkv

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// roundErr is the error that a fake round returns to say which round it was, counting from 1
// in the order in which the rounds began.
type roundErr struct{ n int64 }

func (e roundErr) Error() string { return fmt.Sprintf("round %d", e.n) }

func roundOf(t *testing.T, err error) int64 {
	t.Helper()
	var re roundErr
	require.ErrorAs(t, err, &re)
	return re.n
}

func TestSyncerSharesRounds(t *testing.T) {
	var rounds atomic.Int64
	s := &syncer{sync: func(bool) error {
		rounds.Add(1)
		time.Sleep(2 * time.Millisecond)
		return nil
	}}

	var wg sync.WaitGroup
	errs := make([]error, 64)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = s.wait(false)
		}()
	}
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, rounds.Load(), int64(1))
	require.Less(t, rounds.Load(), int64(64))
	s.close()
}

// TestSyncerRoundBeginsAfterWait checks that no one is served by a round that began before it
// asked.
func TestSyncerRoundBeginsAfterWait(t *testing.T) {
	var starts atomic.Int64
	s := &syncer{sync: func(bool) error {
		n := starts.Add(1)
		time.Sleep(time.Millisecond)
		return roundErr{n}
	}}

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				before := starts.Load()
				err := s.wait(false)
				var re roundErr
				if assert.ErrorAs(t, err, &re) {
					assert.Greater(t, re.n, before)
				}
			}
		}()
	}
	wg.Wait()
	s.close()
}

// gatedRounds is a fake round that says when it has begun and then waits for the gate, so that
// a test can tell which round a caller joins.
type gatedRounds struct {
	starts  atomic.Int64
	started chan int64
	gate    chan struct{}

	mu    sync.Mutex
	lists []bool
}

func newGatedRounds() *gatedRounds {
	return &gatedRounds{started: make(chan int64, 16), gate: make(chan struct{})}
}

func (g *gatedRounds) syncer() *syncer {
	return &syncer{sync: g.round}
}

func (g *gatedRounds) round(list bool) error {
	g.mu.Lock()
	g.lists = append(g.lists, list)
	g.mu.Unlock()

	n := g.starts.Add(1)
	g.started <- n
	<-g.gate
	return roundErr{n}
}

func TestSyncerLateWaiterWaitsForTheNextRound(t *testing.T) {
	g := newGatedRounds()
	s := g.syncer()

	first, err := s.join(false)
	require.NoError(t, err)
	require.Equal(t, int64(1), <-g.started)

	// The first round is running, so the caller is not covered by it.
	late, err := s.join(false)
	require.NoError(t, err)
	require.NotSame(t, first, late)

	close(g.gate)
	<-first.done
	<-late.done
	require.Equal(t, int64(1), roundOf(t, first.err))
	require.Equal(t, int64(2), roundOf(t, late.err))
	s.close()
}

// TestSyncerRoundError checks that the error of a round is the error of all the callers of it,
// and of none of the others.
func TestSyncerRoundError(t *testing.T) {
	boom := errors.New("boom")
	var (
		starts  atomic.Int64
		started = make(chan int64, 8)
		step    = make(chan struct{})
	)
	s := &syncer{sync: func(bool) error {
		n := starts.Add(1)
		started <- n
		<-step
		if n == 2 {
			return boom
		}
		return nil
	}}

	first, err := s.join(false)
	require.NoError(t, err)
	require.Equal(t, int64(1), <-started)

	// These come while the first round is running, so they all share the second.
	var failing []*syncRound
	for range 4 {
		r, err := s.join(false)
		require.NoError(t, err)
		failing = append(failing, r)
	}
	for _, r := range failing {
		require.Same(t, failing[0], r)
	}
	step <- struct{}{}
	<-first.done
	require.NoError(t, first.err)

	// This one comes while the second is running, so it is in the third.
	require.Equal(t, int64(2), <-started)
	after, err := s.join(false)
	require.NoError(t, err)
	require.NotSame(t, failing[0], after)

	step <- struct{}{}
	for _, r := range failing {
		<-r.done
		require.ErrorIs(t, r.err, boom)
	}

	require.Equal(t, int64(3), <-started)
	step <- struct{}{}
	<-after.done
	require.NoError(t, after.err)
	s.close()
}

func TestSyncerListIsOredIntoTheRound(t *testing.T) {
	g := newGatedRounds()
	s := g.syncer()

	first, err := s.join(false)
	require.NoError(t, err)
	require.Equal(t, int64(1), <-g.started)

	// These share the second round, in whichever order they come.
	a, err := s.join(false)
	require.NoError(t, err)
	b, err := s.join(true)
	require.NoError(t, err)
	c, err := s.join(false)
	require.NoError(t, err)
	require.Same(t, a, b)
	require.Same(t, b, c)
	require.True(t, a.list)
	require.False(t, first.list)

	close(g.gate)
	<-a.done
	g.mu.Lock()
	require.Equal(t, []bool{false, true}, g.lists)
	g.mu.Unlock()
	s.close()
}

func TestSyncerClose(t *testing.T) {
	g := newGatedRounds()
	s := g.syncer()

	running, err := s.join(false)
	require.NoError(t, err)
	require.Equal(t, int64(1), <-g.started)
	pending, err := s.join(false)
	require.NoError(t, err)

	closed := make(chan struct{})
	go func() {
		s.close()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closed
	}, 5*time.Second, time.Millisecond)

	// Closing waits for the round that is running, and the one that the waiters are in.
	select {
	case <-closed:
		t.Fatal("close returned while a round was running")
	case <-time.After(20 * time.Millisecond):
	}
	require.ErrorIs(t, s.wait(false), badger.ErrDBClosed)

	close(g.gate)
	<-closed
	<-running.done
	<-pending.done
	require.Equal(t, int64(1), roundOf(t, running.err))
	require.Equal(t, int64(2), roundOf(t, pending.err))

	// It can be closed again, and there is no waiting after it.
	s.close()
	require.ErrorIs(t, s.wait(true), badger.ErrDBClosed)
}

func TestOpenSetsUpGroupCommit(t *testing.T) {
	open := func(t *testing.T, opt badger.Options, opts ...Option) *DB {
		t.Helper()
		db, err := Open(opt, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	// The writes are synced together instead of by badger.
	db := open(t, onDisk(t).WithSyncWrites(true))
	require.False(t, db.Badger().Opts().SyncWrites)
	require.NotNil(t, db.group)

	db = open(t, onDisk(t).WithSyncWrites(true), WithoutGroupCommit())
	require.True(t, db.Badger().Opts().SyncWrites)
	require.Nil(t, db.group)

	// There is nothing to sync, or nothing that is asked for.
	db = open(t, onDisk(t).WithSyncWrites(false))
	require.False(t, db.Badger().Opts().SyncWrites)
	require.Nil(t, db.group)

	db = open(t, badger.DefaultOptions("").WithInMemory(true).WithSyncWrites(true).WithLoggingLevel(badger.ERROR))
	require.Nil(t, db.group)

	opt := onDisk(t).WithSyncWrites(true)
	populated, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, populated.Update(func(tx kv.Tx) error { return tx.Set([]byte("k"), []byte("v")) }))
	require.NoError(t, populated.Close())
	db = open(t, opt.WithReadOnly(true))
	require.Nil(t, db.group)

	// A database that is open already is left as it is.
	base, err := badger.Open(onDisk(t).WithSyncWrites(true))
	require.NoError(t, err)
	wrapped := Wrap(base)
	t.Cleanup(func() { _ = wrapped.Close() })
	require.True(t, wrapped.Badger().Opts().SyncWrites)
	require.Nil(t, wrapped.group)
	require.NoError(t, wrapped.Update(func(tx kv.Tx) error { return tx.Set([]byte("k"), []byte("v")) }))
}

// roundLog counts the rounds that a DB runs, and what each is asked for.
type roundLog struct {
	mu    sync.Mutex
	lists []bool
}

func countRounds(d *DB) *roundLog {
	l := &roundLog{}
	inner := d.group.sync
	d.group.sync = func(list bool) error {
		l.mu.Lock()
		l.lists = append(l.lists, list)
		l.mu.Unlock()
		return inner(list)
	}
	return l
}

func (l *roundLog) all() []bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]bool(nil), l.lists...)
}

func TestGroupCommitOnDisk(t *testing.T) {
	opt := onDisk(t).WithSyncWrites(true)
	db, err := Open(opt)
	require.NoError(t, err)
	log := countRounds(db)

	const writers, each = 32, 20
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				key := fmt.Appendf(nil, "w%02d/%02d", w, i)
				if err := db.Update(func(tx kv.Tx) error { return tx.Set(key, key) }); err != nil {
					errs[w] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, errors.Join(errs...))
	require.NotEmpty(t, log.all())
	require.Less(t, len(log.all()), writers*each)

	require.NoError(t, db.View(func(tx kv.Tx) error {
		for w := range writers {
			for i := range each {
				key := fmt.Appendf(nil, "w%02d/%02d", w, i)
				got, err := tx.Get(key)
				require.NoError(t, err)
				require.Equal(t, key, got)
			}
		}
		return nil
	}))

	// What follows is one at a time, so every step is a round of its own or none.
	steps := func(fn func()) []bool {
		before := len(log.all())
		fn()
		return log.all()[before:]
	}

	require.Empty(t, steps(func() {
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			_, err := tx.Get([]byte("w00/00"))
			return err
		}))
	}), "a transaction that only reads does not wait")

	require.Empty(t, steps(func() {
		boom := errors.New("boom")
		err := db.Update(func(tx kv.Tx) error {
			require.NoError(t, tx.Set([]byte("lost"), []byte("v")))
			return boom
		})
		require.ErrorIs(t, err, boom)
	}), "a transaction that is rolled back does not wait")

	require.Equal(t, []bool{false}, steps(func() {
		require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Set([]byte("one"), []byte("v")) }))
	}))
	require.Equal(t, []bool{false}, steps(func() {
		require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Delete([]byte("one")) }))
	}))
	require.Equal(t, []bool{false}, steps(func() {
		require.NoError(t, db.Update(func(tx kv.Tx) error {
			return tx.(kv.TTLWriter).SetWithTTL([]byte("ttl"), []byte("v"), time.Hour)
		}))
	}))

	require.Equal(t, []bool{true}, steps(func() {
		w := db.NewBulkWriter()
		require.NoError(t, w.Set([]byte("bulk/1"), []byte("v")))
		require.NoError(t, w.Set([]byte("bulk/2"), []byte("v")))
		require.NoError(t, w.Flush())
	}), "a bulk write lists the directories")
	require.Empty(t, steps(func() {
		w := db.NewBulkWriter()
		require.NoError(t, w.Set([]byte("bulk/3"), []byte("v")))
		w.Cancel()
	}))

	// A range that is neither all of the keys nor a prefix is scanned and deleted.
	require.Equal(t, []bool{true}, steps(func() {
		require.NoError(t, db.DropRange([]byte("bulk/1"), []byte("bulk/2")))
	}))
	require.Equal(t, []bool{true}, steps(func() {
		require.NoError(t, db.DropRange([]byte("bulk/"), kv.PrefixEnd([]byte("bulk/"))))
	}))
	require.Equal(t, []bool{true}, steps(func() {
		require.NoError(t, db.DropRange(nil, nil))
	}))

	// What was written stays after the drop of everything, which restarts the value log.
	require.NoError(t, db.Update(func(tx kv.Tx) error { return tx.Set([]byte("after"), []byte("v")) }))
	require.NoError(t, db.Close())

	db, err = Open(opt)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(tx kv.Tx) error {
		got, err := tx.Get([]byte("after"))
		require.NoError(t, err)
		require.Equal(t, []byte("v"), got)
		_, err = tx.Get([]byte("w00/00"))
		require.ErrorIs(t, err, kv.ErrNotFound)
		return nil
	}))
}

// TestGroupCommitSyncsRotatedLogs checks that the files of the logs which rotate while the
// writes are being synced are synced, each once, and that the DB knows the logs it has.
func TestGroupCommitSyncsRotatedLogs(t *testing.T) {
	opt := onDisk(t).
		WithSyncWrites(true).
		WithMemTableSize(1 << 20).
		WithValueLogFileSize(1 << 20).
		WithValueThreshold(1 << 10)
	db, err := Open(opt, WithoutValueLogGC())
	require.NoError(t, err)
	defer db.Close()

	var mu sync.Mutex
	synced := map[string]int{}
	record := func(path string) error {
		err := syncFile(path)
		if err == nil {
			mu.Lock()
			synced[filepath.Base(path)]++
			mu.Unlock()
		}
		return err
	}
	db.mem.fsync, db.vlog.fsync = record, record
	firstMem, firstVlog := db.mem.last, db.vlog.last

	write := func(prefix string, count, size int) {
		const writers = 16
		value := bytes.Repeat([]byte("v"), size)
		var wg sync.WaitGroup
		errs := make([]error, writers)
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := w; i < count; i += writers {
					key := fmt.Appendf(nil, "%s%05d", prefix, i)
					if err := db.Update(func(tx kv.Tx) error { return tx.Set(key, value) }); err != nil {
						errs[w] = err
						return
					}
				}
			}()
		}
		wg.Wait()
		require.NoError(t, errors.Join(errs...))
	}
	// The values are under the threshold, so they are in the logs of the memtables, which the
	// batches of them fill; then the values of 4 KiB are in the value log.
	write("small", 8000, 1000)
	write("large", 1500, 4<<10)

	// A round after the last write finds the logs where they are.
	require.NoError(t, db.group.wait(false))

	mu.Lock()
	defer mu.Unlock()
	var mems, vlogs int
	for name, count := range synced {
		require.Equal(t, 1, count, "%s was synced more than once", name)
		switch filepath.Ext(name) {
		case ".mem":
			mems++
		case ".vlog":
			vlogs++
		}
	}
	require.Positive(t, mems, "no log of a memtable was synced: %v", synced)
	require.Positive(t, vlogs, "no file of the value log was synced: %v", synced)

	mem, _, err := db.mem.largest()
	require.NoError(t, err)
	vlog, _, err := db.vlog.largest()
	require.NoError(t, err)
	require.Greater(t, mem, firstMem)
	require.Greater(t, vlog, firstVlog)
	require.Equal(t, mem, db.mem.last)
	require.Equal(t, vlog, db.vlog.last)
	// Only the logs that are rotated are synced by file, never the current ones.
	require.NotContains(t, synced, filepath.Base(db.mem.path(mem)))
	require.NotContains(t, synced, filepath.Base(db.vlog.path(vlog)))
}

// logDir is a directory of fake logs, with the files that a test makes in it and the files that
// were synced.
type logDir struct {
	t      *testing.T
	l      *logFiles
	synced []uint32
}

func newLogDir(t *testing.T, last uint32, window int) *logDir {
	d := &logDir{t: t}
	d.l = &logFiles{dir: t.TempDir(), ext: ".mem", width: 5, window: window, last: last}
	d.l.fsync = func(path string) error {
		err := syncFile(path)
		if err == nil {
			digits := strings.TrimSuffix(filepath.Base(path), ".mem")
			id, perr := strconv.ParseUint(digits, 10, 32)
			require.NoError(t, perr)
			d.synced = append(d.synced, uint32(id))
		}
		return err
	}
	return d
}

func (d *logDir) create(ids ...uint32) {
	d.t.Helper()
	for _, id := range ids {
		require.NoError(d.t, os.WriteFile(d.l.path(id), nil, 0o600))
	}
}

func (d *logDir) remove(ids ...uint32) {
	d.t.Helper()
	for _, id := range ids {
		require.NoError(d.t, os.Remove(d.l.path(id)))
	}
}

// round runs a round and returns the ids that it synced.
func (d *logDir) round(list bool) ([]uint32, error) {
	d.synced = nil
	err := d.l.syncRotated(list)
	return d.synced, err
}

func TestLogFilesSyncRotated(t *testing.T) {
	d := newLogDir(t, 3, 4)
	d.create(3)

	// Nothing rotated.
	synced, err := d.round(false)
	require.NoError(t, err)
	require.Empty(t, synced)
	require.Equal(t, uint32(3), d.l.last)

	// Two files were added: the one that was current is synced, and the new current is not.
	d.create(4, 5)
	synced, err = d.round(false)
	require.NoError(t, err)
	require.Equal(t, []uint32{3, 4}, synced)
	require.Equal(t, uint32(5), d.l.last)

	// There is a gap in the window, as a memtable that is flushed and gone.
	d.create(8, 9)
	synced, err = d.round(false)
	require.NoError(t, err)
	require.Equal(t, []uint32{5, 8}, synced)
	require.Equal(t, uint32(9), d.l.last)

	// Files that were created while the ids were probed are followed to their end.
	d.create(10, 11, 12, 13, 14, 15)
	synced, err = d.round(false)
	require.NoError(t, err)
	require.Equal(t, []uint32{9, 10, 11, 12, 13, 14}, synced)
	require.Equal(t, uint32(15), d.l.last)

	// The last file is gone, which can only be if the files beyond the window are not found.
	d.remove(15)
	d.create(40, 41)
	synced, err = d.round(false)
	require.NoError(t, err)
	require.Equal(t, []uint32{40}, synced)
	require.Equal(t, uint32(41), d.l.last)
}

func TestLogFilesSyncRotatedLists(t *testing.T) {
	d := newLogDir(t, 3, 4)
	d.create(3, 40)

	// The probing does not see a file that is far beyond the window...
	synced, err := d.round(false)
	require.NoError(t, err)
	require.Empty(t, synced)
	require.Equal(t, uint32(3), d.l.last)

	// ...and listing the directory does.
	synced, err = d.round(true)
	require.NoError(t, err)
	require.Equal(t, []uint32{3}, synced)
	require.Equal(t, uint32(40), d.l.last)

	// A listing of a directory with no files keeps the last id.
	d.remove(3, 40)
	synced, err = d.round(true)
	require.NoError(t, err)
	require.Empty(t, synced)
	require.Equal(t, uint32(40), d.l.last)

	// The ids start again after everything was dropped.
	d.create(1, 2)
	synced, err = d.round(true)
	require.NoError(t, err)
	require.Empty(t, synced)
	require.Equal(t, uint32(2), d.l.last)
}

func TestLogFilesSyncRotatedError(t *testing.T) {
	d := newLogDir(t, 3, 4)
	d.create(3, 4, 5)

	boom := errors.New("boom")
	inner := d.l.fsync
	d.l.fsync = func(path string) error {
		if strings.HasSuffix(path, "00004.mem") {
			return boom
		}
		return inner(path)
	}
	synced, err := d.round(false)
	require.ErrorIs(t, err, boom)
	require.Equal(t, []uint32{3}, synced)
	require.Equal(t, uint32(3), d.l.last, "the next round has to sync what this one did not")

	d.l.fsync = inner
	synced, err = d.round(false)
	require.NoError(t, err)
	require.Equal(t, []uint32{3, 4}, synced)
	require.Equal(t, uint32(5), d.l.last)

	// A directory that cannot be read is an error too.
	d.l.dir = filepath.Join(d.l.dir, "gone")
	_, err = d.round(true)
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Equal(t, uint32(5), d.l.last)
}

func TestLogFilesIgnoreOtherFiles(t *testing.T) {
	d := newLogDir(t, 1, 4)
	d.create(1, 2)
	for _, name := range []string{
		"abc.mem", "00009.mem.tmp", "readme", "-3.mem", "+30.mem", ".mem",
		"99999999999999.mem", "00050.vlog", "00060.sst",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(d.l.dir, name), nil, 0o600))
	}
	require.NoError(t, os.Mkdir(filepath.Join(d.l.dir, "70.mem.d"), 0o700))

	id, found, err := d.l.largest()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(2), id)

	synced, err := d.round(true)
	require.NoError(t, err)
	require.Equal(t, []uint32{1}, synced)
	require.Equal(t, uint32(2), d.l.last)
}

func TestSyncFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "00001.mem")
	require.ErrorIs(t, syncFile(path), fs.ErrNotExist)
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o600))
	require.NoError(t, syncFile(path))
}

func BenchmarkDurableUpdate(b *testing.B) {
	for _, writers := range []int{1, 4, 16} {
		for _, group := range []bool{true, false} {
			name := fmt.Sprintf("writers=%d/group=%t", writers, group)
			b.Run(name, func(b *testing.B) {
				opt := badger.DefaultOptions(b.TempDir()).WithLoggingLevel(badger.ERROR).WithSyncWrites(true)
				var opts []Option
				if !group {
					opts = append(opts, WithoutGroupCommit())
				}
				db, err := Open(opt, append(opts, WithoutValueLogGC())...)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				value := bytes.Repeat([]byte("v"), 100)
				var (
					wg       sync.WaitGroup
					keys     atomic.Int64
					firstErr atomic.Pointer[error]
				)
				b.ResetTimer()
				for w := range writers {
					// The writers share b.N, the first ones taking the rest of the division.
					n := b.N / writers
					if w < b.N%writers {
						n++
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range n {
							key := strconv.AppendInt([]byte("key"), keys.Add(1), 10)
							if err := db.Update(func(tx kv.Tx) error { return tx.Set(key, value) }); err != nil {
								firstErr.CompareAndSwap(nil, &err)
								return
							}
						}
					}()
				}
				wg.Wait()
				b.StopTimer()
				if err := firstErr.Load(); err != nil {
					b.Fatal(*err)
				}
			})
		}
	}
}
