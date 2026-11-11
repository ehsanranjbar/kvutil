package badgerkv

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

// Group commit
//
// With SyncWrites on, badger syncs the log of the memtable once for every request that it
// writes, even when the requests are in one batch, so concurrent writers wait for one sync each
// and do not get faster by being many. The DB opens badger with SyncWrites off instead, and
// every Update that wrote makes sure that a sync which began after its commit has finished. One
// sync then covers all the commits that waited for it.
//
// This is what badger does with SyncWrites off, as of v4.9.6, and what the design relies on:
//
//   - DB.Sync syncs the log of the current memtable (%05d.mem in Options.Dir) and the current
//     file of the value log (%06d.vlog in Options.ValueDir), and nothing else.
//   - When a memtable is full, it is replaced by a new one and its log is not synced. That log is
//     synced when the memtable is flushed to a table, which badger syncs, and the log is deleted
//     after that. A log that is missing has therefore been flushed and needs nothing.
//   - The ids of the memtables are sequential and they are flushed in the order of their ids. The
//     file of the current memtable is never deleted, but the file of a flushed one may stay for a
//     while, as long as a reader holds on to the memtable. The ids that exist are some of those
//     flushed memtables, and then a run of ids with no gap up to the current one.
//   - The ids of the files of the value log are sequential too (the greatest plus one), and only
//     the garbage collection of the value log deletes the old ones. Badger mostly syncs a file
//     when it rotates it, but not on every path, so the DB syncs the rotated files as well.
//
// So a round of syncs is DB.Sync, and then a sync of each of the files that were current at
// some time since the previous round and are not the current one, which DB.Sync has synced.

// syncer runs rounds of syncs, one at a time, for the commits that wait for one.
type syncer struct {
	// sync runs one round. It is a field so that a test can count the rounds.
	sync func(list bool) error

	mu      sync.Mutex
	pending *syncRound // the round that has not started yet, which new waiters join
	running bool       // a goroutine is running rounds
	closed  bool
	done    sync.WaitGroup
}

// syncRound is one round of syncs, and what the waiters of it get from it.
type syncRound struct {
	list bool // the round lists the directories instead of probing them (see logFiles)
	done chan struct{}
	err  error
}

// wait returns after a round has finished that began after the call. Waiters that come while a
// round is running share the round that follows it. list asks for a round that lists the
// directories of the logs, which a write that rotates a log many times needs (see logFiles).
func (s *syncer) wait(list bool) error {
	r, err := s.join(list)
	if err != nil {
		return err
	}
	<-r.done
	return r.err
}

// join puts the caller in the round that has not started yet, and starts the loop if there is
// none. It returns the round, which is done when its done channel is closed.
func (s *syncer) join(list bool) (*syncRound, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, badger.ErrDBClosed
	}
	r := s.pending
	if r == nil {
		r = &syncRound{done: make(chan struct{})}
		s.pending = r
	}
	r.list = r.list || list
	if !s.running {
		s.running = true
		s.done.Add(1)
		go s.loop()
	}
	return r, nil
}

// loop runs the rounds, until no one waits for one. There is never more than one loop, so a
// round begins after the waiters of it have joined it, and rounds do not overlap.
func (s *syncer) loop() {
	defer s.done.Done()
	for {
		s.mu.Lock()
		r := s.pending
		s.pending = nil
		if r == nil {
			s.running = false
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		r.err = s.sync(r.list)
		close(r.done)
	}
}

// close lets the waiters that there are get their rounds, and then stops the syncer. A wait that
// comes after it fails. It can be called again.
func (s *syncer) close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.done.Wait()
}

// awaitSync waits for a round of the syncer, if there is one: a DB that has no syncer syncs in
// badger, or not at all.
func awaitSync(s *syncer, list bool) error {
	if s == nil {
		return nil
	}
	return mapError(s.wait(list))
}

// startGroupCommit makes the syncer of the DB, and finds the logs that it is to look after.
func (d *DB) startGroupCommit() error {
	opt := d.db.Opts()
	d.mem = logFiles{
		dir:    opt.Dir,
		ext:    ".mem",
		width:  5,
		window: opt.NumMemtables + 2,
		fsync:  syncFile,
	}
	d.vlog = logFiles{
		dir:    opt.ValueDir,
		ext:    ".vlog",
		width:  6,
		window: 2,
		fsync:  syncFile,
	}
	for _, l := range []*logFiles{&d.mem, &d.vlog} {
		id, _, err := l.largest()
		if err != nil {
			return err
		}
		l.last = id
	}
	d.group = &syncer{sync: d.syncRound}
	return nil
}

// syncRound syncs everything that was written before it began: the logs that are current, and
// those that were rotated since the previous round. The rounds run on one goroutine at a time,
// and the goroutines that run them are ordered by the mutex of the syncer, so the logs need no
// lock of their own.
func (d *DB) syncRound(list bool) error {
	// This syncs the memtable that is current when it runs, which is the one that any waiter has
	// written to or a later one, and so every file between the current one of the previous round
	// and the current one of now is one of three things: flushed, which badger synced; synced
	// here below; or the one that this call has synced.
	err := d.db.Sync()
	return errors.Join(err, d.mem.syncRotated(list), d.vlog.syncRotated(list))
}

// logFiles keeps track of the files of one of the logs of badger, to sync those that rotated
// between two rounds.
type logFiles struct {
	dir    string
	ext    string // ".mem" or ".vlog"
	width  int    // the digits of an id in the name of a file: 5 for ".mem", 6 for ".vlog"
	window int    // how many ids above the last one are probed: the memtables that there can be, plus 2, for ".mem", and 2 for ".vlog"
	last   uint32 // the id of the current file as of the previous round
	fsync  func(path string) error
}

// syncRotated syncs the files that were current at some time since the previous round, other
// than the one that is current now, which the sync of badger has done.
//
// Listing a directory costs 0.1 to 0.7 ms with the numbers of files that badger keeps, which is
// more than a round needs to spend, so the current id is found by looking for the ids above the
// last one instead. That can only miss the current file if more than the window of memtables
// were flushed between two rounds while a reader held the oldest of them, so that there is a gap
// wider than the window. Bulk writes and drops are the writes that can be that large, and they
// ask for a round that lists the directory.
func (l *logFiles) syncRotated(list bool) error {
	c, err := l.current(list)
	if err != nil {
		return err
	}

	// A file that is missing was flushed, or its log was collected, and needs nothing. The ids
	// can go down when everything was dropped, and then there is nothing to sync either.
	for id := l.last; id < c; id++ {
		if err := l.fsync(l.path(id)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	// Only when they all are synced, so that a round that failed is made up for by the next one.
	l.last = c
	return nil
}

// current returns the id of the current file of the log.
func (l *logFiles) current(list bool) (uint32, error) {
	if !list {
		// The current file is never deleted, so if the last one is, there are files that were
		// created and deleted since, and the probing would not be able to tell where they end.
		ok, err := l.exists(l.last)
		if err != nil {
			return 0, err
		}
		list = !ok
	}
	if list {
		id, found, err := l.largest()
		if err != nil || !found {
			return l.last, err
		}
		return id, nil
	}

	c := l.last
	for i := 1; i <= l.window; i++ {
		id := l.last + uint32(i)
		ok, err := l.exists(id)
		if err != nil {
			return 0, err
		}
		if ok {
			c = id
		}
	}
	// The files that there are form a run with no gap up to the current one, so what follows the
	// largest one that was found, if anything, is files that were created after the probing.
	for {
		ok, err := l.exists(c + 1)
		if err != nil {
			return 0, err
		}
		if !ok {
			return c, nil
		}
		c++
	}
}

func (l *logFiles) path(id uint32) string {
	return filepath.Join(l.dir, fmt.Sprintf("%0*d%s", l.width, id, l.ext))
}

func (l *logFiles) exists(id uint32) (bool, error) {
	_, err := os.Stat(l.path(id))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	}
	return false, err
}

// largest lists the directory and returns the greatest id that a file of the log has in it.
func (l *logFiles) largest() (id uint32, found bool, err error) {
	f, err := os.Open(l.dir)
	if err != nil {
		return 0, false, err
	}
	// Reading the names is cheaper than os.ReadDir, which also sorts them and looks at their types.
	names, err := f.Readdirnames(-1)
	err = errors.Join(err, f.Close())
	if err != nil {
		return 0, false, err
	}

	for _, name := range names {
		digits, ok := strings.CutSuffix(name, l.ext)
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(digits, 10, 32)
		if err != nil {
			continue
		}
		if !found || uint32(n) > id {
			id, found = uint32(n), true
		}
	}
	return id, found, nil
}

// syncFile makes the data of a file durable. A file that is open for reading is enough for that
// on Linux and macOS.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return errors.Join(fmt.Errorf("sync %s: %w", path, err), f.Close())
	}
	return f.Close()
}
