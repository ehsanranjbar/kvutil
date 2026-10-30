// Package kvsys is kvutil on Badger as a system of the benchmarks: databases in directories of a
// volume, opened with the durability of the run, and the retries of Update counted.
package kvsys

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/version"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
)

// Config is how the databases are opened.
type Config struct {
	// DataDir is the directory that the databases are made in; it should be on a volume.
	DataDir    string
	Durability harness.Durability
	// BlockCacheBytes is the block cache of Badger (a quarter of the memory limit, as the
	// shared buffers of PostgreSQL are); 0 is the default of Badger, 256 MiB.
	BlockCacheBytes int64
	// Prefetch is the number of values that the iterators read ahead; 0 reads none ahead.
	Prefetch int
}

// System is kvutil on Badger.
type System struct {
	cfg     Config
	retries atomic.Int64
	seq     atomic.Int64
}

// New creates the system, and clears the databases that an earlier run left in the directory.
func New(cfg Config) (*System, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("kvsys: no data directory")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "kvb-") {
			if err := os.RemoveAll(filepath.Join(cfg.DataDir, e.Name())); err != nil {
				return nil, err
			}
		}
	}
	return &System{cfg: cfg}, nil
}

// Name implements harness.System.
func (s *System) Name() string { return "kvutil" }

// Version implements harness.System.
func (s *System) Version() string {
	return fmt.Sprintf("kvutil %s (badger %s)", version.Kvutil(), version.Module("github.com/dgraph-io/badger/v4"))
}

// Settings implements harness.System.
func (s *System) Settings() map[string]string {
	o := s.options("x")
	return map[string]string{
		"sync_writes":      strconv.FormatBool(o.SyncWrites),
		"block_cache_size": strconv.FormatInt(o.BlockCacheSize, 10),
		"mem_table_size":   strconv.FormatInt(o.MemTableSize, 10),
		"compression":      compression(o.Compression),
		"compactors":       strconv.Itoa(o.NumCompactors),
		"prefetch":         strconv.Itoa(s.cfg.Prefetch),
		"max_retries":      strconv.Itoa(badgerkv.DefaultMaxRetries),
	}
}

// Close implements harness.System.
func (s *System) Close() error { return nil }

// Retries implements harness.RetryCounter: the times that a conflicting transaction was run again.
func (s *System) Retries() int64 { return s.retries.Load() }

func compression(c options.CompressionType) string {
	switch c {
	case options.None:
		return "none"
	case options.Snappy:
		return "snappy"
	case options.ZSTD:
		return "zstd"
	}
	return fmt.Sprint(int(c))
}

func (s *System) options(dir string) badger.Options {
	o := badger.DefaultOptions(dir).
		WithSyncWrites(s.cfg.Durability == harness.Durable).
		WithLogger(nil)
	if s.cfg.BlockCacheBytes > 0 {
		o = o.WithBlockCacheSize(s.cfg.BlockCacheBytes)
	}
	return o
}

// DB is a database of Badger in a directory of its own, which is removed when it is destroyed.
// It is a kv.DB, with the capabilities of the Badger one.
type DB struct {
	*badgerkv.DB
	sys  *System
	dir  string
	opts []badgerkv.Option
	tune func(badger.Options) badger.Options
}

// Open creates a new, empty database. The name only helps whoever looks at the directory.
func (s *System) Open(name string) (*DB, error) {
	return s.OpenPrefetch(name, s.cfg.Prefetch)
}

// OpenPrefetch is Open with the iterators reading the given number of values ahead instead of the
// number of the system: a workload that skips over keys wants none.
func (s *System) OpenPrefetch(name string, prefetch int) (*DB, error) {
	return s.OpenWith(name, prefetch, nil)
}

// Tuning changes the options of Badger that the system opens a database with, and the options of
// kvutil's adapter of it, for a workload that measures something the defaults hide (the garbage
// collection of the value log, the number of retries of a conflict). Either may be nil.
type Tuning struct {
	Badger func(badger.Options) badger.Options
	KV     []badgerkv.Option
}

// OpenWith is OpenPrefetch with the options tuned. The tuning is kept for Reopen.
func (s *System) OpenWith(name string, prefetch int, t *Tuning) (*DB, error) {
	dir := filepath.Join(s.cfg.DataDir, fmt.Sprintf("kvb-%s-%d", name, s.seq.Add(1)))
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	var opts []badgerkv.Option
	if prefetch > 0 {
		opts = append(opts, badgerkv.WithPrefetch(prefetch))
	}
	d := &DB{sys: s, dir: dir, opts: opts}
	if t != nil {
		d.tune = t.Badger
		d.opts = append(d.opts, t.KV...)
	}
	if err := d.open(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DB) open() error {
	o := d.sys.options(d.dir)
	if d.tune != nil {
		o = d.tune(o)
	}
	db, err := badgerkv.Open(o, d.opts...)
	if err != nil {
		return err
	}
	d.DB = db
	return nil
}

// Reopen closes the database and opens it again from its files, which empties the block cache of
// Badger (the pages that the operating system caches stay).
func (d *DB) Reopen() error {
	if err := d.Shutdown(); err != nil {
		return err
	}
	return d.Start()
}

// Shutdown closes the database, which flushes what its memtable holds; Start opens it again from
// its files. Reopen is the two together, for a workload that times them apart.
func (d *DB) Shutdown() error { return d.DB.Close() }

// Start opens the database again after Shutdown.
func (d *DB) Start() error { return d.open() }

// Flatten compacts the levels of the tree into the last one, which drops the versions and the
// keys that were deleted or have expired.
func (d *DB) Flatten(workers int) error {
	return d.Badger().Flatten(workers)
}

// Update implements kv.DB, and counts the times that the function is run again.
func (d *DB) Update(fn func(kv.Tx) error) error {
	calls := 0
	err := d.DB.Update(func(tx kv.Tx) error {
		calls++
		return fn(tx)
	})
	if calls > 1 {
		d.sys.retries.Add(int64(calls - 1))
	}
	return err
}

// Destroy closes the database and deletes its directory.
func (d *DB) Destroy() error {
	err := d.DB.Close()
	return errors.Join(err, os.RemoveAll(d.dir))
}

// Dir is the directory of the database.
func (d *DB) Dir() string { return d.dir }

// DiskBytes returns the size of the files of the database on disk: the blocks that they occupy,
// which is less than their length for the files that Badger sets the length of in advance (the
// write-ahead log of the memtable and the value log).
func (d *DB) DiskBytes() (int64, error) {
	var total int64
	err := filepath.WalkDir(d.dir, func(path string, e os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}
		info, err := e.Info()
		if err != nil {
			return err
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += int64(st.Blocks) * 512
		} else {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// Sync makes what has been written durable, so that the size on disk is that of the data.
func (d *DB) Sync() error {
	return d.Badger().Sync()
}
