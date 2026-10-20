package kvbasic

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/bench/internal/harness"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/kvsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/systems/pgsys"
	"github.com/ehsanranjbar/kvutil/bench/internal/wl"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/store/typed"
	"github.com/jackc/pgx/v5"
)

// store is a key-value store of a system, with the operations that the cases run. The same
// values (the records encoded by the default codec of kvutil) are in all of them.
type store interface {
	// Load writes the records [from, to).
	Load(ctx context.Context, spec dataset.Spec, from, to int64, workers int) error
	// Prepare makes of a record what Set takes, outside of the measured time.
	Prepare(o *dataset.Order) any
	Get(ctx context.Context, id int64) (found bool, err error)
	Set(ctx context.Context, id int64, prepared any) error
	Delete(ctx context.Context, id int64) error
	// Scan reads up to limit values from the id on, in the order of the ids, and returns how
	// many it read.
	Scan(ctx context.Context, from int64, limit int) (rows int, err error)
	// Dump is Scan, returning for each record a line that says its id and a digest of its value.
	Dump(ctx context.Context, from int64, limit int) ([]string, error)
	// Settle brings the database to rest after a load.
	Settle(ctx context.Context) error
	Sizes(ctx context.Context) (harness.Sizes, error)
	Close(ctx context.Context) error
}

func digest(id int64, value []byte) string {
	sum := sha1.Sum(value)
	return fmt.Sprintf("%d %s", id, hex.EncodeToString(sum[:6]))
}

// base is what the stores of kvutil share.
type base struct {
	db *kvsys.DB
}

func (b *base) Settle(context.Context) error { return b.db.Sync() }

func (b *base) Sizes(context.Context) (harness.Sizes, error) {
	n, err := b.db.DiskBytes()
	return harness.Sizes{Data: n}, err
}

func (b *base) Close(context.Context) error { return b.db.Destroy() }

// --- typed: typed.Store[int64, Order], which encodes the key and the record with msgpack.

type typedStore struct {
	base
	st *typed.Store[int64, dataset.Order]
}

func openTyped(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	db, err := sys.(*kvsys.System).Open("kvbasic-typed")
	if err != nil {
		return nil, err
	}
	st := typed.New[int64, dataset.Order]("kv")
	if _, err := st.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return &typedStore{base{db}, st}, nil
}

func (s *typedStore) Load(_ context.Context, spec dataset.Spec, from, to int64, workers int) error {
	_, err := wl.BulkLoad(s.db, from, to, workers, func() func(kv.BulkWriter, int64) error {
		o := &dataset.Order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			k, err := s.st.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			v, err := s.st.ValueCodec().Encode(*o)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(s.st.DataPrefix()), k...), v)
		}
	})
	return err
}

func (s *typedStore) Prepare(o *dataset.Order) any { return o }

func (s *typedStore) Get(_ context.Context, id int64) (bool, error) {
	found := true
	err := s.db.View(func(tx kv.Tx) error {
		_, err := s.st.Instantiate(tx).Get(id)
		if errors.Is(err, kv.ErrNotFound) {
			found = false
			return nil
		}
		return err
	})
	return found, err
}

func (s *typedStore) Set(_ context.Context, id int64, p any) error {
	return s.db.Update(func(tx kv.Tx) error { return s.st.Instantiate(tx).Set(id, p.(*dataset.Order)) })
}

func (s *typedStore) Delete(_ context.Context, id int64) error {
	return s.db.Update(func(tx kv.Tx) error { return s.st.Instantiate(tx).Delete(id) })
}

func (s *typedStore) Scan(_ context.Context, from int64, limit int) (int, error) {
	rows := 0
	err := s.db.View(func(tx kv.Tx) error {
		rows = 0
		it, err := s.st.Instantiate(tx).Iterator(kvutil.From(from))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid() && rows < limit; it.Next() {
			if _, err := it.Value(); err != nil {
				return err
			}
			rows++
		}
		return nil
	})
	return rows, err
}

func (s *typedStore) Dump(_ context.Context, from int64, limit int) ([]string, error) {
	var lines []string
	err := s.db.View(func(tx kv.Tx) error {
		lines = lines[:0]
		it, err := s.st.Instantiate(tx).Iterator(kvutil.From(from))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid() && len(lines) < limit; it.Next() {
			id, err := it.Key()
			if err != nil {
				return err
			}
			o, err := it.Value()
			if err != nil {
				return err
			}
			lines = append(lines, digest(id, wl.Msgpack(o)))
		}
		return nil
	})
	return lines, err
}

// --- typed-bytes: typed.Store[int64, blob], whose values are bytes that encode themselves, so
// that the difference with raw is the typed layer and nothing of msgpack.

type blob []byte

func (b *blob) MarshalBinary() ([]byte, error) { return bytes.Clone(*b), nil }

func (b *blob) UnmarshalBinary(bz []byte) error {
	*b = bytes.Clone(bz)
	return nil
}

type blobStore struct {
	base
	st *typed.Store[int64, blob]
}

func openBlob(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	db, err := sys.(*kvsys.System).Open("kvbasic-typed-bytes")
	if err != nil {
		return nil, err
	}
	st := typed.New[int64, blob]("kv")
	if _, err := st.Migrate(ctx, db); err != nil {
		db.Destroy()
		return nil, err
	}
	return &blobStore{base{db}, st}, nil
}

func (s *blobStore) Load(_ context.Context, spec dataset.Spec, from, to int64, workers int) error {
	_, err := wl.BulkLoad(s.db, from, to, workers, func() func(kv.BulkWriter, int64) error {
		o := &dataset.Order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			k, err := s.st.KeyCodec().Encode(id)
			if err != nil {
				return err
			}
			return w.Set(append(bytes.Clone(s.st.DataPrefix()), k...), wl.Msgpack(o))
		}
	})
	return err
}

func (s *blobStore) Prepare(o *dataset.Order) any {
	b := blob(wl.Msgpack(o))
	return &b
}

func (s *blobStore) Get(_ context.Context, id int64) (bool, error) {
	found := true
	err := s.db.View(func(tx kv.Tx) error {
		_, err := s.st.Instantiate(tx).Get(id)
		if errors.Is(err, kv.ErrNotFound) {
			found = false
			return nil
		}
		return err
	})
	return found, err
}

func (s *blobStore) Set(_ context.Context, id int64, p any) error {
	return s.db.Update(func(tx kv.Tx) error { return s.st.Instantiate(tx).Set(id, p.(*blob)) })
}

func (s *blobStore) Delete(_ context.Context, id int64) error {
	return s.db.Update(func(tx kv.Tx) error { return s.st.Instantiate(tx).Delete(id) })
}

func (s *blobStore) Scan(_ context.Context, from int64, limit int) (int, error) {
	rows := 0
	err := s.db.View(func(tx kv.Tx) error {
		rows = 0
		it, err := s.st.Instantiate(tx).Iterator(kvutil.From(from))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid() && rows < limit; it.Next() {
			if _, err := it.Value(); err != nil {
				return err
			}
			rows++
		}
		return nil
	})
	return rows, err
}

func (s *blobStore) Dump(_ context.Context, from int64, limit int) ([]string, error) {
	var lines []string
	err := s.db.View(func(tx kv.Tx) error {
		lines = lines[:0]
		it, err := s.st.Instantiate(tx).Iterator(kvutil.From(from))
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid() && len(lines) < limit; it.Next() {
			id, err := it.Key()
			if err != nil {
				return err
			}
			b, err := it.Value()
			if err != nil {
				return err
			}
			lines = append(lines, digest(id, []byte(*b)))
		}
		return nil
	})
	return lines, err
}

// --- raw: kv.Tx, with the id as 8 bytes and the value as the bytes that it is given.

type rawStore struct {
	base
}

func openRaw(_ context.Context, sys harness.System, _ harness.Params) (store, error) {
	db, err := sys.(*kvsys.System).Open("kvbasic-raw")
	if err != nil {
		return nil, err
	}
	return &rawStore{base{db}}, nil
}

func (s *rawStore) Load(_ context.Context, spec dataset.Spec, from, to int64, workers int) error {
	_, err := wl.BulkLoad(s.db, from, to, workers, func() func(kv.BulkWriter, int64) error {
		o := &dataset.Order{}
		return func(w kv.BulkWriter, id int64) error {
			spec.GenInto(o, id, 0)
			return w.Set(wl.Key8(id), wl.Msgpack(o))
		}
	})
	return err
}

func (s *rawStore) Prepare(o *dataset.Order) any { return wl.Msgpack(o) }

func (s *rawStore) Get(_ context.Context, id int64) (bool, error) {
	found := true
	err := s.db.View(func(tx kv.Tx) error {
		_, err := tx.Get(wl.Key8(id))
		if errors.Is(err, kv.ErrNotFound) {
			found = false
			return nil
		}
		return err
	})
	return found, err
}

func (s *rawStore) Set(_ context.Context, id int64, p any) error {
	return s.db.Update(func(tx kv.Tx) error { return tx.Set(wl.Key8(id), p.([]byte)) })
}

func (s *rawStore) Delete(_ context.Context, id int64) error {
	return s.db.Update(func(tx kv.Tx) error { return tx.Delete(wl.Key8(id)) })
}

func (s *rawStore) Scan(_ context.Context, from int64, limit int) (int, error) {
	rows := 0
	err := s.db.View(func(tx kv.Tx) error {
		rows = 0
		it, err := tx.Iterator(wl.Key8(from), nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid() && rows < limit; it.Next() {
			if _, err := it.Value(); err != nil {
				return err
			}
			rows++
		}
		return nil
	})
	return rows, err
}

func (s *rawStore) Dump(_ context.Context, from int64, limit int) ([]string, error) {
	var lines []string
	err := s.db.View(func(tx kv.Tx) error {
		lines = lines[:0]
		it, err := tx.Iterator(wl.Key8(from), nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid() && len(lines) < limit; it.Next() {
			v, err := it.Value()
			if err != nil {
				return err
			}
			lines = append(lines, digest(int64(binary.BigEndian.Uint64(it.Key())), v))
		}
		return nil
	})
	return lines, err
}

// --- PostgreSQL: kv(k bigint PRIMARY KEY, v bytea).

type pgStore struct {
	sys *pgsys.System
	sc  *pgsys.Schema
	tbl string
}

func openPG(ctx context.Context, sys harness.System, _ harness.Params) (store, error) {
	ps := sys.(*pgsys.System)
	sc, err := ps.Schema(ctx, "kv-basic")
	if err != nil {
		return nil, err
	}
	tbl := sc.T("kv")
	if err := sc.Exec(ctx, "CREATE TABLE "+tbl+" (k bigint PRIMARY KEY, v bytea NOT NULL)"); err != nil {
		sc.Drop(ctx)
		return nil, err
	}
	return &pgStore{sys: ps, sc: sc, tbl: tbl}, nil
}

func (s *pgStore) Load(ctx context.Context, spec dataset.Spec, from, to int64, workers int) error {
	return wl.ParallelPG(from, to, workers, func(lo, hi int64) error {
		o := &dataset.Order{}
		id := lo
		_, err := s.sys.Pool().CopyFrom(ctx, pgx.Identifier{s.sc.Name, "kv"}, []string{"k", "v"},
			pgx.CopyFromFunc(func() ([]any, error) {
				if id >= hi {
					return nil, nil
				}
				spec.GenInto(o, id, 0)
				row := []any{id, wl.Msgpack(o)}
				id++
				return row, nil
			}))
		return err
	})
}

func (s *pgStore) Prepare(o *dataset.Order) any { return wl.Msgpack(o) }

func (s *pgStore) Get(ctx context.Context, id int64) (bool, error) {
	var v []byte
	err := s.sys.Pool().QueryRow(ctx, "SELECT v FROM "+s.tbl+" WHERE k = $1", id).Scan(&v)
	if pgsys.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func (s *pgStore) Set(ctx context.Context, id int64, p any) error {
	_, err := s.sys.Pool().Exec(ctx,
		"INSERT INTO "+s.tbl+" (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", id, p.([]byte))
	return err
}

func (s *pgStore) Delete(ctx context.Context, id int64) error {
	_, err := s.sys.Pool().Exec(ctx, "DELETE FROM "+s.tbl+" WHERE k = $1", id)
	return err
}

func (s *pgStore) Scan(ctx context.Context, from int64, limit int) (int, error) {
	rows, err := s.sys.Pool().Query(ctx, "SELECT k, v FROM "+s.tbl+" WHERE k >= $1 ORDER BY k LIMIT $2", from, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var k int64
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return n, err
		}
		n++
	}
	return n, rows.Err()
}

func (s *pgStore) Dump(ctx context.Context, from int64, limit int) ([]string, error) {
	rows, err := s.sys.Pool().Query(ctx, "SELECT k, v FROM "+s.tbl+" WHERE k >= $1 ORDER BY k LIMIT $2", from, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var k int64
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		lines = append(lines, digest(k, v))
	}
	return lines, rows.Err()
}

func (s *pgStore) Settle(ctx context.Context) error { return s.sc.Settle(ctx, "kv") }

func (s *pgStore) Sizes(ctx context.Context) (harness.Sizes, error) { return s.sc.Sizes(ctx) }

func (s *pgStore) Close(ctx context.Context) error { return s.sc.Drop(ctx) }
