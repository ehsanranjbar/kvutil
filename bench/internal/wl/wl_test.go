package wl

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/ehsanranjbar/kvutil/bench/internal/dataset"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"

	badger "github.com/dgraph-io/badger/v4"
)

func TestSplitCoversTheRange(t *testing.T) {
	for _, tc := range []struct {
		from, to int64
		n        int
	}{{0, 10, 3}, {1, 101, 4}, {5, 6, 4}, {0, 0, 2}, {7, 1000, 1}, {0, 7, 0}} {
		next := tc.from
		for _, r := range Split(tc.from, tc.to, tc.n) {
			if r[0] != next || r[1] <= r[0] {
				t.Fatalf("%+v: bad part %v after %d", tc, r, next)
			}
			next = r[1]
		}
		if next != tc.to {
			t.Errorf("%+v: the parts end at %d", tc, next)
		}
	}
}

func TestCursorHandsOutEachIDOnce(t *testing.T) {
	c := NewCursor(100, 1100)
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				id, ok := c.Take()
				if !ok {
					return
				}
				mu.Lock()
				seen[id]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != 1000 {
		t.Fatalf("%d ids taken", len(seen))
	}
	for id, n := range seen {
		if id < 100 || id >= 1100 || n != 1 {
			t.Fatalf("id %d taken %d times", id, n)
		}
	}
}

func TestKey8SortsLikeTheIDs(t *testing.T) {
	ids := []int64{0, 1, 255, 256, 65536, 1 << 40}
	for i := 1; i < len(ids); i++ {
		if bytes.Compare(Key8(ids[i-1]), Key8(ids[i])) >= 0 {
			t.Errorf("Key8(%d) is not before Key8(%d)", ids[i-1], ids[i])
		}
	}
}

func TestMsgpackRoundTrip(t *testing.T) {
	o := dataset.Spec{Seed: 1, Scale: 100, Values: dataset.Large}.Gen(9, 2)
	got, err := DecodeMsgpack(Msgpack(o), 9)
	if err != nil {
		t.Fatal(err)
	}
	if dataset.Canon(got) != dataset.Canon(o) {
		t.Errorf("the record changed:\n%s\n%s", dataset.Canon(got), dataset.Canon(o))
	}
}

func openBadger(t *testing.T) kv.DB {
	t.Helper()
	db, err := badgerkv.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func count(t *testing.T, db kv.DB) int {
	t.Helper()
	n := 0
	err := db.View(func(tx kv.Tx) error {
		it, err := tx.Iterator(nil, nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for ; it.Valid(); it.Next() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestLoaders(t *testing.T) {
	put := func() func(kv.BulkWriter, int64) error {
		return func(w kv.BulkWriter, id int64) error { return w.Set(Key8(id), []byte("v")) }
	}
	putTx := func() func(kv.Tx, int64) error {
		return func(tx kv.Tx, id int64) error { return tx.Set(Key8(id), []byte("v")) }
	}

	db := openBadger(t)
	if n, err := BulkLoad(db, 0, 1000, 3, put); err != nil || n != 1000 {
		t.Fatalf("BulkLoad = %d, %v", n, err)
	}
	if got := count(t, db); got != 1000 {
		t.Errorf("BulkLoad wrote %d keys", got)
	}

	db = openBadger(t)
	if n, err := TxLoad(context.Background(), db, 10, 1010, 64, 4, putTx); err != nil || n != 1000 {
		t.Fatalf("TxLoad = %d, %v", n, err)
	}
	if got := count(t, db); got != 1000 {
		t.Errorf("TxLoad wrote %d keys", got)
	}

	mem := memkv.New()
	if _, err := BulkLoad(mem, 0, 10, 1, put); err == nil {
		t.Error("BulkLoad on an engine without bulk writers does not fail")
	}
}
