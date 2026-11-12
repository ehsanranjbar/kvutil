package main

import (
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/codec"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/keyspace"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/kv/memkv"
	"github.com/ehsanranjbar/kvutil/query"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// countingCodec counts how many pets are decoded in full and how many in part.
type countingCodec struct {
	codec.ValueCodec[types.PetRecord]
	full, part atomic.Int64
}

// Decode implements the ValueCodec interface.
func (c *countingCodec) Decode(bz []byte) (types.PetRecord, error) {
	c.full.Add(1)
	return c.ValueCodec.Decode(bz)
}

// PartialDecode implements the PartialDecoder interface.
func (c *countingCodec) PartialDecode(fields []string) (func([]byte, *types.PetRecord) error, bool) {
	decode, ok := codec.PartialOf(c.ValueCodec, fields)
	if !ok {
		return nil, false
	}
	return func(bz []byte, p *types.PetRecord) error {
		c.part.Add(1)
		return decode(bz, p)
	}, true
}

// TestPartialDecoding checks that a scan with a condition that no index serves decodes the
// field that the condition reads of every pet, and the pets that satisfy it in full.
func TestPartialDecoding(t *testing.T) {
	counting := &countingCodec{ValueCodec: types.Codec[types.PetRecord]()}
	pets := record.NewWithCodecs[int64, types.PetRecord]("pets", nil, codec.ValueCodec[types.PetRecord](counting))
	mem := memkv.New()
	if err := pets.Check(mem); err != nil {
		t.Fatal(err)
	}
	err := mem.Update(func(tx kv.Tx) error {
		ins := pets.Instantiate(tx)
		for id := int64(1); id <= 100; id++ {
			p := &types.PetRecord{Id: id, CategoryId: id % 5, Name: fmt.Sprint("pet", id), Status: types.Status_AVAILABLE}
			if err := ins.Set(p); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Nothing serves the category of a pet, so the pets are scanned.
	category := field.Path[types.PetRecord, int64]("CategoryId")
	q := query.From(pets).Where(category.Eq(3)).FullScan()

	var names []string
	err = mem.View(func(tx kv.Tx) error {
		var err error
		names, err = iterutil.Of(q.Run(tx).Iterator).
			Map(func(_ int64, p *types.PetRecord) (string, error) { return p.Name, nil }).
			Values()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 20 || names[0] != "pet3" || names[19] != "pet98" {
		t.Fatalf("got %v", names)
	}
	// The category is read of each of the 100 pets, and the 20 of the category are read in full.
	if part, full := counting.part.Load(), counting.full.Load(); part != 100 || full != 20 {
		t.Fatalf("got %d decoded in part and %d in full", part, full)
	}

	// A count decodes nothing in full.
	counting.part.Store(0)
	counting.full.Store(0)
	err = mem.View(func(tx kv.Tx) error {
		n, err := q.Count(tx)
		if n != 20 {
			t.Errorf("got %d pets", n)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if part, full := counting.part.Load(), counting.full.Load(); part != 100 || full != 0 {
		t.Fatalf("got %d decoded in part and %d in full", part, full)
	}
}

// TestPartialCodecs checks that the stores of the database have the partial decoding of their
// types, and that a query of the database gives the right pets through it.
func TestPartialCodecs(t *testing.T) {
	db, err := NewDB(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := newClient(t, db)
	c.addPet("rex", "AVAILABLE", map[string]any{"name": "dogs"})
	c.addPet("tom", "SOLD", map[string]any{"name": "cats"})
	c.addPet("fido", "SOLD", map[string]any{"id": 1})

	if db.pets.ValueCodec().Name() != "flatbuffers" {
		t.Fatalf("got the codec %q", db.pets.ValueCodec().Name())
	}
	decode, ok := codec.PartialOf(db.pets.ValueCodec(), []string{"Name"})
	if !ok {
		t.Fatal("the codec of the pets has no partial decoding")
	}
	if _, ok := codec.PartialOf(db.categories.ValueCodec(), []string{"Name"}); !ok {
		t.Fatal("the codec of the categories has no partial decoding")
	}

	// The name is not served by an index either.
	name := field.Path[types.PetRecord, string]("Name")
	err = db.View(func(tx kv.Tx) error {
		pr, err := db.GetPet(tx, 2)
		if err != nil {
			return err
		}
		bz, err := db.pets.ValueCodec().Encode(*pr)
		if err != nil {
			return err
		}
		var part types.PetRecord
		if err := decode(bz, &part); err != nil || part.Name != "tom" || part.CategoryId != 0 {
			t.Errorf("got %+v, %v", part, err)
		}

		found, err := iterutil.Of(query.From(db.pets).Where(name.Gte("r")).FullScan().Run(tx).Iterator).
			Map(func(_ int64, p *types.PetRecord) (string, error) { return p.Name, nil }).
			Values()
		slices.Sort(found)
		if err != nil || strings.Join(found, ",") != "rex,tom" {
			t.Errorf("got %v, %v", found, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestProtobufDatabase checks that a database of the version of the program that stored
// protobuf is not read as FlatBuffers: the codec of its pets has another name.
func TestProtobufDatabase(t *testing.T) {
	opt := badger.DefaultOptions(t.TempDir()).WithLogger(nil)

	base, err := badgerkv.Open(opt)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := keyspace.Open(base)
	if err != nil {
		t.Fatal(err)
	}
	// The codec that a type gets by its own MarshalBinary, as the protobuf messages did.
	pets := record.NewWithCodecs[int64, types.PetRecord](ks.Prefix("pets"), nil, codec.Binary[types.PetRecord]()).
		WithVersion(2)
	if _, err := ks.Migrate(t.Context(), pets); err != nil {
		t.Fatal(err)
	}
	err = base.Update(func(tx kv.Tx) error {
		return pets.Instantiate(tx).Set(&types.PetRecord{Id: 1, Name: "rex"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := NewDB(opt)
	if err == nil {
		db.Close()
		t.Fatal("the database was opened")
	}
	t.Log(err)
}
