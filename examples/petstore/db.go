package main

import (
	"context"
	"fmt"
	"log"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil"
	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/schema"
	"github.com/ehsanranjbar/kvutil/store/record"
	"github.com/google/uuid"
)

// File is an image with its metadata. It has no codec of its own, so it is stored in the
// default format of the schema.
type File = record.Object[uuid.UUID, []byte]

// DB is the database of the pet store: the stores, what is derived from them, and what runs
// in the background.
type DB struct {
	*badgerkv.DB

	sequences []*badger.Sequence
	stop      context.CancelFunc
	stopped   chan struct{}

	categories *record.Store[int64, types.Category]
	pets       *record.Store[int64, types.PetRecord]
	images     *record.Store[uuid.UUID, File]

	// The pets of a category and the images of a pet. Deleting a category deletes its pets,
	// and deleting a pet deletes its images.
	categoryPets *relation.Association[int64, types.Category, int64, types.PetRecord]
	petImages    *relation.Association[int64, types.PetRecord, uuid.UUID, File]

	// The pets by their status.
	byStatus *index.FieldIndex[int64, types.PetRecord, types.Status]

	// The number of the pets of each category.
	perCategory *agg.Aggregation[int64, types.PetRecord, int64, int64, int64]
	// The number of the pets that were added in each day, week and month.
	added *agg.Aggregation[int64, types.PetRecord, agg.NoGroup, int64, int64]
}

// NewDB opens the database and defines everything that is in it.
func NewDB(opt badger.Options) (*DB, error) {
	base, err := badgerkv.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to open the database: %w", err)
	}
	db := &DB{DB: base}

	// The names of the namespaces are given short prefixes, which are kept in the database.
	names, err := schema.NewNameRegistry(base)
	if err != nil {
		return nil, fmt.Errorf("failed to load the names: %w", err)
	}
	sch := schema.New(schema.WithPrefixAllocator(names))

	// The ids are taken from sequences of Badger, which the general interface does not
	// have: the Badger database is always at hand for what is specific to it.
	sequences := sch.Namespace("sequences")
	sequences.Define(schema.Definition{Kind: "sequences", Keys: true})
	sequence := func(name string) (*badger.Sequence, error) {
		seq, err := base.Badger().GetSequence(append(sequences.Prefix(), name...), 10)
		if err != nil {
			return nil, fmt.Errorf("failed to get the sequence of %s: %w", name, err)
		}
		db.sequences = append(db.sequences, seq)
		return seq, nil
	}
	categoryIDs, err := sequence("categories")
	if err != nil {
		return nil, err
	}
	petIDs, err := sequence("pets")
	if err != nil {
		return nil, err
	}

	// The stores are given no codecs. The ids are encoded by their types, the messages of
	// protobuf encode themselves, and the files are in the default format.
	db.categories = record.New[int64, types.Category](sch.Namespace("categories")).
		WithIDFunc(badgerkv.SequenceID[int64, types.Category](categoryIDs))
	db.pets = record.New[int64, types.PetRecord](sch.Namespace("pets")).
		WithIDFunc(badgerkv.SequenceID[int64, types.PetRecord](petIDs))
	db.images = record.New[uuid.UUID, File](sch.Namespace("images")).
		WithIDFunc(func(*File) (uuid.UUID, error) { return uuid.New(), nil })

	db.categoryPets = relation.Associate(sch.Namespace("category_pets"), db.categories, db.pets).
		WithParentID(func(p *types.PetRecord) (int64, bool) { return p.CategoryId, true })
	db.petImages = relation.Associate(sch.Namespace("pet_images"), db.pets, db.images)

	db.byStatus = index.On(db.pets, "by_status", field.Path[types.PetRecord, types.Status]("Status"))

	// A count can take a pet back out, so it is kept by what the pets add and remove.
	db.perCategory = agg.Count(db.pets, "per_category",
		func(p *types.PetRecord) (int64, bool) { return p.CategoryId, true })

	// The pets of a day are counted from the pets of the day, and the weeks and the months
	// are put together from the days.
	db.added = agg.Recompute(db.pets, "added",
		func(*types.PetRecord) (agg.NoGroup, int64, bool) { return agg.NoGroup{}, 1, true },
		func(pets iterutil.AdvancedIterator[time.Time, int64]) (int64, error) {
			return pets.Aggregate(0, func(n int64, _ time.Time, one int64) int64 { return n + one })
		},
		agg.ByTime(func(p *types.PetRecord) time.Time { return time.Unix(p.CreatedAt, 0) },
			agg.Daily, agg.Weekly, agg.Monthly),
		agg.WithMerge(func(a, b int64) int64 { return a + b }),
		agg.Describe("count", "", "CreatedAt"))

	// The definitions are checked against the ones that the database was written with.
	if err := sch.Check(base); err != nil {
		return nil, fmt.Errorf("the database does not fit its definitions: %w", err)
	}

	ctx, stop := context.WithCancel(context.Background())
	db.stop, db.stopped = stop, make(chan struct{}, 2)

	// The collector folds what the writes left for the aggregations into their states.
	// Reading an aggregation does not wait for it.
	collector := agg.NewCollector(base, sch,
		agg.WithInterval(time.Second),
		agg.WithErrorHandler(func(err error) { log.Println("collector:", err) }))
	go func() {
		defer func() { db.stopped <- struct{}{} }()
		if err := collector.Run(ctx); err != nil && ctx.Err() == nil {
			log.Println("collector:", err)
		}
	}()

	// The garbage collection of the value log is specific to Badger too.
	go func() {
		defer func() { db.stopped <- struct{}{} }()

		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for base.Badger().RunValueLogGC(0.7) == nil {
				}
			}
		}
	}()

	return db, nil
}

// Close stops what runs in the background and closes the database.
func (db *DB) Close() error {
	db.stop()
	<-db.stopped
	<-db.stopped

	for _, seq := range db.sequences {
		if err := seq.Release(); err != nil {
			return err
		}
	}
	return db.DB.Close()
}

// CreateCategory creates a new category.
func (db *DB) CreateCategory(tx kv.Tx, name string) (int64, error) {
	cat := &types.Category{Name: name}
	err := db.categories.Instantiate(tx).Set(cat)
	return cat.Id, err
}

// GetCategory gets a category by id.
func (db *DB) GetCategory(tx kv.Tx, id int64) (*types.Category, error) {
	return db.categories.Instantiate(tx).Get(id)
}

// CategoryCount is a category with the number of the pets that it has.
type CategoryCount struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
	Pets int64  `json:"pets"`
}

// CountPetsByCategory returns the number of the pets of every category.
func (db *DB) CountPetsByCategory(tx kv.Tx) ([]CategoryCount, error) {
	it, err := db.categories.Instantiate(tx).Iterator(kvutil.All[int64]())
	if err != nil {
		return nil, err
	}

	counts := db.perCategory.Instantiate(tx)
	return iterutil.Of[int64, *types.Category](it).
		Map(func(id int64, cat *types.Category) (CategoryCount, error) {
			n, _, err := counts.Get(id)
			return CategoryCount{Id: id, Name: cat.Name, Pets: n}, err
		}).
		Values()
}

// CreatePet creates a new pet in a category.
func (db *DB) CreatePet(tx kv.Tx, catId int64, name string, status types.Status) (int64, error) {
	p := &types.PetRecord{
		CategoryId: catId,
		Name:       name,
		Status:     status,
		CreatedAt:  time.Now().Unix(),
	}
	err := db.pets.Instantiate(tx).Set(p)
	return p.Id, err
}

// GetPet gets a pet by id.
func (db *DB) GetPet(tx kv.Tx, id int64) (*types.PetRecord, error) {
	return db.pets.Instantiate(tx).Get(id)
}

// GetPetImageIds gets the ids of the images of a pet.
func (db *DB) GetPetImageIds(tx kv.Tx, petId int64) ([]uuid.UUID, error) {
	images, err := db.petImages.Instantiate(tx).Children(petId)
	if err != nil {
		return nil, err
	}
	return iterutil.Of(images).Keys()
}

// ListPets lists the pets, or the pets of a status, a page at a time.
func (db *DB) ListPets(tx kv.Tx, status *types.Status, limit, offset int) ([]*types.PetRecord, error) {
	var (
		pets iterutil.Iterator[int64, *types.PetRecord]
		err  error
	)
	if status != nil {
		pets, err = db.byStatus.Instantiate(tx).Match(*status)
	} else {
		pets, err = db.pets.Instantiate(tx).Iterator(kvutil.All[int64]())
	}
	if err != nil {
		return nil, err
	}

	skipped := 0
	return iterutil.Of(pets).
		Filter(func(int64, *types.PetRecord) bool {
			skipped++
			return skipped > offset
		}).
		Limit(limit).
		Values()
}

// DeletePet deletes a pet by id, with its images.
func (db *DB) DeletePet(tx kv.Tx, id int64) error {
	return db.pets.Instantiate(tx).Delete(id)
}

// PetsAdded returns the number of the pets that were added in each of the days, the weeks or
// the months of a time range.
func (db *DB) PetsAdded(tx kv.Tx, bucket agg.Bucket, from, to time.Time) ([]agg.Point[int64], error) {
	return db.added.Instantiate(tx).Series(bucket, agg.NoGroup{}, from, to, agg.FillGaps())
}

// CreateFile creates a new image of a pet.
func (db *DB) CreateFile(tx kv.Tx, petId int64, data []byte, metadata string) (uuid.UUID, error) {
	f := &File{
		Data:     data,
		Metadata: map[string]any{"additionalMetadata": metadata},
	}
	// The pet that the image belongs to is told to the relation.
	err := db.images.Instantiate(tx).Set(f, db.petImages.Under(petId))
	if err != nil {
		return uuid.Nil, err
	}
	return f.Id, nil
}

// GetFile gets an image by id.
func (db *DB) GetFile(tx kv.Tx, id uuid.UUID) ([]byte, error) {
	f, err := db.images.Instantiate(tx).Get(id)
	if err != nil {
		return nil, err
	}
	return f.Data, nil
}

// DeleteFile deletes an image by id.
func (db *DB) DeleteFile(tx kv.Tx, id uuid.UUID) error {
	return db.images.Instantiate(tx).Delete(id)
}
