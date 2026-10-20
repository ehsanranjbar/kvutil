package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"time"
	"uuid"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/agg"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/field"
	"github.com/ehsanranjbar/kvutil/index"
	"github.com/ehsanranjbar/kvutil/iterutil"
	"github.com/ehsanranjbar/kvutil/keyspace"
	"github.com/ehsanranjbar/kvutil/kv"
	"github.com/ehsanranjbar/kvutil/kv/badgerkv"
	"github.com/ehsanranjbar/kvutil/migrate"
	"github.com/ehsanranjbar/kvutil/relation"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// File is an image with its metadata. It has no codec of its own, so it is stored in the
// default format of its type.
type File = record.Object[uuid.UUID, []byte]

// DB is the database of the pet store: the stores, what is derived from them, and what runs
// in the background.
type DB struct {
	*badgerkv.DB

	// Migrated is what was done to the database when it was opened.
	Migrated *migrate.Report

	// The collector folds what the writes left for the aggregations into their states.
	collector *agg.Collector

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
	// The DB looks after Badger: it collects the garbage of the value log in the background,
	// and releases the sequences that it gives out when it is closed.
	base, err := badgerkv.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to open the database: %w", err)
	}
	db, err := define(base)
	if err != nil {
		return nil, errors.Join(err, base.Close())
	}
	return db, nil
}

// define defines everything that is in the database, brings the database to it, and starts
// what runs in the background.
func define(base *badgerkv.DB) (*DB, error) {
	db := &DB{DB: base}

	// The stores are given long names, and the keyspace gives each of them a short prefix, which
	// is kept in the database.
	ks, err := keyspace.Open(base)
	if err != nil {
		return nil, fmt.Errorf("failed to load the names: %w", err)
	}

	// The ids are taken from sequences of Badger, which the general interface does not have.
	// They are raw keys under a prefix of their own, which no store is defined at.
	sequences := ks.Prefix("sequences")
	sequence := func(name string) (*badger.Sequence, error) {
		seq, err := base.Sequence(append(bytes.Clone(sequences), name...), 10)
		if err != nil {
			return nil, fmt.Errorf("failed to get the sequence of %s: %w", name, err)
		}
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
	db.categories = record.New[int64, types.Category](ks.Prefix("categories")).
		WithIDFunc(badgerkv.SequenceID[int64, types.Category](categoryIDs))
	// The pets are of the second version: see migratePets.
	db.pets = record.New[int64, types.PetRecord](ks.Prefix("pets")).
		WithVersion(2).
		WithIDFunc(badgerkv.SequenceID[int64, types.PetRecord](petIDs))
	migratePets(db.pets)
	db.images = record.New[uuid.UUID, File](ks.Prefix("images")).
		WithIDFunc(func(*File) (uuid.UUID, error) { return uuid.New(), nil })

	db.categoryPets = relation.Associate(ks.Prefix("category_pets"), db.categories, db.pets).
		WithParentID(func(p *types.PetRecord) (int64, bool) { return p.CategoryId, true })
	db.petImages = relation.Associate(ks.Prefix("pet_images"), db.pets, db.images)

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

	// The database is brought to these definitions before anything uses it: the pets of an
	// older version are migrated, and an index or an aggregation that the database has not
	// seen is built from the pets that are there.
	db.Migrated, err = ks.Migrate(context.Background(),
		db.categories, db.pets, db.images, db.categoryPets, db.petImages)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate the database: %w", err)
	}
	if report := db.Migrated.String(); report != "" {
		log.Println("migrated the database:\n" + report)
	}

	// Reading an aggregation does not wait for the collector, which runs in the background.
	db.collector = agg.NewCollector(base, []agg.Source{db.pets},
		agg.WithInterval(time.Second),
		agg.WithErrorHandler(func(err error) { log.Println("collector:", err) }))
	db.collector.Spawn(context.Background())

	return db, nil
}

// closeTimeout is how long Close waits for the collector to finish the batch that it is in.
const closeTimeout = 10 * time.Second

// Close stops the collector, after the batch that it is in, and closes the database.
func (db *DB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()
	err := db.collector.Shutdown(ctx)
	return errors.Join(err, db.DB.Close())
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
	it, err := db.categories.Instantiate(tx).All()
	if err != nil {
		return nil, err
	}

	counts := db.perCategory.Instantiate(tx)
	return iterutil.Of(it).
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
		pets, err = db.pets.Instantiate(tx).All()
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
		return uuid.Nil(), err
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
