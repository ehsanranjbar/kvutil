package main

import (
	"fmt"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/legacy"
	"github.com/ehsanranjbar/kvutil/legacy/iters"
	extstore "github.com/ehsanranjbar/kvutil/legacy/store/ext"
	pstore "github.com/ehsanranjbar/kvutil/legacy/store/prefix"
	recstore "github.com/ehsanranjbar/kvutil/legacy/store/rec"
	"github.com/ehsanranjbar/kvutil/legacy/store/rec/rectools"
	"github.com/google/uuid"
)

type File = recstore.Object[uuid.UUID, []byte]

// DB is a repository for pets
type DB struct {
	base     *badger.DB
	gcTicker *time.Ticker
	reg      *legacy.NameRegistry

	categoriesIdSeq   *badger.Sequence
	categoriesStore   *recstore.Store[int64, types.Category, *types.Category]
	petsIdSeq         *badger.Sequence
	petsStore         *recstore.Store[int64, types.PetRecord, *types.PetRecord]
	imagesStore       *recstore.Store[uuid.UUID, File, *File]
	categoriesPetsRel *rectools.Association[int64, types.Category, *types.Category, int64, types.PetRecord, *types.PetRecord]
	petsImagesRel     *rectools.Association[int64, types.PetRecord, *types.PetRecord, uuid.UUID, File, *File]
}

// NewDB creates a new DB
func NewDB(opt badger.Options) (*DB, error) {
	base, err := badger.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}
	gcTicker := time.NewTicker(5 * time.Minute)
	go func() {
		for range gcTicker.C {
		again:
			err := base.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()

	reg, err := legacy.NewNameRegistry(base)
	if err != nil {
		return nil, fmt.Errorf("failed to create name registry: %w", err)
	}

	categoriesIdSeq, err := base.GetSequence(reg.MustName("categories_seq"), 10)
	if err != nil {
		return nil, fmt.Errorf("failed to get categories sequence: %w", err)
	}

	categoriesStore := recstore.New[int64, types.Category](pstore.New(nil, reg.MustName("categories"))).
		WithIdFunc(recstore.WithSequenceAsIdFunc[int64, types.Category](categoriesIdSeq))

	petsIdSequence, err := base.GetSequence(reg.MustName("pets_seq"), 10)
	if err != nil {
		return nil, fmt.Errorf("failed to get pets sequence: %w", err)
	}

	petsRepository := recstore.New[int64, types.PetRecord](pstore.New(nil, reg.MustName("pets"))).
		WithIdFunc(recstore.WithSequenceAsIdFunc[int64, types.PetRecord](petsIdSequence))

	categoriesPetsRel := rectools.Associate("categories_pets_rel", categoriesStore, petsRepository).
		WithPIDFunc(func(p *types.PetRecord) (int64, error) {
			return p.CategoryId, nil
		})

	imagesRepository := recstore.New[uuid.UUID, File](pstore.New(nil, reg.MustName("images"))).
		WithIdFunc(func(f *File) (uuid.UUID, error) {
			return uuid.New(), nil
		})

	petsImagesRel := rectools.Associate("pets_images_rel", petsRepository, imagesRepository)

	return &DB{
		base:              base,
		gcTicker:          gcTicker,
		reg:               reg,
		categoriesIdSeq:   categoriesIdSeq,
		categoriesStore:   categoriesStore,
		petsIdSeq:         petsIdSequence,
		petsStore:         petsRepository,
		imagesStore:       imagesRepository,
		categoriesPetsRel: categoriesPetsRel,
		petsImagesRel:     petsImagesRel,
	}, nil
}

// Close closes the DB
func (db *DB) Close() error {
	db.categoriesIdSeq.Release()
	db.petsIdSeq.Release()
	db.gcTicker.Stop()
	return db.base.Close()
}

// CreateCategory creates a new category
func (db *DB) CreateCategory(txn *badger.Txn, name string) (int64, error) {
	categoriesStore := db.categoriesStore.Instantiate(txn)
	cat := &types.Category{
		Name: name,
	}
	err := categoriesStore.Set(cat)
	if err != nil {
		return 0, err
	}

	return cat.Id, err
}

// GetCategory gets a category by id
func (db *DB) GetCategory(txn *badger.Txn, id int64) (*types.Category, error) {
	return db.categoriesStore.Instantiate(txn).Get(id)
}

// CreatePet creates a new pet
func (db *DB) CreatePet(txn *badger.Txn, catId int64, name string, status types.Status) (int64, error) {
	petsStore := db.petsStore.Instantiate(txn)
	p := &types.PetRecord{
		CategoryId: catId,
		Name:       name,
		Status:     status,
	}
	err := petsStore.Set(p)
	return p.Id, err
}

// GetPet gets a pet by id
func (db *DB) GetPet(txn *badger.Txn, id int64) (*types.PetRecord, error) {
	return db.petsStore.Instantiate(txn).Get(id)
}

// GetPetImageIds gets image ids of a pet
func (db *DB) GetPetImageIds(txn *badger.Txn, petId int64) ([]uuid.UUID, error) {
	rel := db.petsImagesRel.Instantiate(txn)
	imagesIter, err := rel.GetChildrenIterator(petId, badger.IteratorOptions{PrefetchValues: false})
	if err != nil {
		return nil, err
	}
	defer imagesIter.Close()

	var imageIds []uuid.UUID
	for imagesIter.Rewind(); imagesIter.Valid(); imagesIter.Next() {
		id, err := imagesIter.Key()
		if err != nil {
			return nil, err
		}
		imageIds = append(imageIds, id)
	}
	return imageIds, nil
}

// ListPets list pets with pagination
func (db *DB) ListPets(txn *badger.Txn, limit, offset int) ([]*types.PetRecord, error) {
	petsStore := db.petsStore.Instantiate(txn)
	it := iters.Limit(
		iters.SkipN(
			petsStore.NewIterator(badger.DefaultIteratorOptions),
			int(offset),
		),
		int(limit),
	)
	defer it.Close()

	pets, err := iters.Collect(it)
	if err != nil {
		return nil, err
	}

	return pets, nil
}

// DeletePet deletes a pet by id
func (db *DB) DeletePet(txn *badger.Txn, id int64) error {
	return db.petsStore.Instantiate(txn).Delete(id)
}

// CreateFile creates a new file
func (db *DB) CreateFile(txn *badger.Txn, petId int64, data []byte, metadata string) (uuid.UUID, error) {
	imagesStore := db.imagesStore.Instantiate(txn)
	f := &File{
		Data: data,
		Metadata: map[string]any{
			"additionalMetadata": metadata,
		},
	}
	err := imagesStore.Set(f, extstore.WithExtOption(db.petsImagesRel.Name(), petId))
	if err != nil {
		return uuid.Nil, err
	}
	return f.Id, nil
}

// GetFile gets a file by id
func (db *DB) GetFile(txn *badger.Txn, id uuid.UUID) ([]byte, error) {
	imagesStore := db.imagesStore.Instantiate(txn)
	f, err := imagesStore.Get(id)
	if err != nil {
		return nil, err
	}
	return f.Data, nil
}

// DeleteFile deletes a file by id
func (db *DB) DeleteFile(txn *badger.Txn, id uuid.UUID) error {
	return db.imagesStore.Instantiate(txn).Delete(id)
}
