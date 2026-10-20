package main

import (
	"time"

	"github.com/ehsanranjbar/kvutil/examples/petstore/types"
	"github.com/ehsanranjbar/kvutil/store/record"
)

// The migrations of the store are a part of the program of the version that they lead to.
// A database of an older version is brought to this one when the program opens it, a version
// at a time, and a database of this version is left as it is.

// migratePets adds the migrations of the pets.
func migratePets(pets *record.Store[int64, types.PetRecord]) {
	// Version 1 had no time for when a pet was added. The field itself needs no migration,
	// as a message of protobuf is read without the fields that it does not have. But a pet
	// without the time would count as added in 1970, so the pets of version 1 are given the
	// time of the upgrade.
	//
	// The type of the pets is the same on both sides here. When it is not, the type of the
	// version before stays in the program, next to its migration, and is what the function
	// is given. A store that is two versions on has a step for each, through the type of the
	// version in between, and the last step gives the type of the store:
	//
	//	pets.WithVersion(3).
	//		MigrateFrom(1, func(_ int64, old *PetV1) (*PetV2, error) { ... }).
	//		MigrateFrom(2, func(_ int64, old *PetV2) (*types.PetRecord, error) { ... })
	upgraded := time.Now().Unix()
	pets.MigrateFrom(1, func(_ int64, p *types.PetRecord) (*types.PetRecord, error) {
		if p.CreatedAt == 0 {
			p.CreatedAt = upgraded
		}
		return p, nil
	})

	// The index of the statuses and the two aggregations need nothing here: they are built
	// again from the pets once the pets are migrated.
}
