// Package types has the types of the pet store: the records that it stores, which encode
// themselves as FlatBuffers, and the pet as the API has it, which is JSON.
//
// The tables of FlatBuffers are in types.fbs, and the code of the package fb is generated from
// them. From this directory, run:
//
//	flatc --go -o . types.fbs && gofmt -w fb
//
// The types here are plain structs, so that the paths of their fields, which the index and the
// aggregations are defined by, are the names of the fields.
package types

//go:generate sh -c "flatc --go -o . types.fbs && gofmt -w fb"
