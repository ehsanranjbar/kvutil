// Package kvutil is a toolkit of layered, optional abstractions over ordered key-value stores.
package kvutil

import "github.com/ehsanranjbar/kvutil/kv"

// Instantiator creates the instance of something for a transaction.
// A store, an index and the like are defined once and are instantiated for each transaction.
type Instantiator[I any] interface {
	Instantiate(tx kv.Tx) I
}
