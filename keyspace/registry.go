package keyspace

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/kv"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// registry associates names with short keys of a fixed size and remembers them in the
// database, so that a long name can be used as a short prefix.
type registry struct {
	db      kv.DB
	key     []byte
	keyLen  int
	nextKey []byte
	m       map[string][]byte
	mu      sync.Mutex
}

func newRegistry(db kv.DB, keyLen int, key []byte) (*registry, error) {
	if keyLen < 1 {
		return nil, fmt.Errorf("the length of the keys must be at least 1, not %d", keyLen)
	}

	r := &registry{
		db:     db,
		keyLen: keyLen,
		m:      make(map[string][]byte),
	}
	r.key = bytes.Clone(key)
	if len(r.key) == 0 {
		r.key = bytes.Repeat([]byte{0}, keyLen)
	}
	r.nextKey = lex.Increment(bytes.Repeat([]byte{0}, keyLen))

	if err := r.load(); err != nil {
		return nil, fmt.Errorf("failed to load name registry: %w", err)
	}
	return r, nil
}

func (r *registry) load() error {
	return r.db.View(func(tx kv.Tx) error {
		bz, err := tx.Get(r.key)
		if errors.Is(err, kv.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get config: %w", err)
		}

		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(bz))
		defer msgpack.PutDecoder(dec)

		err = dec.DecodeMulti(&r.m, &r.nextKey)
		if err != nil {
			return fmt.Errorf("failed to decode config: %w", err)
		}
		if r.m == nil {
			r.m = make(map[string][]byte)
		}
		return nil
	})
}

// name returns the key of a name, giving it the next free key if it has none yet.
func (r *registry) name(name string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if key, ok := r.m[name]; ok {
		return bytes.Clone(key), nil
	}

	// The key of the registry itself is not for a name, nor is anything that it is under.
	next := bytes.Clone(r.nextKey)
	for len(next) <= r.keyLen && bytes.HasPrefix(r.key, next) {
		next = lex.Increment(next)
	}
	if len(next) > r.keyLen {
		return nil, errors.New("name registry is full")
	}

	key := next
	nextKey := lex.Increment(bytes.Clone(key))
	r.m[name] = key

	// The registry in memory must not get ahead of what is persisted.
	if err := r.persist(nextKey); err != nil {
		delete(r.m, name)
		return nil, fmt.Errorf("failed to update name registry: %w", err)
	}
	r.nextKey = nextKey

	return bytes.Clone(key), nil
}

// forget removes a name, whose key is not given to another name.
func (r *registry) forget(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key, ok := r.m[name]
	if !ok {
		return nil
	}
	delete(r.m, name)
	if err := r.persist(r.nextKey); err != nil {
		r.m[name] = key
		return fmt.Errorf("failed to update name registry: %w", err)
	}
	return nil
}

// lookup returns the key of a name, if it has one.
func (r *registry) lookup(name string) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key, ok := r.m[name]
	return bytes.Clone(key), ok
}

// nameOf returns the name that a key is of.
func (r *registry) nameOf(key []byte) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, k := range r.m {
		if bytes.Equal(k, key) {
			return name, true
		}
	}
	return "", false
}

// names returns the names that the registry has, sorted.
func (r *registry) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Sorted(maps.Keys(r.m))
}

func (r *registry) persist(nextKey []byte) error {
	return r.db.Update(func(tx kv.Tx) error {
		enc := msgpack.GetEncoder()
		var buf bytes.Buffer
		enc.Reset(&buf)
		defer msgpack.PutEncoder(enc)

		err := enc.EncodeMulti(r.m, nextKey)
		if err != nil {
			return fmt.Errorf("failed to encode config: %w", err)
		}

		err = tx.Set(r.key, buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to set config: %w", err)
		}
		return nil
	})
}
