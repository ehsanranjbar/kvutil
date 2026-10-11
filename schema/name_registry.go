package schema

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/ehsanranjbar/kvutil/codec/lex"
	"github.com/ehsanranjbar/kvutil/kv"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// NameRegistry associates names with short keys of a fixed size and remembers them in the
// database, so that a long name can be used as a short prefix.
// It can give the namespaces of a schema their prefixes, as a PrefixAllocator.
type NameRegistry struct {
	db      kv.DB
	prefix  []byte
	keyLen  int
	nextKey []byte
	m       map[string][]byte
	mu      sync.Mutex
}

// NewNameRegistry creates a new NameRegistry and loads the names that the database has.
func NewNameRegistry(db kv.DB, opts ...func(*NameRegistry)) (*NameRegistry, error) {
	nreg := &NameRegistry{
		db:     db,
		keyLen: 1,
		m:      make(map[string][]byte),
	}
	for _, opt := range opts {
		opt(nreg)
	}
	nreg.nextKey = lex.Increment(bytes.Repeat([]byte{0}, nreg.keyLen))

	err := nreg.load()
	if err != nil {
		return nil, fmt.Errorf("failed to load name registry: %w", err)
	}
	return nreg, nil
}

// WithRegistryPrefix sets the key that the registry is stored at.
// By default it is the key of zeros that has the length of the keys of the registry.
func WithRegistryPrefix(prefix []byte) func(*NameRegistry) {
	return func(nreg *NameRegistry) {
		nreg.prefix = bytes.Clone(prefix)
	}
}

// WithRegistryKeyLen sets the length of the keys of the registry, which is 1 by default.
func WithRegistryKeyLen(keyLen int) func(*NameRegistry) {
	return func(nreg *NameRegistry) {
		nreg.keyLen = keyLen
	}
}

func (nreg *NameRegistry) load() error {
	return nreg.db.View(func(tx kv.Tx) error {
		bz, err := tx.Get(nreg.configKey())
		if errors.Is(err, kv.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to get config: %w", err)
		}

		dec := msgpack.GetDecoder()
		dec.Reset(bytes.NewReader(bz))
		defer msgpack.PutDecoder(dec)

		err = dec.DecodeMulti(&nreg.m, &nreg.nextKey)
		if err != nil {
			return fmt.Errorf("failed to decode config: %w", err)
		}
		return nil
	})
}

func (nreg *NameRegistry) configKey() []byte {
	if len(nreg.prefix) == 0 {
		return bytes.Repeat([]byte{0}, nreg.keyLen)
	}
	return nreg.prefix
}

// MustName is like Name but panics if an error occurs.
func (nreg *NameRegistry) MustName(name string) []byte {
	key, err := nreg.Name(name)
	if err != nil {
		panic(err)
	}
	return key
}

// Name returns the key of a name, giving it the next free key if it has none yet.
func (nreg *NameRegistry) Name(name string) ([]byte, error) {
	nreg.mu.Lock()
	defer nreg.mu.Unlock()

	if key, ok := nreg.m[name]; ok {
		return bytes.Clone(key), nil
	}

	if len(nreg.nextKey) > nreg.keyLen {
		return nil, fmt.Errorf("name registry is full")
	}

	key := bytes.Clone(nreg.nextKey)
	nextKey := lex.Increment(bytes.Clone(nreg.nextKey))
	nreg.m[name] = key

	// The registry in memory must not get ahead of what is persisted.
	err := nreg.update(nextKey)
	if err != nil {
		delete(nreg.m, name)
		return nil, fmt.Errorf("failed to update name registry: %w", err)
	}
	nreg.nextKey = nextKey

	return bytes.Clone(key), nil
}

func (nreg *NameRegistry) update(nextKey []byte) error {
	return nreg.db.Update(func(tx kv.Tx) error {
		enc := msgpack.GetEncoder()
		var buf bytes.Buffer
		enc.Reset(&buf)
		defer msgpack.PutEncoder(enc)

		err := enc.EncodeMulti(nreg.m, nextKey)
		if err != nil {
			return fmt.Errorf("failed to encode config: %w", err)
		}

		err = tx.Set(nreg.configKey(), buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to set config: %w", err)
		}
		return nil
	})
}
