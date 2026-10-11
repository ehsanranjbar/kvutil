package legacy

import (
	"bytes"
	"fmt"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/kvutil/codec/lex"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// NameRegistry associates a long string name with a unique sized byte slice.
type NameRegistry struct {
	db      *badger.DB
	prefix  []byte
	keyLen  int
	nextKey []byte
	m       map[string][]byte
	mu      sync.Mutex
}

// NewNameRegistry creates a new NameRegistry.
func NewNameRegistry(db *badger.DB, opts ...func(*NameRegistry)) (*NameRegistry, error) {
	nreg := &NameRegistry{
		db:     db,
		keyLen: 1,
		m:      make(map[string][]byte),
	}
	for _, opt := range opts {
		opt(nreg)
	}
	nreg.nextKey = lex.Increment(bytes.Repeat([]byte{0}, int(nreg.keyLen)))

	err := nreg.load()
	if err != nil {
		return nil, fmt.Errorf("failed to load name registry: %w", err)
	}
	return nreg, nil
}

// WithRegistryPrefix sets the prefix for the NameRegistry.
func WithRegistryPrefix(prefix []byte) func(*NameRegistry) {
	return func(nreg *NameRegistry) {
		nreg.prefix = prefix
	}
}

// WithRegistryKeyLen sets the key length for the NameRegistry.
func WithRegistryKeyLen(keyLen int) func(*NameRegistry) {
	return func(nreg *NameRegistry) {
		nreg.keyLen = keyLen
	}
}

func (nreg *NameRegistry) load() error {
	nreg.m = make(map[string][]byte)
	err := nreg.db.View(func(txn *badger.Txn) error {

		configItem, err := txn.Get(nreg.getConfigKey())
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return fmt.Errorf("failed to get config item: %w", err)
		}

		err = configItem.Value(func(val []byte) error {
			dec := msgpack.GetDecoder()
			dec.Reset(bytes.NewReader(val))
			defer msgpack.PutDecoder(dec)

			err := dec.DecodeMulti(&nreg.m, &nreg.nextKey)
			if err != nil {
				return fmt.Errorf("failed to decode config: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (nreg *NameRegistry) getConfigKey() []byte {
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

// Name associates a name with a unique key.
func (nreg *NameRegistry) Name(name string) ([]byte, error) {
	nreg.mu.Lock()
	defer nreg.mu.Unlock()

	if key, ok := nreg.m[name]; ok {
		return key, nil
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

	return key, nil
}

func (nreg *NameRegistry) update(nextKey []byte) error {
	err := nreg.db.Update(func(txn *badger.Txn) error {
		enc := msgpack.GetEncoder()
		var buf bytes.Buffer
		enc.Reset(&buf)
		defer msgpack.PutEncoder(enc)

		err := enc.EncodeMulti(nreg.m, nextKey)
		if err != nil {
			return fmt.Errorf("failed to encode config: %w", err)
		}

		err = txn.Set(nreg.getConfigKey(), buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to set config item: %w", err)
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
