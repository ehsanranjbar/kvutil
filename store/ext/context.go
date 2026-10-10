package ext

import (
	"context"

	"github.com/ehsanranjbar/kvutil/codec"
)

type contextKey string

const (
	keyCodecContextKey contextKey = "kc"
	keyBytesContextKey contextKey = "kbz"
)

// GetKeyCodecFromContext returns the KeyCodec from the context.
func GetKeyCodecFromContext[K any](ctx context.Context) codec.Codec[K] {
	if value := ctx.Value(keyCodecContextKey); value != nil {
		return value.(codec.Codec[K])
	}
	return nil
}

// GetKeyBytesFromContext returns the key serialized as bytes from the context.
func GetKeyBytesFromContext(ctx context.Context) []byte {
	if value := ctx.Value(keyBytesContextKey); value != nil {
		return value.([]byte)
	}
	return nil
}
