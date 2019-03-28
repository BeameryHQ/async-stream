package kvstore

import (
	"context"
	"errors"
	"github.com/BeameryHQ/async-stream/stream"
)

var (
	ErrNotFound         = errors.New("record  not found")
	ErrConcurrentUpdate = errors.New("concurrent update")
)

type PutConfig struct {
	disableLease bool
	version      int64
	ttl          int64
}

type PutOption func(c *PutConfig)

func newPutConfig() *PutConfig {
	return &PutConfig{
		disableLease: false,
		version:      0,
		ttl:          0,
	}
}

func WithVersion(version int64) PutOption {
	return func(c *PutConfig) {
		c.version = version
	}
}

func WithNoLease() PutOption {
	return func(c *PutConfig) {
		c.disableLease = true
	}
}

func WithTtl(ttl int64) PutOption {
	return func(c *PutConfig) {
		c.ttl = ttl
	}
}

type Store interface {
	Get(ctx context.Context, key string) (*stream.FlowKeyValue, error)
	Put(ctx context.Context, key, value string, opts ...PutOption) error
	Delete(ctx context.Context, key string) error
}
