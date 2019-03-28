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
	DisableLease bool
	Version      int64
	Ttl          int64
}

type PutOption func(c *PutConfig)

func NewPutConfig() *PutConfig {
	return &PutConfig{
		DisableLease: false,
		Version:      0,
		Ttl:          0,
	}
}

func WithVersion(version int64) PutOption {
	return func(c *PutConfig) {
		c.Version = version
	}
}

func WithNoLease() PutOption {
	return func(c *PutConfig) {
		c.DisableLease = true
	}
}

func WithTtl(ttl int64) PutOption {
	return func(c *PutConfig) {
		c.Ttl = ttl
	}
}

type Store interface {
	Get(ctx context.Context, key string) (*stream.FlowKeyValue, error)
	Put(ctx context.Context, key, value string, opts ...PutOption) error
	Delete(ctx context.Context, key string) error
}
